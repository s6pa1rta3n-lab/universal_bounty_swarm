"""
Escort Sidecar — Autonomous PR Lifecycle Monitor & CI Escort.

Monitors active PRs, CI statuses, review states, and inactivity across Firestore
`bounty_memory` and GitHub. Identifies CI failures to trigger fixer passes,
detects 14-day inactivity staleness to schedule polite follow-up bumps, and
updates lifecycle telemetry in Firestore.
"""

import json
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from google.cloud import firestore

from src.core.config import COLLECTION_BOUNTY_MEMORY, get_config
from src.core.firestore_client import get_firestore_client
from src.core.listener import FirestoreEvent, FirestoreListener, listen_collection

logger = logging.getLogger("UniversalBountySwarm.EscortSidecar")

# GraphQL query for active pull requests authored by @me
GRAPHQL_PR_ESCORT_QUERY = """
query($cursor: String) {
  viewer {
    pullRequests(first: 50, after: $cursor, states: OPEN, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        id
        number
        title
        url
        state
        isDraft
        createdAt
        updatedAt
        repository {
          nameWithOwner
          isArchived
        }
        commits(last: 1) {
          nodes {
            commit {
              oid
              committedDate
              statusCheckRollup {
                state
              }
              checkSuites(first: 10) {
                nodes {
                  app {
                    name
                  }
                  status
                  conclusion
                  checkRuns(first: 15) {
                    nodes {
                      name
                      status
                      conclusion
                      detailsUrl
                    }
                  }
                }
              }
            }
          }
        }
        latestReviews(first: 10) {
          nodes {
            author {
              login
            }
            state
            body
            submittedAt
          }
        }
        comments(last: 20) {
          nodes {
            author {
              login
            }
            body
            createdAt
          }
        }
      }
    }
  }
}
"""

IGNORE_BOTS = {
    "s6pa1rta3n-lab",
    "github-advanced-security",
    "coderabbitai",
    "gitar-bot",
    "vercel",
    "github-actions",
    "codecov",
    "coveralls",
    "dependabot",
    "stale",
    "github-actions[bot]",
    "vercel[bot]",
    "coderabbitai[bot]",
    "codecov[bot]",
    "dependabot[bot]",
}


def parse_iso_datetime(dt_val: Union[str, datetime, int, float, None]) -> Optional[datetime]:
    """Safely parses various datetime representations into UTC datetime."""
    if dt_val is None:
        return None
    if isinstance(dt_val, datetime):
        return dt_val if dt_val.tzinfo else dt_val.replace(tzinfo=timezone.utc)
    if isinstance(dt_val, (int, float)):
        return datetime.fromtimestamp(dt_val, tz=timezone.utc)
    if isinstance(dt_val, str):
        try:
            return datetime.fromisoformat(dt_val.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


class EscortSidecar:
    """
    Modular PR Escort & CI Monitor Sidecar.
    Tracks active PRs, checks for failing CI checks, detects 14-day inactivity stalls,
    and maintains real-time status in Firestore `bounty_memory`.
    """

    def __init__(
        self,
        db: Optional[Any] = None,
        memory_collection: str = COLLECTION_BOUNTY_MEMORY,
        stale_days_threshold: int = 14,
        auto_start: bool = False,
    ):
        self.db = db if db is not None else get_firestore_client()
        self.memory_collection = memory_collection
        self.stale_days_threshold = stale_days_threshold

        self._listener: Optional[FirestoreListener] = None
        self._is_running = False
        self._lock = threading.RLock()
        self._monitored_prs: Dict[str, Dict[str, Any]] = {}

        if auto_start:
            self.start()

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._is_running

    def start(self) -> "EscortSidecar":
        """Starts listening to bounty_memory in real-time."""
        with self._lock:
            if self._is_running:
                logger.warning("EscortSidecar is already running.")
                return self

            logger.info(f"Starting EscortSidecar on collection '{self.memory_collection}'...")
            col_ref = self.db.collection(self.memory_collection)
            self._listener = listen_collection(
                col_ref=col_ref,
                callback=self._handle_memory_event,
                include_initial_snapshot=True,
            )
            self._is_running = True
            return self

    def stop(self) -> None:
        """Stops the real-time listener."""
        with self._lock:
            if not self._is_running:
                return
            self._is_running = False
            if self._listener:
                self._listener.unsubscribe()
                self._listener = None
            logger.info("EscortSidecar stopped.")

    def __enter__(self) -> "EscortSidecar":
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()

    def _handle_memory_event(self, *args) -> None:
        """Callback for memory collection changes."""
        if not self._is_running:
            return

        if len(args) == 1 and isinstance(args[0], FirestoreEvent):
            event: FirestoreEvent = args[0]
            if event.data:
                self.audit_and_update_pr(event.document_id, event.data)
        elif len(args) >= 2:
            changes = args[1]
            for change in changes:
                doc = change.document
                if doc.exists:
                    self.audit_and_update_pr(doc.id, doc.to_dict())

    def inspect_pr_data(self, pr_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Inspects PR attributes (CI status, review state, staleness) and returns an audit evaluation.
        """
        now = datetime.now(timezone.utc)

        # 1. CI Status Evaluation
        ci_status = "UNKNOWN"
        ci_failures: List[str] = []

        # Check statusCheckRollup or explicit CI fields
        if "ci_status" in pr_data:
            ci_status = str(pr_data["ci_status"]).upper()
        elif "commits" in pr_data and pr_data["commits"]:
            commits_nodes = pr_data["commits"].get("nodes", []) if isinstance(pr_data["commits"], dict) else pr_data["commits"]
            if commits_nodes and isinstance(commits_nodes, list):
                last_commit = commits_nodes[0].get("commit", {}) if isinstance(commits_nodes[0], dict) else {}
                rollup = last_commit.get("statusCheckRollup", {})
                if rollup and isinstance(rollup, dict):
                    ci_status = rollup.get("state", "UNKNOWN").upper()

                # Inspect checkSuites
                check_suites = last_commit.get("checkSuites", {}).get("nodes", [])
                for suite in check_suites:
                    if isinstance(suite, dict) and suite.get("conclusion") in ("FAILURE", "TIMED_OUT", "ACTION_REQUIRED"):
                        runs = suite.get("checkRuns", {}).get("nodes", [])
                        for r in runs:
                            if isinstance(r, dict) and r.get("conclusion") == "FAILURE":
                                ci_failures.append(r.get("name", "unnamed_check"))

        needs_ci_fix = ci_status in ("FAILURE", "ERROR") or bool(ci_failures)

        # 2. Inactivity / Staleness Evaluation
        created_at = parse_iso_datetime(pr_data.get("created_at") or pr_data.get("createdAt"))
        updated_at = parse_iso_datetime(pr_data.get("updated_at") or pr_data.get("updatedAt"))

        last_activity = updated_at or created_at or now
        inactivity_delta = now - last_activity
        inactivity_days = max(0, inactivity_delta.total_seconds() / 86400.0)

        is_stalled = inactivity_days >= self.stale_days_threshold
        needs_maintainer_bump = is_stalled and not pr_data.get("is_draft", False)

        # 3. Review Status Evaluation
        review_status = "PENDING_REVIEW"
        if pr_data.get("audit_status") == "PASS":
            review_status = "VICTORY_AUDIT_PASSED"
        if pr_data.get("state") == "MERGED":
            review_status = "MERGED"
        elif pr_data.get("state") == "CLOSED":
            review_status = "CLOSED"

        return {
            "pr_url": pr_data.get("pr_url") or pr_data.get("url"),
            "pr_number": pr_data.get("pr_number") or pr_data.get("number"),
            "repo": pr_data.get("repo") or (pr_data.get("repository", {}).get("nameWithOwner") if isinstance(pr_data.get("repository"), dict) else None),
            "ci_status": ci_status,
            "needs_ci_fix": needs_ci_fix,
            "ci_failures": ci_failures,
            "inactivity_days": round(inactivity_days, 1),
            "is_stalled": is_stalled,
            "needs_maintainer_bump": needs_maintainer_bump,
            "review_status": review_status,
            "audited_at_iso": now.isoformat(),
        }

    def audit_and_update_pr(self, doc_id: str, doc_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Audits a single PR and updates its Firestore memory document with latest telemetry.
        """
        eval_result = self.inspect_pr_data(doc_data)
        self._monitored_prs[doc_id] = eval_result

        doc_ref = self.db.collection(self.memory_collection).document(doc_id)
        update_payload: Dict[str, Any] = {
            "escort_telemetry": {
                "ci_status": eval_result["ci_status"],
                "needs_ci_fix": eval_result["needs_ci_fix"],
                "is_stalled": eval_result["is_stalled"],
                "inactivity_days": eval_result["inactivity_days"],
                "needs_maintainer_bump": eval_result["needs_maintainer_bump"],
                "audited_at_iso": eval_result["audited_at_iso"],
            },
            "updated_at_iso": datetime.now(timezone.utc).isoformat(),
        }

        try:
            doc_ref.update(update_payload)
            logger.info(
                f"[Escort] Updated doc {doc_id}: CI={eval_result['ci_status']}, Stalled={eval_result['is_stalled']} ({eval_result['inactivity_days']}d)"
            )
        except Exception as e:
            logger.error(f"[!] Failed to update escort telemetry for {doc_id}: {e}")

        return eval_result

    def query_github_open_prs(self) -> List[Dict[str, Any]]:
        """Queries GitHub GraphQL for open PRs authored by @me."""
        cmd = ["gh", "api", "graphql", "-f", f"query={GRAPHQL_PR_ESCORT_QUERY}"]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if res.returncode != 0:
                logger.debug(f"GitHub PR GraphQL query skipped or non-zero: {res.stderr.strip()}")
                return []
            data = json.loads(res.stdout)
            viewer = data.get("data", {}).get("viewer", {})
            return viewer.get("pullRequests", {}).get("nodes", []) or []
        except Exception as e:
            logger.debug(f"Could not query GitHub PRs: {e}")
            return []

    def check_prs(self) -> List[Dict[str, Any]]:
        """
        Scans all documents in bounty_memory collection and evaluates PR health.
        """
        results: List[Dict[str, Any]] = []
        col_ref = self.db.collection(self.memory_collection)

        try:
            docs = col_ref.get()
            for doc in docs:
                data = doc.to_dict() or {}
                res = self.audit_and_update_pr(doc.id, data)
                results.append(res)
        except Exception as e:
            logger.error(f"Error checking PRs in {self.memory_collection}: {e}")

        return results

    def run_once(self) -> List[Dict[str, Any]]:
        """Single execution pass."""
        logger.info("Executing single PR Escort sweep...")
        results = self.check_prs()
        logger.info(f"PR Escort sweep completed for {len(results)} PRs.")
        return results

    def run(self, interval_sec: int = 3600, stop_event: Optional[Any] = None) -> None:
        """Runs continuous escort loop."""
        logger.info(f"Starting continuous EscortSidecar loop (interval={interval_sec}s)...")
        while True:
            if stop_event and stop_event.is_set():
                logger.info("Stop event received. Exiting EscortSidecar loop.")
                break
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"Error during escort loop iteration: {e}", exc_info=True)

            if stop_event:
                if stop_event.wait(timeout=interval_sec):
                    break
            else:
                time.sleep(interval_sec)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    sidecar = EscortSidecar()
    sidecar.run_once()

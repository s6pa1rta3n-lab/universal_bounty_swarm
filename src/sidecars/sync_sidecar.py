"""
Sync Sidecar — Settlement Verification, Accounting Ledger & Coordinator State Sync.

Monitors merged PRs and completed bounties in Firestore (`bounty_memory` and `bounty_leads`),
extracts verified payout values, synchronizes settlement ledgers in `bounty_settlements`,
and updates system-wide swarm telemetry in `swarm_coordinator/state`.
"""

import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from google.cloud import firestore

from src.core.config import (
    COLLECTION_BOUNTY_LEADS,
    COLLECTION_BOUNTY_MEMORY,
    COLLECTION_SWARM_COORDINATOR,
    get_config,
)
from src.core.firestore_client import get_firestore_client
from src.core.listener import FirestoreEvent, FirestoreListener, listen_collection

logger = logging.getLogger("UniversalBountySwarm.SyncSidecar")

COLLECTION_BOUNTY_SETTLEMENTS = "bounty_settlements"


def extract_payout_numeric(data: Dict[str, Any]) -> Tuple[str, float]:
    """
    Extracts payout string and numeric USD value from various document schemas.
    """
    # 1. Direct numeric float/int fields
    if "projected_payout_usd" in data and isinstance(data["projected_payout_usd"], (int, float)):
        val = float(data["projected_payout_usd"])
        p_str = data.get("projected_payout") or f"${val:.2f}"
        return p_str, val

    if "escrow" in data and isinstance(data["escrow"], dict):
        escrow_data = data["escrow"]
        amount_usd = escrow_data.get("amount_usd")
        if isinstance(amount_usd, (int, float)) and amount_usd > 0:
            return f"${float(amount_usd):.2f}", float(amount_usd)

    # 2. Text extraction
    raw_text = ""
    for field in ["projected_payout", "reward_tokens", "payout", "title", "body"]:
        v = data.get(field)
        if isinstance(v, str):
            raw_text += f" {v}"

    # Dollar amounts ($100, $1,500.00, etc.)
    m_dollar = re.findall(r"\$\s*(\d+(?:,\d{3})*(?:\.\d+)?)", raw_text)
    if m_dollar:
        val = float(m_dollar[0].replace(",", ""))
        return f"${val:.2f}", val

    # Token amounts (500 USDC, 1000 XLM, etc.)
    m_token = re.findall(
        r"(\d+(?:,\d{3})*(?:\.\d+)?)\s*(USDC|USDT|XLM|ETH|WETH|DAI|MATIC|POL|OP|ARB|SOL|USD)",
        raw_text,
        re.IGNORECASE,
    )
    if m_token:
        val_str, tok = m_token[0]
        val = float(val_str.replace(",", ""))
        return f"{val} {tok.upper()}", val

    return "$0.00", 0.0


class SyncSidecar:
    """
    Modular Sync & Settlement Sidecar.
    Maintains financial ledgers and updates `swarm_coordinator` global state.
    """

    def __init__(
        self,
        db: Optional[Any] = None,
        memory_collection: str = COLLECTION_BOUNTY_MEMORY,
        leads_collection: str = COLLECTION_BOUNTY_LEADS,
        coordinator_collection: str = COLLECTION_SWARM_COORDINATOR,
        settlements_collection: str = COLLECTION_BOUNTY_SETTLEMENTS,
        auto_start: bool = False,
    ):
        self.db = db if db is not None else get_firestore_client()
        self.memory_collection = memory_collection
        self.leads_collection = leads_collection
        self.coordinator_collection = coordinator_collection
        self.settlements_collection = settlements_collection

        self._listener: Optional[FirestoreListener] = None
        self._is_running = False
        self._lock = threading.RLock()
        self._settled_records: Dict[str, Dict[str, Any]] = {}

        if auto_start:
            self.start()

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._is_running

    def start(self) -> "SyncSidecar":
        """Starts real-time listener on memory and leads."""
        with self._lock:
            if self._is_running:
                logger.warning("SyncSidecar is already running.")
                return self

            logger.info("Starting SyncSidecar real-time listeners...")
            mem_col = self.db.collection(self.memory_collection)
            self._listener = listen_collection(
                col_ref=mem_col,
                callback=self._handle_event,
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
            logger.info("SyncSidecar stopped.")

    def __enter__(self) -> "SyncSidecar":
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()

    def _handle_event(self, *args) -> None:
        """Callback for Firestore mutations."""
        if not self._is_running:
            return

        if len(args) == 1 and isinstance(args[0], FirestoreEvent):
            event: FirestoreEvent = args[0]
            if event.data:
                self.process_doc_settlement(event.document_id, event.data)
        elif len(args) >= 2:
            changes = args[1]
            for change in changes:
                doc = change.document
                if doc.exists:
                    self.process_doc_settlement(doc.id, doc.to_dict())

    def check_github_pr_merged(self, pr_url_or_number: Union[str, int], repo: Optional[str] = None) -> bool:
        """Checks if a PR is merged via GitHub CLI."""
        target = str(pr_url_or_number)
        cmd = ["gh", "pr", "view", target, "--json", "state,mergedAt"]
        if repo:
            cmd.extend(["--repo", repo])

        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if res.returncode == 0:
                data = json.loads(res.stdout)
                return data.get("state") == "MERGED" or bool(data.get("mergedAt"))
        except Exception:
            pass
        return False

    def process_doc_settlement(self, doc_id: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Evaluates a document to determine if it is eligible for settlement recording:
        - state == 'MERGED' or status == 'completed' or audit_status == 'PASS' (and merged)
        """
        state = str(data.get("state", "")).upper()
        status = str(data.get("status", "")).lower()
        audit_status = str(data.get("audit_status", "")).upper()

        is_settled = (
            state == "MERGED"
            or status in ("completed", "settled")
            or (audit_status == "PASS" and data.get("merge_allowed", False))
        )

        if not is_settled:
            return None

        payout_str, payout_usd = extract_payout_numeric(data)
        now_iso = datetime.now(timezone.utc).isoformat()

        settlement_id = f"settle_{doc_id.replace('/', '_').replace('#', '_')}"
        settlement_record: Dict[str, Any] = {
            "settlement_id": settlement_id,
            "source_doc_id": doc_id,
            "repo": data.get("repo") or data.get("repository"),
            "pr_number": data.get("pr_number") or data.get("number"),
            "pr_url": data.get("pr_url") or data.get("url"),
            "title": data.get("title"),
            "payout_str": payout_str,
            "payout_usd": payout_usd,
            "settled_at_iso": now_iso,
            "status": "SETTLED",
        }

        # Write settlement record
        try:
            settle_ref = self.db.collection(self.settlements_collection).document(settlement_id)
            settle_ref.set(settlement_record, merge=True)
            self._settled_records[settlement_id] = settlement_record
            logger.info(f"[Sync] Recorded settlement {settlement_id} for {doc_id}: {payout_str}")
        except Exception as e:
            logger.error(f"[!] Error writing settlement record {settlement_id}: {e}")

        return settlement_record

    def sync_settlements(self) -> Dict[str, Any]:
        """
        Full sweep:
        1. Scans bounty_memory and bounty_leads for completed/merged items
        2. Records settlements
        3. Aggregates financial totals
        4. Updates swarm_coordinator/state
        """
        settled_docs: List[Dict[str, Any]] = []

        # Scan bounty_memory
        try:
            mem_docs = self.db.collection(self.memory_collection).get()
            for doc in mem_docs:
                rec = self.process_doc_settlement(doc.id, doc.to_dict())
                if rec:
                    settled_docs.append(rec)
        except Exception as e:
            logger.error(f"Error scanning {self.memory_collection}: {e}")

        # Scan bounty_leads
        try:
            leads_docs = self.db.collection(self.leads_collection).where("status", "==", "completed").get()
            for doc in leads_docs:
                rec = self.process_doc_settlement(doc.id, doc.to_dict())
                if rec:
                    settled_docs.append(rec)
        except Exception as e:
            logger.error(f"Error scanning {self.leads_collection}: {e}")

        # Compute aggregate metrics
        total_settled_usd = 0.0
        try:
            all_settlements = self.db.collection(self.settlements_collection).get()
            for s_doc in all_settlements:
                s_data = s_doc.to_dict() or {}
                total_settled_usd += float(s_data.get("payout_usd", 0.0))
        except Exception as e:
            logger.warning(f"Could not aggregate total settlements: {e}")

        # Update swarm_coordinator singleton doc
        now_iso = datetime.now(timezone.utc).isoformat()
        coordinator_state = {
            "total_settled_usd": round(total_settled_usd, 2),
            "total_settled_count": len(all_settlements) if 'all_settlements' in locals() else len(settled_docs),
            "last_sync_iso": now_iso,
            "status": "HEALTHY",
            "active_sidecars": ["intake_sidecar", "executor_sidecar", "escort_sidecar", "sync_sidecar"],
        }
        if hasattr(firestore, "SERVER_TIMESTAMP"):
            coordinator_state["last_sync_timestamp"] = firestore.SERVER_TIMESTAMP

        try:
            state_ref = self.db.collection(self.coordinator_collection).document("state")
            state_ref.set(coordinator_state, merge=True)
            logger.info(
                f"[Sync] Updated coordinator state: Total Settled=${total_settled_usd:.2f} ({coordinator_state['total_settled_count']} bounties)"
            )
        except Exception as e:
            logger.error(f"[!] Error updating coordinator state: {e}")

        return {
            "synced_count": len(settled_docs),
            "total_settled_usd": round(total_settled_usd, 2),
            "coordinator_state": coordinator_state,
        }

    def run_once(self) -> Dict[str, Any]:
        """Single execution pass."""
        logger.info("Executing single Sync & Settlement sweep...")
        res = self.sync_settlements()
        logger.info(f"Sync sweep completed. Synced {res['synced_count']} items.")
        return res

    def run(self, interval_sec: int = 3600, stop_event: Optional[Any] = None) -> None:
        """Runs continuous sync loop."""
        logger.info(f"Starting continuous SyncSidecar loop (interval={interval_sec}s)...")
        while True:
            if stop_event and stop_event.is_set():
                logger.info("Stop event received. Exiting SyncSidecar loop.")
                break
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"Error during sync loop iteration: {e}", exc_info=True)

            if stop_event:
                if stop_event.wait(timeout=interval_sec):
                    break
            else:
                time.sleep(interval_sec)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    sidecar = SyncSidecar()
    sidecar.run_once()

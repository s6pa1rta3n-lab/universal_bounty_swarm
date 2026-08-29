"""
Intake Sidecar — High-Value GitHub Bounty Ingestion & Sniper Filter.

Queries GitHub GraphQL API for open bounty issues across high-conviction ecosystems,
applies the strict Sniper Filter (rejecting banned platforms, subjective tasks,
archived repos, and unverified escrow), deduplicates against local seen caches and
Firestore, and writes qualified leads to the Firestore `bounty_leads` collection
with status 'priority_triage' or 'pending_triage'.
"""

import json
import logging
import os
import re
import subprocess
import time
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from google.cloud import firestore

from src.core.config import COLLECTION_BOUNTY_LEADS, get_config
from src.core.firestore_client import get_firestore_client, get_leads_collection
from src.core.path_guard import DEFAULT_PATH_GUARD, PathGuard
from src.core.safe_io import SafeIO

logger = logging.getLogger("UniversalBountySwarm.IntakeSidecar")

# Banned platforms and organizations (Strictly Enforced)
BANNED_PLATFORMS: List[str] = ["algora", "polar", "twentyhq/twenty", "twentyhq", "opire"]

# Subjective / Non-technical disqualification keywords
DISQUALIFY_KEYWORDS: List[str] = [
    "video pitch",
    "record a video",
    "record video",
    "recorded video",
    "recording a video",
    "submit a video",
    "video presentation",
    "video demo required",
    "demo video",
    "video walkthrough",
    "loom",
    "loom.com",
    "screencast",
    "interview",
    "zoom interview",
    "zoom call",
    "pitch deck",
    "manual kyc",
    "kyc required",
    "figma only",
    "design only",
]

# High-priority ecosystems and keywords
HIGH_PRIORITY_KEYWORDS: List[str] = [
    "grantfox",
    "grantfox oss",
    "stellar",
    "soroban",
    "xlm",
    "stellar wave",
    "evm",
    "ethereum",
    "base",
    "arbitrum",
    "optimism",
    "polygon",
    "matic",
    "pol",
    "zkevm",
    "avalanche",
    "avax",
    "bsc",
    "binance",
    "smart contract",
    "solidity",
    "foundry",
    "hardhat",
    "rust",
    "web3",
    "gitcoin",
    "bounties-network",
]

# Search categories
DEFAULT_SEARCH_CATEGORIES: List[Dict[str, str]] = [
    {"name": "GrantFox Global Search", "query": "is:issue is:open grantfox", "priority": "high"},
    {"name": "GrantFox Escrow Label", "query": "is:issue is:open label:grantfox", "priority": "high"},
    {"name": "GrantFox OSS", "query": 'is:issue is:open label:"GrantFox OSS"', "priority": "high"},
    {
        "name": "Stellar / Soroban Ecosystem",
        "query": "is:issue is:open stellar OR soroban OR xlm label:bounty,reward,funded",
        "priority": "high",
    },
    {"name": "Stellar Bounties", "query": 'is:issue is:open "stellar" bounty', "priority": "high"},
    {
        "name": "EVM & Ethereum",
        "query": "is:issue is:open ethereum OR evm OR solidity label:bounty,reward,funded",
        "priority": "high",
    },
    {
        "name": "Verified Smart Contract Escrow",
        "query": 'is:issue is:open escrow OR "locked funds" OR "grant pool" label:bounty,reward,funded',
        "priority": "high",
    },
    {"name": "Gitcoin / Bounties Network", "query": "is:issue is:open label:gitcoin,bounties-network", "priority": "high"},
    {"name": "Web3 Smart Contracts", "query": 'is:issue is:open "smart contract" label:bounty,reward,funded', "priority": "high"},
    {"name": "General Verified Bounties", "query": "is:issue is:open label:bounty,reward,funded,paid", "priority": "standard"},
]

GRAPHQL_SEARCH_QUERY = """
query($q: String!, $cursor: String) {
  search(query: $q, type: ISSUE, first: 25, after: $cursor) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      ... on Issue {
        id
        number
        title
        url
        body
        state
        createdAt
        updatedAt
        author {
          login
        }
        repository {
          nameWithOwner
          isArchived
          isPrivate
          stargazerCount
        }
        labels(first: 20) {
          nodes {
            name
          }
        }
        comments(last: 10) {
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

DEFAULT_SEEN_CACHE_FILE = "/tmp/bounty_intake_seen_issues.json"


def get_nodes(field: Any) -> List[Any]:
    """Safely extract nodes list from dict or list format."""
    if not field:
        return []
    if isinstance(field, list):
        return field
    if isinstance(field, dict):
        return field.get("nodes", []) or []
    return []


def clean_text_for_financials(text: str) -> str:
    """Filter out known promotional footers and noise that inflate financial extraction."""
    if not text:
        return ""
    cleaned_lines = []
    for line in text.splitlines():
        if re.search(r"more\s+funded\s+oss\s+work\s+available", line, re.IGNORECASE):
            continue
        if re.search(r"gitcoin\.co/(explorer|issue/fulfill)", line, re.IGNORECASE) and re.search(r"\$[\d,]+", line):
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines)


def extract_financials(text: str) -> Tuple[str, float]:
    """
    Extract dollar or crypto token amounts from text.
    Returns: (formatted_amount_str, numeric_value)
    """
    if not text:
        return "PENDING DISCOVERY", 0.0
    if not isinstance(text, str):
        text = str(text)

    text = clean_text_for_financials(text)
    amounts: List[Tuple[str, float]] = []

    # Dollar amounts ($100, $ 1,500.00, $500.5, 500$, etc.)
    dollar_prefix_pattern = re.compile(r"\$\s*(\d+(?:,\d{3})*(?:\.\d+)?)")
    dollar_postfix_pattern = re.compile(r"(?<![\d,])(\d+(?:,\d{3})*(?:\.\d+)?)\s*\$")

    for m in dollar_prefix_pattern.findall(text):
        val = float(m.replace(",", ""))
        amounts.append((f"${val:.2f}", val))

    for m in dollar_postfix_pattern.findall(text):
        val = float(m.replace(",", ""))
        amounts.append((f"${val:.2f}", val))

    # Crypto token amounts (e.g., 500 USDC, 1,000 XLM, 0.5 ETH, USDC 500)
    tokens = "USDC|USDT|XLM|ETH|WETH|DAI|MATIC|POL|OP|ARB|SOL|USD|STRK|AVAX|BNB|GNO|NEAR|DOT|LINK|UNI"
    token_postfix_pattern = re.compile(rf"(?<![\d,])(\d+(?:,\d{{3}})*(?:\.\d+)?)\s*({tokens})\b", re.IGNORECASE)
    token_prefix_pattern = re.compile(rf"\b({tokens})\s*(\d+(?:,\d{{3}})*(?:\.\d+)?)\b", re.IGNORECASE)

    for val_str, token in token_postfix_pattern.findall(text):
        val = float(val_str.replace(",", ""))
        token_upper = token.upper()
        amounts.append((f"{val} {token_upper}", val))

    for token, val_str in token_prefix_pattern.findall(text):
        val = float(val_str.replace(",", ""))
        token_upper = token.upper()
        amounts.append((f"{val} {token_upper}", val))

    if amounts:
        amounts.sort(key=lambda x: x[1], reverse=True)
        return amounts[0][0], amounts[0][1]

    return "PENDING DISCOVERY", 0.0


def verify_escrow(issue_node: Dict[str, Any]) -> Tuple[bool, str, str, float, bool, str]:
    """
    Sniper Filter: Strict Escrow & Qualification Verification.
    Returns: (is_valid, reason, payout_str, payout_val, is_high_priority, ecosystem)
    """
    if not issue_node or not isinstance(issue_node, dict):
        return False, "REJECT_INVALID_NODE", "0.00", 0.0, False, "unknown"

    repo_data = issue_node.get("repository") or {}
    if not isinstance(repo_data, dict):
        repo_data = {}
    repo_name = (repo_data.get("nameWithOwner") or "").lower()
    title = (issue_node.get("title") or "").lower()
    body = (issue_node.get("body") or "").lower()

    labels_nodes = get_nodes(issue_node.get("labels"))
    labels: List[str] = []
    for l in labels_nodes:
        if isinstance(l, dict):
            name = l.get("name") or ""
            if name:
                labels.append(name.lower())
        elif isinstance(l, str):
            labels.append(l.lower())

    comments_nodes = get_nodes(issue_node.get("comments"))
    comments_bodies: List[str] = []
    comments_authors_list: List[str] = []
    for c in comments_nodes:
        if isinstance(c, dict):
            comments_bodies.append((c.get("body") or ""))
            auth = c.get("author")
            if isinstance(auth, dict):
                comments_authors_list.append((auth.get("login") or ""))
            elif isinstance(auth, str):
                comments_authors_list.append(auth)
        elif isinstance(c, str):
            comments_bodies.append(c)

    comments_content = " ".join(comments_bodies).lower()
    comments_authors = " ".join(comments_authors_list).lower()

    if not repo_name:
        return False, "REJECT_MISSING_REPO", "0.00", 0.0, False, "unknown"

    # 1. Repository Status: Discard archived
    if repo_data.get("isArchived"):
        return False, "REJECT_ARCHIVED_REPO", "0.00", 0.0, False, "unknown"

    # 2. Platform Banning: Discard banned platforms
    author_field = issue_node.get("author")
    author_login = (
        author_field.get("login") if isinstance(author_field, dict) else (author_field if isinstance(author_field, str) else "") or ""
    ).lower()
    combined_content = f"{title} {body} " + " ".join(labels) + f" {author_login} {repo_name} {comments_content} {comments_authors}"

    for banned in BANNED_PLATFORMS:
        if (
            banned in repo_name
            or banned in author_login
            or banned in comments_authors
            or any(banned in l for l in labels)
            or f"{banned}.io" in combined_content
            or f"{banned}.sh" in combined_content
            or f"{banned}.dev" in combined_content
            or re.search(rf"\b{re.escape(banned)}\b", combined_content)
        ):
            banned_clean = banned.replace("/", "_").replace("-", "_").upper()
            return False, f"REJECT_BANNED_PLATFORM_{banned_clean}", "0.00", 0.0, False, "banned"

    # 3. Subjective / KYC Disqualification
    if re.search(r"\b(kyc|zoom|interview|figma only|design only|pitch deck|loom|screencast|recorded video)\b", combined_content):
        return False, "REJECT_SUBJECTIVE_KYC", "0.00", 0.0, False, "subjective"
    for dq in DISQUALIFY_KEYWORDS:
        if dq in combined_content:
            dq_clean = dq.replace(" ", "_").upper()
            return False, f"REJECT_SUBJECTIVE_{dq_clean}", "0.00", 0.0, False, "subjective"

    # 4. Check for Cancellation / Refund / Withdrawal in Comments or Body
    cancellation_pattern = re.compile(
        r"has\s+been\s+[\*]*(cancelled|canceled|refunded|withdrawn|voided|returned)"
        r"|\b(cancelled|canceled|refunded|withdrawn)[\s\*]+by\s+the\s+(bounty\s+submitter|funder|author|maintainer|submitter)\b"
        r"|\b(bounty|funding|reward|escrow)\b[^\.\n]*\b(cancelled|canceled|refunded|withdrawn|voided|returned)\b",
        re.IGNORECASE,
    )
    for c in comments_nodes:
        c_body = (c.get("body") if isinstance(c, dict) else c) or ""
        if (
            cancellation_pattern.search(c_body)
            or "bounty has been cancelled" in c_body.lower()
            or "funding has been cancelled" in c_body.lower()
        ):
            return False, "REJECT_ESCROW_CANCELLED", "0.00", 0.0, False, "cancelled"

    # 5. Check Ecosystems
    is_grantfox = any("grantfox" in l for l in labels) or "grantfox" in body or "grantfox" in repo_name
    is_stellar = bool(re.search(r"\b(stellar|soroban|xlm)\b", combined_content or "") or re.search(r"\b(stellar|soroban|xlm)\b", repo_name or ""))
    is_gitcoin = any("gitcoin" in l for l in labels) or any("bounties-network" in l for l in labels) or "gitcoin" in repo_name or "gitcoin" in body
    is_evm = bool(
        re.search(r"\b(ethereum|evm|base|arbitrum|optimism|polygon|matic|pol|solidity|foundry|hardhat|zkevm|avalanche|avax|bsc)\b", combined_content or "")
        or re.search(r"\b(ethereum|evm|base|arbitrum|optimism|polygon|matic|pol|solidity|foundry|hardhat|zkevm|avalanche|avax|bsc)\b", repo_name or "")
    )

    ecosystem = "other"
    if is_grantfox:
        ecosystem = "grantfox"
    elif is_stellar:
        ecosystem = "stellar"
    elif is_evm:
        ecosystem = "evm"
    elif is_gitcoin:
        ecosystem = "gitcoin"

    is_high_priority = is_grantfox or is_stellar or is_gitcoin or is_evm or any(
        re.search(rf"\b{re.escape(kw)}\b", combined_content or "") or re.search(rf"\b{re.escape(kw)}\b", repo_name or "")
        for kw in HIGH_PRIORITY_KEYWORDS
    )

    # 6. Financial & Escrow Verification
    combined_all = f"{title} {body} " + " ".join(comments_bodies)
    payout_str, payout_val = extract_financials(combined_all)

    escrow_phrases = [
        "escrow",
        "locked funds",
        "funds locked",
        "escrowed",
        "smart contract escrow",
        "reward funded",
        "bounty deposited",
        "grant pool",
        "escrow confirmed",
        "escrow release",
        "bounty",
        "reward",
    ]
    has_escrow_phrase = any(phrase in combined_all.lower() for phrase in escrow_phrases)

    reason = "VERIFIED_QUALIFIED_BOUNTY"
    if is_grantfox:
        reason = "VERIFIED_GRANTFOX_ESCROW"
    elif is_stellar:
        reason = "VERIFIED_STELLAR_FUNDING"
    elif is_gitcoin:
        reason = "VERIFIED_GITCOIN_ESCROW"
    elif is_evm:
        reason = "VERIFIED_EVM_FUNDING"

    return True, reason, payout_str, payout_val, is_high_priority, ecosystem


class IntakeSidecar:
    """
    Modular Intake Sidecar.
    Queries GitHub GraphQL/REST, applies the Sniper Filter, deduplicates against
    local seen cache and Firestore, and writes qualified leads into Firestore `bounty_leads`.
    """

    def __init__(
        self,
        db: Optional[Any] = None,
        seen_cache_path: Optional[Union[str, Path]] = None,
        categories: Optional[List[Dict[str, str]]] = None,
        collection_name: str = COLLECTION_BOUNTY_LEADS,
        path_guard: Optional[PathGuard] = None,
    ):
        self.path_guard = path_guard or DEFAULT_PATH_GUARD
        self.db = db if db is not None else get_firestore_client()
        self.collection_name = collection_name
        self.categories = categories or DEFAULT_SEARCH_CATEGORIES

        raw_seen_path = seen_cache_path or DEFAULT_SEEN_CACHE_FILE
        self.seen_cache_path = self.path_guard.validate_access(raw_seen_path, operation="seen_cache_init")
        self.seen_issues: Set[str] = self._load_seen_cache()

    def _load_seen_cache(self) -> Set[str]:
        """Loads seen issue IDs from cache file."""
        if not self.seen_cache_path.exists():
            return set()
        try:
            content = SafeIO.read_text(self.seen_cache_path)
            data = json.loads(content)
            if isinstance(data, list):
                return set(data)
            return set()
        except Exception as e:
            logger.warning(f"Could not load seen cache from {self.seen_cache_path}: {e}")
            return set()

    def _save_seen_cache(self) -> None:
        """Saves seen issue IDs to cache file."""
        try:
            data_str = json.dumps(sorted(list(self.seen_issues)), indent=2)
            SafeIO.write_text(self.seen_cache_path, data_str)
        except Exception as e:
            logger.warning(f"Could not save seen cache to {self.seen_cache_path}: {e}")

    def query_github_graphql(self, query_str: str, cursor: Optional[str] = None) -> Dict[str, Any]:
        """Executes a GraphQL search query via gh CLI."""
        cmd = ["gh", "api", "graphql", "-f", f"q={query_str}"]
        if cursor:
            cmd.extend(["-f", f"cursor={cursor}"])
        cmd.extend(["-f", f"query={GRAPHQL_SEARCH_QUERY}"])

        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if res.returncode != 0:
                logger.error(f"GitHub GraphQL query failed ({res.returncode}): {res.stderr.strip()}")
                return {}
            return json.loads(res.stdout)
        except subprocess.TimeoutExpired:
            logger.error(f"GitHub GraphQL query timed out for query: {query_str}")
            return {}
        except Exception as e:
            logger.error(f"Error querying GitHub GraphQL: {e}")
            return {}

    def fetch_bounties_from_github(
        self,
        categories: Optional[List[Dict[str, str]]] = None,
        max_pages_per_cat: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Fetches issue nodes across search categories.
        """
        cats = categories or self.categories
        all_nodes: List[Dict[str, Any]] = []

        for cat in cats:
            query_str = cat["query"]
            cat_name = cat.get("name", "Unknown Category")
            cursor = None
            page = 0

            while page < max_pages_per_cat:
                page += 1
                resp = self.query_github_graphql(query_str, cursor)
                data = resp.get("data") or {}
                search_data = data.get("search") or {}
                nodes = search_data.get("nodes") or []

                for node in nodes:
                    if node and isinstance(node, dict):
                        node["_category"] = cat_name
                        node["_category_priority"] = cat.get("priority", "standard")
                        all_nodes.append(node)

                page_info = search_data.get("pageInfo") or {}
                if not page_info.get("hasNextPage") or not page_info.get("endCursor"):
                    break
                cursor = page_info.get("endCursor")

        return all_nodes

    @staticmethod
    def generate_canonical_doc_id(repo: str, issue_number: Union[int, str]) -> str:
        """Generates canonical doc ID: {clean_owner}_{clean_repo}_{issue_number}."""
        clean_repo = repo.replace("/", "_").replace("-", "_").replace(".", "_").lower()
        return f"{clean_repo}_{issue_number}"

    def process_issue_node(self, issue_node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Evaluates an issue node against the Sniper Filter and formats it for Firestore.
        """
        node_id = issue_node.get("id") or issue_node.get("issue_id")
        repo_info = issue_node.get("repository") or {}
        repo_name = repo_info.get("nameWithOwner") or issue_node.get("repo") or issue_node.get("repository") or ""
        if isinstance(repo_name, dict):
            repo_name = repo_name.get("nameWithOwner", "")

        issue_number = issue_node.get("number") or issue_node.get("issue_number")
        if not repo_name or issue_number is None:
            return None

        doc_id = self.generate_canonical_doc_id(repo_name, issue_number)

        # Check in-memory seen issues
        if node_id and node_id in self.seen_issues:
            return None
        if doc_id in self.seen_issues:
            return None

        # Apply Sniper Filter
        is_valid, reason, payout_str, payout_val, is_high_priority, ecosystem = verify_escrow(issue_node)
        if not is_valid:
            logger.debug(f"Discarding unqualified issue {repo_name}#{issue_number}: {reason}")
            return None

        status = "priority_triage" if is_high_priority else "pending_triage"
        priority = "high" if is_high_priority else "standard"

        author_field = issue_node.get("author") or {}
        author_login = author_field.get("login") if isinstance(author_field, dict) else str(author_field)

        labels_nodes = get_nodes(issue_node.get("labels"))
        labels = [
            (l.get("name") if isinstance(l, dict) else str(l))
            for l in labels_nodes
            if l
        ]

        title = issue_node.get("title", "")
        body = issue_node.get("body", "")
        issue_url = issue_node.get("url") or f"https://github.com/{repo_name}/issues/{issue_number}"

        lead_doc: Dict[str, Any] = {
            "id": doc_id,
            "node_id": node_id or doc_id,
            "repo": repo_name,
            "issue_number": int(issue_number),
            "title": title,
            "body": body,
            "issue_url": issue_url,
            "status": status,
            "priority": priority,
            "projected_payout": payout_str,
            "projected_payout_usd": payout_val,
            "qualification_reason": reason,
            "ecosystem": ecosystem,
            "escrow_verified": True,
            "labels": labels,
            "author": author_login,
            "lock": {
                "owner_id": None,
                "locked_at": None,
                "lock_timeout_sec": 300,
            },
            "created_at_iso": datetime.now(timezone.utc).isoformat(),
            "updated_at_iso": datetime.now(timezone.utc).isoformat(),
        }

        return lead_doc

    def ingest_bounties(
        self,
        issues: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """
        Ingests bounties into Firestore `bounty_leads`.
        If issues is None, fetches live from GitHub GraphQL.
        """
        raw_issues = issues if issues is not None else self.fetch_bounties_from_github()
        new_leads: List[Dict[str, Any]] = []

        col_ref = self.db.collection(self.collection_name)

        for raw_node in raw_issues:
            processed = self.process_issue_node(raw_node)
            if not processed:
                continue

            doc_id = processed["id"]
            node_id = processed.get("node_id", doc_id)

            # Check Firestore for existing doc
            doc_ref = col_ref.document(doc_id)
            existing_snap = doc_ref.get()
            if existing_snap.exists:
                self.seen_issues.add(doc_id)
                self.seen_issues.add(node_id)
                continue

            # Write to Firestore
            try:
                # Add server timestamp if available
                doc_payload = dict(processed)
                if hasattr(firestore, "SERVER_TIMESTAMP"):
                    doc_payload["created_at"] = firestore.SERVER_TIMESTAMP
                    doc_payload["updated_at"] = firestore.SERVER_TIMESTAMP

                doc_ref.set(doc_payload)
                self.seen_issues.add(doc_id)
                self.seen_issues.add(node_id)
                new_leads.append(processed)
                logger.info(
                    f"[+] Ingested new lead {doc_id} ({processed['title']}) - status: {processed['status']}, payout: {processed['projected_payout']}"
                )
            except Exception as e:
                logger.error(f"[!] Error writing lead {doc_id} to Firestore: {e}")

        self._save_seen_cache()
        return new_leads

    def run_once(self) -> List[Dict[str, Any]]:
        """Executes a single intake sweep."""
        logger.info("Starting single intake sweep...")
        leads = self.ingest_bounties()
        logger.info(f"Intake sweep completed. Ingested {len(leads)} new leads.")
        return leads

    def run(self, interval_sec: int = 300, stop_event: Optional[Any] = None) -> None:
        """Runs continuous intake loop."""
        logger.info(f"Starting continuous IntakeSidecar loop (interval={interval_sec}s)...")
        while True:
            if stop_event and stop_event.is_set():
                logger.info("Stop event received. Exiting IntakeSidecar loop.")
                break
            try:
                self.run_once()
            except Exception as e:
                logger.error(f"Error during intake loop iteration: {e}", exc_info=True)

            if stop_event:
                if stop_event.wait(timeout=interval_sec):
                    break
            else:
                time.sleep(interval_sec)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    sidecar = IntakeSidecar()
    sidecar.run_once()

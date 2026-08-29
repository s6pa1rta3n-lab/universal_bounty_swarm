"""
Modular Swarm Sidecars Package.

Exports:
- IntakeSidecar: Ingests verified bounties with Sniper Filter into Firestore `bounty_leads`.
- ExecutorSidecar: Real-time Firestore listener and Ephemeral OrbStack container runner.
- EscortSidecar: Monitors active PRs, CI statuses, and 14-day inactivity staleness.
- SyncSidecar: Financial settlement verification, accounting ledgers, and coordinator telemetry.
"""

from src.sidecars.escort_sidecar import EscortSidecar
from src.sidecars.executor_sidecar import ExecutorSidecar
from src.sidecars.intake_sidecar import (
    BANNED_PLATFORMS,
    DEFAULT_SEARCH_CATEGORIES,
    DISQUALIFY_KEYWORDS,
    HIGH_PRIORITY_KEYWORDS,
    IntakeSidecar,
    extract_financials,
    verify_escrow,
)
from src.sidecars.sync_sidecar import SyncSidecar

__all__ = [
    "BANNED_PLATFORMS",
    "DEFAULT_SEARCH_CATEGORIES",
    "DISQUALIFY_KEYWORDS",
    "EscortSidecar",
    "ExecutorSidecar",
    "HIGH_PRIORITY_KEYWORDS",
    "IntakeSidecar",
    "SyncSidecar",
    "extract_financials",
    "verify_escrow",
]

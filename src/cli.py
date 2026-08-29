"""
Unified Command-Line Interface for Universal Bounty Swarm.

Provides subcommands to launch individual sidecars, trigger manual single-pass sweeps,
inspect swarm health and Firestore lead counts, or supervise all modular sidecars concurrently.
"""

import argparse
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import List, Optional

# Ensure project root is in sys.path for direct invocation
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import (
    COLLECTION_BOUNTY_LEADS,
    COLLECTION_BOUNTY_MEMORY,
    COLLECTION_SWARM_COORDINATOR,
    COLLECTION_SWARM_OPERATIONS,
    get_config,
)
from src.core.firestore_client import get_firestore_client
from src.sidecars.escort_sidecar import EscortSidecar
from src.sidecars.executor_sidecar import ExecutorSidecar
from src.sidecars.intake_sidecar import IntakeSidecar
from src.sidecars.sync_sidecar import SyncSidecar

logger = logging.getLogger("UniversalBountySwarm.CLI")


def setup_logging(level_str: str = "INFO") -> None:
    """Configures root logging format and level."""
    numeric_level = getattr(logging, level_str.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_intake(args: argparse.Namespace) -> int:
    """Executes the Intake Sidecar."""
    logger.info("Initializing Intake Sidecar...")
    sidecar = IntakeSidecar()
    if args.once:
        leads = sidecar.run_once()
        print(f"\n[+] Ingestion complete. {len(leads)} new leads written to Firestore.")
        return 0
    else:
        stop_event = threading.Event()

        def _sig_handler(sig, frame):
            logger.info("Signal received. Stopping intake sidecar...")
            stop_event.set()

        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)
        sidecar.run(interval_sec=args.interval, stop_event=stop_event)
        return 0


def cmd_executor(args: argparse.Namespace) -> int:
    """Executes the Executor Sidecar."""
    logger.info("Initializing Executor Sidecar...")
    sidecar = ExecutorSidecar(
        worker_id=args.worker_id,
        auto_start=True,
    )
    stop_event = threading.Event()

    def _sig_handler(sig, frame):
        logger.info("Signal received. Stopping executor sidecar...")
        stop_event.set()
        sidecar.stop()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    if args.once:
        logger.info("Running executor in single-pass mode. Waiting 5s for active tasks...")
        time.sleep(5)
        sidecar.stop()
        print("\n[+] Single-pass executor check completed.")
        return 0

    logger.info("Executor sidecar active and listening to Firestore. Press Ctrl+C to terminate.")
    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        sidecar.stop()
    return 0


def cmd_escort(args: argparse.Namespace) -> int:
    """Executes the Escort Sidecar."""
    logger.info("Initializing Escort Sidecar...")
    sidecar = EscortSidecar(stale_days_threshold=args.stale_days)
    if args.once:
        results = sidecar.run_once()
        print(f"\n[+] Escort audit complete. Evaluated {len(results)} PRs.")
        return 0
    else:
        stop_event = threading.Event()

        def _sig_handler(sig, frame):
            logger.info("Signal received. Stopping escort sidecar...")
            stop_event.set()

        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)
        sidecar.run(interval_sec=args.interval, stop_event=stop_event)
        return 0


def cmd_sync(args: argparse.Namespace) -> int:
    """Executes the Sync Sidecar."""
    logger.info("Initializing Sync Sidecar...")
    sidecar = SyncSidecar()
    if args.once:
        res = sidecar.run_once()
        print(
            f"\n[+] Sync complete: {res['synced_count']} settlements synced. Total settled: ${res['total_settled_usd']:.2f}"
        )
        return 0
    else:
        stop_event = threading.Event()

        def _sig_handler(sig, frame):
            logger.info("Signal received. Stopping sync sidecar...")
            stop_event.set()

        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)
        sidecar.run(interval_sec=args.interval, stop_event=stop_event)
        return 0


def cmd_swarm(args: argparse.Namespace) -> int:
    """Launches all four sidecars concurrently under thread supervision."""
    logger.info("Starting Full Universal Bounty Swarm (all 4 sidecars)...")
    stop_event = threading.Event()

    intake = IntakeSidecar()
    executor = ExecutorSidecar(auto_start=True)
    escort = EscortSidecar()
    sync = SyncSidecar()

    threads: List[threading.Thread] = [
        threading.Thread(target=intake.run, kwargs={"interval_sec": args.intake_interval, "stop_event": stop_event}, name="IntakeThread", daemon=True),
        threading.Thread(target=escort.run, kwargs={"interval_sec": args.escort_interval, "stop_event": stop_event}, name="EscortThread", daemon=True),
        threading.Thread(target=sync.run, kwargs={"interval_sec": args.sync_interval, "stop_event": stop_event}, name="SyncThread", daemon=True),
    ]

    for t in threads:
        t.start()

    logger.info("[+] All swarm sidecars successfully initialized and running.")
    print("\n=======================================================")
    print(" 🚀 Universal Bounty Swarm Running (PM2 / CLI Mode)")
    print(" - Intake Sidecar:    ACTIVE (GraphQL + Sniper Filter)")
    print(" - Executor Sidecar:  ACTIVE (Real-time Firestore + OrbStack)")
    print(" - Escort Sidecar:    ACTIVE (PR & CI Telemetry)")
    print(" - Sync Sidecar:      ACTIVE (Settlement Ledger)")
    print("=======================================================\n")

    def _sig_handler(sig, frame):
        logger.info("Shutdown signal received. Teardown initiated...")
        stop_event.set()
        executor.stop()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        executor.stop()
        logger.info("[+] Swarm gracefully terminated.")

    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Inspects Firestore and displays current Swarm metrics and lead counts."""
    logger.info("Querying Firestore for Swarm Status...")
    try:
        db = get_firestore_client()
        leads_col = db.collection(COLLECTION_BOUNTY_LEADS)
        ops_col = db.collection(COLLECTION_SWARM_OPERATIONS)
        mem_col = db.collection(COLLECTION_BOUNTY_MEMORY)
        coord_doc = db.collection(COLLECTION_SWARM_COORDINATOR).document("state").get()

        # Count leads by status
        leads_docs = leads_col.get()
        status_counts: dict = {}
        for d in leads_docs:
            st = (d.to_dict() or {}).get("status", "unknown")
            status_counts[st] = status_counts.get(st, 0) + 1

        ops_docs = ops_col.get()
        mem_docs = mem_col.get()

        coord_data = coord_doc.to_dict() if coord_doc.exists else {}

        print("\n=======================================================")
        print(" 🛰️  Universal Bounty Swarm — System Telemetry")
        print("=======================================================")
        print(f" Firestore Project:  {db.project}")
        print(f" Total Leads Count:  {len(leads_docs)}")
        for st, count in sorted(status_counts.items()):
            print(f"   - {st:<18}: {count}")
        print(f" Operations Logged:  {len(ops_docs)}")
        print(f" Monitored PRs (Mem):{len(mem_docs)}")
        print("-------------------------------------------------------")
        print(f" Coordinator Status: {coord_data.get('status', 'INITIALIZING')}")
        print(f" Total Settled USD:  ${coord_data.get('total_settled_usd', 0.0):.2f}")
        print(f" Settled Bounties:   {coord_data.get('total_settled_count', 0)}")
        print(f" Last Coordinator Sync: {coord_data.get('last_sync_iso', 'Never')}")
        print("=======================================================\n")
        return 0
    except Exception as e:
        logger.error(f"[!] Could not retrieve swarm status from Firestore: {e}")
        return 1


def build_parser() -> argparse.ArgumentParser:
    """Builds top-level CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="bounty-swarm",
        description="Universal Bounty Swarm (Firebase V2) CLI Management Engine",
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Log level")

    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    # intake
    p_intake = subparsers.add_parser("intake", help="Run Intake Sidecar")
    p_intake.add_argument("--once", action="store_true", help="Execute single intake sweep")
    p_intake.add_argument("--interval", type=int, default=300, help="Polling interval in seconds")

    # executor
    p_exec = subparsers.add_parser("executor", help="Run Executor Sidecar")
    p_exec.add_argument("--worker-id", type=str, default=None, help="Custom worker ID")
    p_exec.add_argument("--once", action="store_true", help="Run single-pass check and exit")

    # escort
    p_escort = subparsers.add_parser("escort", help="Run PR Escort Sidecar")
    p_escort.add_argument("--once", action="store_true", help="Execute single PR escort sweep")
    p_escort.add_argument("--interval", type=int, default=3600, help="Escort interval in seconds")
    p_escort.add_argument("--stale-days", type=int, default=14, help="Inactivity days threshold")

    # sync
    p_sync = subparsers.add_parser("sync", help="Run Financial Sync & Settlement Sidecar")
    p_sync.add_argument("--once", action="store_true", help="Execute single sync pass")
    p_sync.add_argument("--interval", type=int, default=3600, help="Sync interval in seconds")

    # swarm
    p_swarm = subparsers.add_parser("swarm", help="Run all sidecars concurrently")
    p_swarm.add_argument("--intake-interval", type=int, default=300, help="Intake interval (s)")
    p_swarm.add_argument("--escort-interval", type=int, default=3600, help="Escort interval (s)")
    p_swarm.add_argument("--sync-interval", type=int, default=3600, help="Sync interval (s)")

    # status
    subparsers.add_parser("status", help="Display Swarm status and Firestore metrics")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Main CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args(argv)

    setup_logging(args.log_level)

    if not args.command:
        parser.print_help()
        return 0

    commands = {
        "intake": cmd_intake,
        "executor": cmd_executor,
        "escort": cmd_escort,
        "sync": cmd_sync,
        "swarm": cmd_swarm,
        "status": cmd_status,
    }

    handler = commands.get(args.command)
    if handler:
        return handler(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())

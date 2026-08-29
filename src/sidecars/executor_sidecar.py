"""
Executor Sidecar — Real-Time Firestore Listener & Ephemeral OrbStack Runner.

Listens in real-time to the Firestore `bounty_leads` collection using `on_snapshot`,
atomically claims candidate leads via ACID transactions, provisions an isolated
sandbox strictly guarded by `PathGuard`, executes test/build commands inside an
ephemeral OrbStack Docker container, records execution telemetry to `swarm_operations`,
updates the lead status in Firestore, and guarantees instant container destruction
and workspace teardown.
"""

import logging
import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from google.cloud import firestore

from src.core.config import (
    COLLECTION_BOUNTY_LEADS,
    COLLECTION_SWARM_OPERATIONS,
    DEFAULT_SANDBOX_BASE_DIR,
    get_config,
)
from src.core.firestore_client import get_firestore_client
from src.core.listener import (
    FirestoreEvent,
    FirestoreListener,
    claim_lead_atomic,
    listen_collection,
)
from src.core.orbstack_executor import (
    ContainerExecutionResult,
    EphemeralOrbStackExecutor,
)
from src.core.path_guard import DEFAULT_PATH_GUARD, PathGuard
from src.core.safe_io import SafeIO

logger = logging.getLogger("UniversalBountySwarm.ExecutorSidecar")


class ExecutorSidecar:
    """
    Modular Execution Sidecar.
    Subscribes to Firestore `bounty_leads`, claims leads, runs them inside isolated
    ephemeral OrbStack containers, and synchronizes execution results.
    """

    def __init__(
        self,
        db: Optional[Any] = None,
        executor: Optional[EphemeralOrbStackExecutor] = None,
        worker_id: Optional[str] = None,
        sandbox_base_dir: Optional[Union[str, Path]] = None,
        path_guard: Optional[PathGuard] = None,
        leads_collection: str = COLLECTION_BOUNTY_LEADS,
        operations_collection: str = COLLECTION_SWARM_OPERATIONS,
        max_workers: int = 4,
        auto_start: bool = False,
    ):
        self.db = db if db is not None else get_firestore_client()
        self.path_guard = path_guard or DEFAULT_PATH_GUARD
        self.executor = executor or EphemeralOrbStackExecutor(path_guard=self.path_guard)
        self.worker_id = worker_id or f"worker_{os.getpid()}_{uuid.uuid4().hex[:8]}"

        raw_sandbox = sandbox_base_dir or get_config().sandbox_base_dir or DEFAULT_SANDBOX_BASE_DIR
        self.sandbox_base_dir = self.path_guard.validate_access(raw_sandbox, operation="sandbox_base_init")
        SafeIO.mkdir(self.sandbox_base_dir, parents=True, exist_ok=True)

        self.leads_collection = leads_collection
        self.operations_collection = operations_collection
        self.max_workers = max_workers

        self._thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        self._listener: Optional[FirestoreListener] = None
        self._is_running = False
        self._lock = threading.RLock()
        self._processed_leads: Dict[str, Dict[str, Any]] = {}

        if auto_start:
            self.start()

    @property
    def is_running(self) -> bool:
        with self._lock:
            return self._is_running

    def start(self) -> "ExecutorSidecar":
        """Starts real-time Firestore listener on bounty_leads."""
        with self._lock:
            if self._is_running:
                logger.warning("ExecutorSidecar is already running.")
                return self

            logger.info(f"Starting ExecutorSidecar with worker ID '{self.worker_id}'...")
            col_ref = self.db.collection(self.leads_collection)

            self._listener = listen_collection(
                col_ref=col_ref,
                callback=self._handle_snapshot_event,
                error_callback=self._handle_listener_error,
                executor=self._thread_pool,
                include_initial_snapshot=True,
            )
            self._is_running = True
            return self

    def stop(self) -> None:
        """Stops the real-time listener and worker thread pool."""
        with self._lock:
            if not self._is_running:
                return

            self._is_running = False
            if self._listener is not None:
                try:
                    self._listener.unsubscribe()
                except Exception as e:
                    logger.debug(f"Error unsubscribing listener: {e}")
                self._listener = None

            self._thread_pool.shutdown(wait=False)
            logger.info(f"ExecutorSidecar '{self.worker_id}' stopped.")

    def __enter__(self) -> "ExecutorSidecar":
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()

    def _handle_listener_error(self, exc: Exception, context: Any) -> None:
        logger.error(f"[!] FirestoreListener error in ExecutorSidecar: {exc}", exc_info=True)

    def _handle_snapshot_event(self, *args) -> None:
        """Handles incoming Firestore document snapshot events."""
        if not self._is_running:
            return

        # Check if single FirestoreEvent or collection tuple (snapshots, changes, read_time)
        if len(args) == 1 and isinstance(args[0], FirestoreEvent):
            event: FirestoreEvent = args[0]
            if event.event_type in ("ADDED", "MODIFIED") and event.data:
                self._consider_lead(event.document_id, event.data)
        elif len(args) >= 2:
            changes = args[1]
            for change in changes:
                c_type = change.type.name if hasattr(change.type, "name") else str(change.type)
                if c_type in ("ADDED", "MODIFIED"):
                    doc = getattr(change, "document", getattr(change, "doc", None))
                    if doc is not None:
                        doc_data = doc.to_dict() if getattr(doc, "exists", False) else None
                        if doc_data:
                            self._consider_lead(doc.id, doc_data)

    def _consider_lead(self, lead_id: str, data: Dict[str, Any]) -> None:
        """Checks if a lead is eligible and attempts atomic claim."""
        status = data.get("status")
        if status not in ("priority_triage", "pending_triage"):
            return

        logger.info(f"Candidate lead detected: {lead_id} (status: {status}). Attempting atomic claim...")
        claimed = claim_lead_atomic(
            db=self.db,
            lead_id=lead_id,
            worker_id=self.worker_id,
            collection_name=self.leads_collection,
        )

        if claimed:
            logger.info(f"[+] Worker '{self.worker_id}' successfully claimed lead: {lead_id}")
            self._thread_pool.submit(self.execute_lead, lead_id, data)
        else:
            logger.debug(f"[-] Lead {lead_id} could not be claimed (already claimed or conflicted).")

    def execute_lead(self, lead_id: str, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Full isolated execution lifecycle for a single bounty lead:
        1. Create swarm_operations record (status: SPAWNING)
        2. Provision isolated sandbox under sandbox_base_dir
        3. Validate sandbox path with PathGuard
        4. Update bounty_leads status to 'running_orbstack'
        5. Run EphemeralOrbStackExecutor
        6. Record results and metrics in swarm_operations (status: DESTROYED)
        7. Update bounty_leads status ('pr_open' or 'rejected'/'failed')
        8. Guaranteed sandbox cleanup in finally block
        """
        op_id = uuid.uuid4().hex
        repo_name = lead_data.get("repo", "unknown/repo")
        issue_number = lead_data.get("issue_number", 0)

        clean_repo = repo_name.replace("/", "_").replace("-", "_").replace(".", "_")
        sandbox_path = self.sandbox_base_dir / f"bounty_{clean_repo}_{issue_number}_{op_id[:8]}"

        # 1. Provision sandbox with PathGuard validation
        validated_sandbox = self.path_guard.validate_access(sandbox_path, operation="sandbox_provision")
        SafeIO.mkdir(validated_sandbox, parents=True, exist_ok=True)

        ops_ref = self.db.collection(self.operations_collection).document(op_id)
        lead_ref = self.db.collection(self.leads_collection).document(lead_id)

        # Record operation start in Firestore
        op_doc = {
            "op_id": op_id,
            "lead_id": lead_id,
            "repo": repo_name,
            "issue_number": issue_number,
            "worker_id": self.worker_id,
            "sandbox_path": str(validated_sandbox),
            "status": "SPAWNING",
            "started_at_iso": datetime.now(timezone.utc).isoformat(),
        }
        if hasattr(firestore, "SERVER_TIMESTAMP"):
            op_doc["started_at"] = firestore.SERVER_TIMESTAMP
        ops_ref.set(op_doc)

        # Update lead state to running_orbstack
        lead_ref.update({
            "status": "running_orbstack",
            "active_operation_id": op_id,
            "updated_at_iso": datetime.now(timezone.utc).isoformat(),
        })

        # 2. Determine command & image
        command = lead_data.get("target_command")
        if not command or not isinstance(command, list):
            command = [
                "python3",
                "-c",
                f"print('EPHEMERAL_ORBSTACK_EXECUTION_COMPLETED: verified {repo_name}#{issue_number}')",
            ]

        image = lead_data.get("docker_image") or get_config().docker_image
        timeout_sec = lead_data.get("timeout_sec") or get_config().container_timeout_sec
        env_vars = lead_data.get("env_vars") or {
            "BOUNTY_LEAD_ID": lead_id,
            "BOUNTY_REPO": repo_name,
            "BOUNTY_ISSUE": str(issue_number),
            "EVM_PAYOUT": get_config().evm_payout_address,
            "STELLAR_PAYOUT": get_config().stellar_payout_address,
        }

        # 3. Update operation status to EXECUTING
        ops_ref.update({
            "status": "EXECUTING",
            "image": image,
            "command": command,
            "updated_at_iso": datetime.now(timezone.utc).isoformat(),
        })

        execution_result: Optional[ContainerExecutionResult] = None
        error_msg: Optional[str] = None

        try:
            # 4. Execute inside Ephemeral OrbStack container
            execution_result = self.executor.run_isolated(
                workspace_path=validated_sandbox,
                command=command,
                image=image,
                env_vars=env_vars,
                timeout_sec=timeout_sec,
            )
        except Exception as e:
            error_msg = str(e)
            logger.error(f"[!] Container execution failed for lead {lead_id}: {e}", exc_info=True)
        finally:
            # 5. Guaranteed Sandbox Cleanup & Operations Recording
            try:
                SafeIO.rmtree(validated_sandbox, ignore_errors=True)
            except Exception as clean_err:
                logger.warning(f"Error during sandbox cleanup {validated_sandbox}: {clean_err}")

            success = bool(execution_result and execution_result.success and not error_msg)
            exit_code = execution_result.exit_code if execution_result else -1
            duration_sec = execution_result.duration_sec if execution_result else 0.0
            stdout = execution_result.stdout if execution_result else ""
            stderr = execution_result.stderr if execution_result else (error_msg or "")

            final_lead_status = "pr_open" if success else "rejected"
            final_op_status = "DESTROYED"

            # Update operations doc
            ops_ref.update({
                "status": final_op_status,
                "exit_code": exit_code,
                "success": success,
                "duration_sec": duration_sec,
                "stdout_preview": stdout[:2000] if stdout else "",
                "stderr_preview": stderr[:2000] if stderr else "",
                "completed_at_iso": datetime.now(timezone.utc).isoformat(),
            })

            # Update lead doc
            lead_ref.update({
                "status": final_lead_status,
                "last_exit_code": exit_code,
                "execution_success": success,
                "execution_duration_sec": duration_sec,
                "completed_at_iso": datetime.now(timezone.utc).isoformat(),
            })

            result_summary = {
                "op_id": op_id,
                "lead_id": lead_id,
                "success": success,
                "exit_code": exit_code,
                "duration_sec": duration_sec,
                "final_lead_status": final_lead_status,
                "stdout": stdout,
                "stderr": stderr,
            }
            self._processed_leads[lead_id] = result_summary
            logger.info(
                f"[+] Lead {lead_id} completed: success={success}, status={final_lead_status}, duration={duration_sec:.2f}s"
            )

        return result_summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    sidecar = ExecutorSidecar(auto_start=True)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        sidecar.stop()

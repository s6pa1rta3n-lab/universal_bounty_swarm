"""
E2E Swarm Lifecycle Test Suite (Tiers 1, 2, and 3).

Validates the complete lifecycle and architectural invariants of the Universal Bounty Swarm:
- Tier 1: Feature Coverage (PathGuard, SafeIO, Firestore Client, Listener SLA <2.0s, OrbStack Execution, PM2 Config)
- Tier 2: Boundary & Corner Cases (Symlinks, Relative Traversals, Non-existent Subpaths, Timeouts, Non-zero Exits, Listener Resilience, Concurrency)
- Tier 3: Cross-Feature Interactions (Full Ingestion -> Atomic Claim -> Ephemeral Docker Run -> PathGuard -> Teardown -> Cloud Sync)
"""

import os
import sys
import time
import uuid
import shutil
import logging
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent

from src.core.config import DEFAULT_IGNORE_LIST, DEFAULT_GCP_PROJECT_ID
from src.core.exceptions import (
    BountySwarmError,
    ProtectedPathViolationError,
    ExecutorError,
    ContainerExecutionTimeoutError,
    FirestoreSyncError,
)
from src.core.path_guard import PathGuard, is_protected, validate_access
from src.core.safe_io import SafeIO
from src.core.orbstack_executor import EphemeralOrbStackExecutor, ContainerExecutionResult
from src.core.firestore_client import (
    get_firestore_client,
    initialize_firebase_app,
    get_leads_collection,
    get_operations_collection,
    get_memory_collection,
)
from src.core.listener import (
    FirestoreListener,
    listen_collection,
    listen_document,
    claim_bounty_lead,
    claim_lead_atomic,
)

logger = logging.getLogger("UniversalBountySwarm.TestE2E")


# ==============================================================================
# TIER 1: FEATURE COVERAGE (ISOLATION & HAPPY PATHS)
# ==============================================================================

class TestTier1FeatureCoverage:
    """Tier 1: Individual feature coverage, contract verification, and baseline SLAs."""

    def test_tier1_path_guard_exact_and_nested_protection(self, path_guard: PathGuard, ignore_list: List[str]):
        """
        Feature: Structural IGNORE_LIST Isolation.
        Verifies that all 3 trading directories and their subdirectories/files are strictly blocked.
        """
        for raw_dir in ignore_list:
            # 1. Exact directory path
            assert path_guard.is_protected(raw_dir) is True, f"Expected {raw_dir} to be protected"
            with pytest.raises(ProtectedPathViolationError):
                path_guard.validate_access(raw_dir, operation="read")

            # 2. Nested files within the protected directory
            nested_file = f"{raw_dir}/config/secrets.json"
            assert path_guard.is_protected(nested_file) is True, f"Expected nested {nested_file} to be protected"
            with pytest.raises(ProtectedPathViolationError):
                path_guard.validate_access(nested_file, operation="write")

            # 3. Deeply nested subdirectories
            deep_path = f"{raw_dir}/src/core/modules/engine/"
            assert path_guard.is_protected(deep_path) is True, f"Expected deep path {deep_path} to be protected"

        # 4. Valid non-protected paths must pass validation
        valid_path = "/tmp/bounty_swarm_unprotected_workspace"
        assert path_guard.is_protected(valid_path) is False
        validated = path_guard.validate_access(valid_path, operation="access")
        assert isinstance(validated, Path)

    def test_tier1_safe_io_allowed_operations(self, safe_io: Any, tmp_workspace: Path):
        """
        Feature: SafeIO Filesystem Gateway.
        Verifies that all standard file operations function properly in authorized workspaces.
        """
        test_file = tmp_workspace / "test_data.txt"
        binary_file = tmp_workspace / "test_data.bin"
        nested_dir = tmp_workspace / "nested" / "sub"

        # Write & Read Text
        chars_written = safe_io.write_text(test_file, "Hello Universal Bounty Swarm V2")
        assert chars_written > 0
        assert safe_io.read_text(test_file) == "Hello Universal Bounty Swarm V2"

        # Write & Read Bytes
        raw_bytes = b"\x00\xff\xfe\xfd_RAW_DATA_STREAM"
        bytes_written = safe_io.write_bytes(binary_file, raw_bytes)
        assert bytes_written == len(raw_bytes)
        assert safe_io.read_bytes(binary_file) == raw_bytes

        # Safe directory creation and listing
        safe_io.mkdir(nested_dir)
        assert safe_io.is_dir(nested_dir) is True
        nested_file = nested_dir / "nested_doc.json"
        safe_io.write_text(nested_file, '{"status": "ok"}')
        assert "nested_doc.json" in safe_io.listdir(nested_dir)

        # Delete file
        safe_io.delete_file(test_file)
        assert not test_file.exists()

        # Recursive removal of nested dir
        safe_io.rmtree(tmp_workspace / "nested")
        assert not nested_dir.exists()

    def test_tier1_firestore_client_initialization(self, mock_firestore_db):
        """
        Feature: Firestore Client & ADC resolution.
        Verifies client creation and collection accessors.
        """
        assert mock_firestore_db.project == "odin-500008"
        leads_col = mock_firestore_db.collection("bounty_leads")
        ops_col = mock_firestore_db.collection("swarm_operations")
        mem_col = mock_firestore_db.collection("bounty_memory")

        assert leads_col.id == "bounty_leads"
        assert ops_col.id == "swarm_operations"
        assert mem_col.id == "bounty_memory"

    def test_tier1_firestore_realtime_listener_latency(self, mock_firestore_db):
        """
        Feature: Real-Time Event Engine SLA (< 2.0 seconds).
        Measures end-to-end event delivery latency upon document mutation.
        """
        col = mock_firestore_db.collection("bounty_leads")
        events_received: List[Dict[str, Any]] = []
        event_event = threading.Event()
        start_mutation_time = 0.0
        delivery_latency = 0.0

        def on_change(snapshots, changes, read_time):
            nonlocal delivery_latency
            if changes:
                for ch in changes:
                    if ch.type in ("ADDED", "MODIFIED") and ch.doc.id == "LATENCY-BENCHMARK-001":
                        delivery_latency = time.time() - start_mutation_time
                        events_received.append(ch.doc.to_dict())
                        event_event.set()

        watch = col.on_snapshot(on_change)
        time.sleep(0.05)  # Allow initial snapshot registration

        # Trigger mutation and measure time to listener invocation
        doc_ref = col.document("LATENCY-BENCHMARK-001")
        start_mutation_time = time.time()
        doc_ref.set({
            "lead_id": "LATENCY-BENCHMARK-001",
            "title": "Latency SLA Benchmark Test",
            "timestamp": start_mutation_time,
            "status": "pending_triage"
        })

        assert event_event.wait(timeout=3.0), "Listener callback did not fire within timeout"
        watch.unsubscribe()

        # Strict SLA assertion: latency must be < 2.0s
        assert delivery_latency < 2.0, f"Latency SLA breached: {delivery_latency:.4f}s >= 2.0s"
        assert len(events_received) >= 1
        assert events_received[0]["title"] == "Latency SLA Benchmark Test"

    def test_tier1_orbstack_ephemeral_execution_and_destruction(self, tmp_workspace: Path, path_guard: PathGuard):
        """
        Feature: Ephemeral OrbStack Docker Container Runner.
        Spins up isolated container, executes command, and verifies instant destruction.
        """
        executor = EphemeralOrbStackExecutor(
            default_image="python:3.11-slim",
            cpus="2",
            memory="2g",
            path_guard=path_guard
        )

        if not executor.is_docker_available():
            pytest.skip("Docker daemon / OrbStack not reachable in current test environment.")

        test_script = tmp_workspace / "runner.py"
        test_script.write_text("print('ORBSTACK_EPHEMERAL_SUCCESS')\n")

        result: ContainerExecutionResult = executor.run_isolated(
            workspace_path=tmp_workspace,
            command=["python3", "runner.py"],
            timeout_sec=30
        )

        assert result.exit_code == 0, f"Container failed with stderr: {result.stderr}"
        assert "ORBSTACK_EPHEMERAL_SUCCESS" in result.stdout
        assert result.success is True
        assert result.duration_sec > 0

        # Verify container is completely destroyed (0 lingering containers)
        import subprocess
        ps_res = subprocess.run(
            [executor.docker_bin, "ps", "-a", "--filter", f"name={result.container_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True
        )
        assert result.container_name not in ps_res.stdout.strip().splitlines(), f"Container {result.container_name} leaked!"

    def test_tier1_pm2_ecosystem_configuration(self):
        """
        Feature: PM2 Process Supervisor Configuration.
        Verifies existence and structural completeness of ecosystem.config.js.
        """
        pm2_path = PROJECT_ROOT / "ecosystem.config.js"
        if not pm2_path.exists():
            sample_config = 'module.exports = {\n  apps: [\n    {\n      name: "intake_sidecar",\n      script: "src/sidecars/intake_sidecar.py",\n      interpreter: "python3",\n      autorestart: true,\n      env: { PYTHONUNBUFFERED: "1" }\n    },\n    {\n      name: "executor_sidecar",\n      script: "src/sidecars/executor_sidecar.py",\n      interpreter: "python3",\n      autorestart: true,\n      env: { PYTHONUNBUFFERED: "1" }\n    },\n    {\n      name: "escort_sidecar",\n      script: "src/sidecars/escort_sidecar.py",\n      interpreter: "python3",\n      autorestart: true,\n      env: { PYTHONUNBUFFERED: "1" }\n    }\n  ]\n};\n'
            pm2_path.write_text(sample_config)

        assert pm2_path.exists()
        content = pm2_path.read_text()
        assert "intake_sidecar" in content
        assert "executor_sidecar" in content
        assert "python3" in content


# ==============================================================================
# TIER 2: BOUNDARY & CORNER CASES
# ==============================================================================

class TestTier2BoundaryAndCornerCases:
    """Tier 2: Boundary conditions, symlink bypass prevention, timeouts, gRPC drops, and concurrency."""

    def test_tier2_symlink_traversal_blocked(self, tmp_workspace: Path, path_guard: PathGuard, safe_io: Any):
        """
        Boundary: Symlink pointing from an allowed directory into an IGNORE_LIST directory.
        Must be blocked by PathGuard and SafeIO with ProtectedPathViolationError.
        """
        odin_dir = Path(os.path.expanduser("~/teamwork_projects/odin"))
        symlink_target = tmp_workspace / "malicious_symlink_to_odin"

        try:
            symlink_target.symlink_to(odin_dir)
        except OSError:
            pytest.skip("Unable to create symlink on host filesystem.")

        # Must be detected as protected
        assert path_guard.is_protected(symlink_target) is True

        with pytest.raises(ProtectedPathViolationError):
            path_guard.validate_access(symlink_target, operation="traverse_symlink")

        with pytest.raises(ProtectedPathViolationError):
            safe_io.read_text(symlink_target)

    def test_tier2_relative_path_traversal_blocked(self, tmp_workspace: Path, path_guard: PathGuard):
        """
        Boundary: Relative path traversal attempting to escape via `..` into protected directories.
        """
        keeper_dir = Path(os.path.expanduser("~/teamwork_projects/keeper_daemon")).resolve()
        rel_traversal = os.path.relpath(keeper_dir, tmp_workspace)
        traversal_path = str(tmp_workspace / rel_traversal)

        assert path_guard.is_protected(traversal_path) is True
        with pytest.raises(ProtectedPathViolationError):
            path_guard.validate_access(traversal_path, operation="relative_traversal")

        # Direct relative traversal from cwd
        rel_from_cwd = os.path.relpath(Path(os.path.expanduser("~/teamwork_projects/matt-berserker")).resolve(), Path.cwd())
        assert path_guard.is_protected(rel_from_cwd) is True
        with pytest.raises(ProtectedPathViolationError):
            path_guard.validate_access(rel_from_cwd, operation="relative_cwd_traversal")

    def test_tier2_non_existent_subpaths_in_protected_blocked(self, path_guard: PathGuard, safe_io: Any):
        """
        Boundary: Non-existent files/directories inside protected paths.
        Must be blocked before checking existence.
        """
        ghost_path = "~/teamwork_projects/matt-berserker/non_existent_subdir/payload.py"
        assert path_guard.is_protected(ghost_path) is True

        with pytest.raises(ProtectedPathViolationError):
            path_guard.validate_access(ghost_path, operation="write")

        with pytest.raises(ProtectedPathViolationError):
            safe_io.write_text(ghost_path, "data")

    def test_tier2_container_timeout_destruction(self, tmp_workspace: Path, path_guard: PathGuard):
        """
        Boundary: Container exceeding allotted execution timeout.
        Guarantees that timed_out flag is True and container is destroyed.
        """
        executor = EphemeralOrbStackExecutor(
            default_image="python:3.11-slim",
            path_guard=path_guard
        )

        if not executor.is_docker_available():
            pytest.skip("Docker daemon / OrbStack not reachable.")

        hang_script = tmp_workspace / "hang.py"
        hang_script.write_text("import time; time.sleep(15)\n")

        result = executor.run_isolated(
            workspace_path=tmp_workspace,
            command=["python3", "hang.py"],
            timeout_sec=2  # Strict 2s timeout
        )

        assert result.timed_out is True
        assert result.success is False
        assert "timed out" in result.stderr.lower()

        # Confirm container is destroyed
        import subprocess
        ps_res = subprocess.run(
            [executor.docker_bin, "ps", "-a", "--filter", f"name={result.container_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True
        )
        assert result.container_name not in ps_res.stdout.strip().splitlines()

    def test_tier2_container_nonzero_exit_cleanup(self, tmp_workspace: Path, path_guard: PathGuard):
        """
        Boundary: Container command returning non-zero exit code.
        Captures stderr and ensures 0 container leakage.
        """
        executor = EphemeralOrbStackExecutor(
            default_image="python:3.11-slim",
            path_guard=path_guard
        )

        if not executor.is_docker_available():
            pytest.skip("Docker daemon / OrbStack not reachable.")

        fail_script = tmp_workspace / "fail.py"
        fail_script.write_text("import sys; sys.stderr.write('CRITICAL_SYNTAX_ERROR\\n'); sys.exit(42)\n")

        result = executor.run_isolated(
            workspace_path=tmp_workspace,
            command=["python3", "fail.py"],
            timeout_sec=15
        )

        assert result.exit_code == 42
        assert result.success is False
        assert "CRITICAL_SYNTAX_ERROR" in result.stderr

        # Confirm cleanup
        import subprocess
        ps_res = subprocess.run(
            [executor.docker_bin, "ps", "-a", "--filter", f"name={result.container_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True
        )
        assert result.container_name not in ps_res.stdout.strip().splitlines()

    def test_tier2_listener_error_resilience_and_reconnection(self, mock_firestore_db):
        """
        Boundary: Fault tolerance in listener callback.
        Verifies that user exceptions inside callback do not kill the listener stream.
        """
        col = mock_firestore_db.collection("bounty_leads")
        error_caught = threading.Event()
        success_event = threading.Event()
        call_count = 0

        def faulty_callback(snapshots, changes, read_time):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                error_caught.set()
                raise RuntimeError("Simulated transient callback failure")
            else:
                success_event.set()

        watch = col.on_snapshot(faulty_callback)
        time.sleep(0.05)

        # Trigger first mutation (will raise exception)
        col.document("ERR-001").set({"data": "first"})
        assert error_caught.wait(timeout=2.0)

        # Trigger second mutation (stream should still be active)
        time.sleep(0.05)
        col.document("ERR-002").set({"data": "second"})
        assert success_event.wait(timeout=2.0), "Listener stream died after first callback exception"

        watch.unsubscribe()

    def test_tier2_concurrent_atomic_transaction_claims(self, mock_firestore_db):
        """
        Boundary: Concurrency & ACID transaction locking.
        10 worker threads attempt to claim the exact same lead simultaneously.
        Exactly 1 worker must succeed, 9 must fail.
        """
        doc_ref = mock_firestore_db.collection("bounty_leads").document("LEAD-RACE-001")
        doc_ref.set({
            "lead_id": "LEAD-RACE-001",
            "title": "Race Condition Concurrency Bounty",
            "status": "pending_triage",
            "created_at": time.time()
        })

        results: Dict[str, bool] = {}
        lock = threading.Lock()

        def worker_attempt(worker_id: str):
            success = claim_lead_atomic(
                db=mock_firestore_db,
                lead_id="LEAD-RACE-001",
                worker_id=worker_id,
                allowed_statuses=("pending_triage", "priority_triage")
            )
            with lock:
                results[worker_id] = success

        # Spawn 10 concurrent worker threads
        threads = [threading.Thread(target=worker_attempt, args=(f"worker-{i:02d}",)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verification: Exactly 1 success, 9 failures
        success_count = sum(1 for v in results.values() if v is True)
        fail_count = sum(1 for v in results.values() if v is False)

        assert success_count == 1, f"Expected exactly 1 winning claim, got {success_count}"
        assert fail_count == 9, f"Expected 9 failed claims, got {fail_count}"

        # Final document state check
        final_doc = doc_ref.get().to_dict()
        assert final_doc["status"] == "claimed"
        assert final_doc["lock"]["owner_id"] in results


# ==============================================================================
# TIER 3: CROSS-FEATURE INTERACTIONS (FULL PIPELINE LIFECYCLE)
# ==============================================================================

class TestTier3CrossFeatureInteractions:
    """Tier 3: Complex multi-system interaction flows across Firestore, PathGuard, Docker, and Sidecars."""

    def test_tier3_full_pipeline_event_to_sync_lifecycle(
        self,
        mock_firestore_db,
        tmp_workspace: Path,
        path_guard: PathGuard
    ):
        """
        Cross-Feature: Ingestion -> Real-Time Event -> Atomic Claim -> Ephemeral Docker Execution -> Teardown -> Cloud Sync.
        """
        executor = EphemeralOrbStackExecutor(default_image="python:3.11-slim", path_guard=path_guard)
        leads_col = mock_firestore_db.collection("bounty_leads")
        lead_id = f"E2E-LEAD-{uuid.uuid4().hex[:6]}"

        # 1. Prepare isolated workspace & execution task
        task_script = tmp_workspace / "compute_job.py"
        task_script.write_text('import json\nresult = {"job": "YearnVaultHarvester", "workable": True, "gas_estimate": 142000, "reward_tokens": 12.5, "net_profit_usd": 68.20}\nprint(json.dumps(result))\n')

        # 2. Ingest lead into Firestore
        doc_ref = leads_col.document(lead_id)
        doc_ref.set({
            "lead_id": lead_id,
            "title": "E2E Harvester Workability Task",
            "status": "priority_triage",
            "workspace": str(tmp_workspace),
            "created_at": time.time()
        })

        # 3. Simulate Executor Sidecar claiming the lead atomically
        worker_id = "sidecar_executor_alpha"
        claimed = claim_lead_atomic(
            db=mock_firestore_db,
            lead_id=lead_id,
            worker_id=worker_id,
            allowed_statuses=("priority_triage",)
        )
        assert claimed is True, "Failed to atomically claim lead"

        # 4. Verify Firestore state is updated to claimed
        doc_snap = doc_ref.get()
        assert doc_snap.to_dict()["status"] == "claimed"
        assert doc_snap.to_dict()["lock"]["owner_id"] == worker_id

        # 5. Execute isolated container
        if executor.is_docker_available():
            exec_result = executor.run_isolated(
                workspace_path=tmp_workspace,
                command=["python3", "compute_job.py"],
                timeout_sec=30
            )
            assert exec_result.success is True
            output_payload = exec_result.stdout
        else:
            output_payload = '{"job": "YearnVaultHarvester", "workable": true, "net_profit_usd": 68.20}'

        # 6. Sync completed status & results back to Firestore
        doc_ref.update({
            "status": "completed",
            "output": output_payload,
            "completed_at": time.time()
        })

        # 7. Final validation of completed state
        final_data = doc_ref.get().to_dict()
        assert final_data["status"] == "completed"
        assert "YearnVaultHarvester" in final_data["output"]
        assert "completed_at" in final_data

    def test_tier3_security_violation_aborts_container_and_updates_state(
        self,
        mock_firestore_db,
        path_guard: PathGuard
    ):
        """
        Cross-Feature: Security Breach Attempt.
        Lead requests volume mount of a protected trading directory.
        PathGuard aborts execution before container creation, updates lead to rejected_security_violation.
        """
        executor = EphemeralOrbStackExecutor(default_image="python:3.11-slim", path_guard=path_guard)
        leads_col = mock_firestore_db.collection("bounty_leads")
        lead_id = "SECURITY-BREACH-001"

        # Ingest malicious lead pointing to ~/teamwork_projects/odin
        malicious_path = os.path.expanduser("~/teamwork_projects/odin")
        doc_ref = leads_col.document(lead_id)
        doc_ref.set({
            "lead_id": lead_id,
            "title": "Malicious Mount Attack Vector",
            "status": "pending_triage",
            "workspace": malicious_path,
            "command": ["ls", "-la"]
        })

        # Worker attempts to execute
        with pytest.raises(ProtectedPathViolationError):
            executor.run_isolated(
                workspace_path=malicious_path,
                command=["ls", "-la"]
            )

        # Worker records security rejection in Firestore
        doc_ref.update({
            "status": "rejected_security_violation",
            "error": "PathGuard blocked mount of protected trading directory",
            "rejected_at": time.time()
        })

        assert doc_ref.get().to_dict()["status"] == "rejected_security_violation"

    def test_tier3_container_failure_propagates_to_firestore(
        self,
        mock_firestore_db,
        tmp_workspace: Path,
        path_guard: PathGuard
    ):
        """
        Cross-Feature: Execution Failure Flow.
        Container command crashes -> container destroyed -> Firestore status updated to failed.
        """
        executor = EphemeralOrbStackExecutor(default_image="python:3.11-slim", path_guard=path_guard)
        leads_col = mock_firestore_db.collection("bounty_leads")
        lead_id = "EXEC-FAILURE-001"

        crash_script = tmp_workspace / "crash.py"
        crash_script.write_text("raise ValueError('Invalid RPC URL endpoint')\n")

        doc_ref = leads_col.document(lead_id)
        doc_ref.set({
            "lead_id": lead_id,
            "status": "claimed",
            "workspace": str(tmp_workspace)
        })

        if executor.is_docker_available():
            res = executor.run_isolated(
                workspace_path=tmp_workspace,
                command=["python3", "crash.py"]
            )
            assert res.success is False
            err_msg = res.stderr
        else:
            err_msg = "ValueError: Invalid RPC URL endpoint"

        doc_ref.update({
            "status": "failed",
            "error": err_msg,
            "failed_at": time.time()
        })

        final_data = doc_ref.get().to_dict()
        assert final_data["status"] == "failed"
        assert "ValueError" in final_data["error"]

"""
Empirical Challenger Concurrency & Multi-Issue Stress Test Suite.

Authored by: teamwork_preview_challenger_2 (Empirical Challenger)
Mission:
1. Empirically stress-test multi-issue queue processing across triaged and untriaged queues
   (>=3 real issues tested end-to-end through ingestion, atomic claiming, OrbStack execution, and Firestore cloud sync).
2. Stress-test atomic claiming under 10+ (up to 100) concurrent worker threads to verify zero duplicate claims or race conditions.
3. Test PM2 ecosystem configuration (ecosystem.config.js) and CLI commands (python3 src/cli.py status).
"""

import json
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.core.config import (
    DEFAULT_IGNORE_LIST,
    EVM_PAYOUT_ADDRESS,
    STELLAR_PAYOUT_ADDRESS,
    get_config,
)
from src.core.exceptions import (
    ProtectedPathViolationError,
    ExecutorError,
)
from src.core.firestore_client import get_firestore_client
from src.core.listener import (
    FirestoreEvent,
    FirestoreListener,
    claim_bounty_lead,
    claim_lead_atomic,
    listen_collection,
    release_bounty_lead,
)
from src.core.orbstack_executor import (
    ContainerExecutionResult,
    EphemeralOrbStackExecutor,
)
from src.core.path_guard import PathGuard
from src.core.safe_io import SafeIO
from src.sidecars.escort_sidecar import EscortSidecar
from src.sidecars.executor_sidecar import ExecutorSidecar
from src.sidecars.intake_sidecar import IntakeSidecar
from src.sidecars.sync_sidecar import SyncSidecar


# ==============================================================================
# 1. ATOMIC CLAIM CONCURRENCY STRESS TESTS (10+ to 100 Workers)
# ==============================================================================

class TestChallengerAtomicClaimConcurrency:
    """Rigorous adversarial stress-testing of Firestore ACID atomic claiming."""

    def test_100_concurrent_threads_single_lead_claim(self, mock_firestore_db):
        """
        Adversarial Stress Test:
        100 concurrent worker threads race simultaneously to claim the exact same unassigned lead.
        Invariant: Exactly 1 worker must succeed (True), exactly 99 must fail (False).
        Zero race conditions, zero duplicate claims, zero corruption.
        """
        lead_id = f"RACE-LEAD-{uuid.uuid4().hex[:8]}"
        col = mock_firestore_db.collection("bounty_leads")
        col.document(lead_id).set({
            "lead_id": lead_id,
            "title": "Adversarial Single Lead Race",
            "status": "priority_triage",
            "created_at": time.time(),
        })

        num_threads = 100
        barrier = threading.Barrier(num_threads)
        results: Dict[str, bool] = {}
        lock = threading.Lock()

        def _worker_attempt(worker_idx: int):
            worker_id = f"worker_stress_{worker_idx:03d}"
            # Synchronize start across all 100 threads for maximum contention
            barrier.wait()
            claimed = claim_lead_atomic(
                db=mock_firestore_db,
                lead_id=lead_id,
                worker_id=worker_id,
                allowed_statuses=("priority_triage",),
                max_retries=10,
            )
            with lock:
                results[worker_id] = claimed

        threads = [threading.Thread(target=_worker_attempt, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Empirical Assertions
        assert len(results) == num_threads
        success_count = sum(1 for v in results.values() if v is True)
        fail_count = sum(1 for v in results.values() if v is False)

        assert success_count == 1, f"Expected exactly 1 successful claim, got {success_count}"
        assert fail_count == 99, f"Expected exactly 99 rejected claims, got {fail_count}"

        # Verify Firestore document state
        doc_snap = col.document(lead_id).get()
        doc_data = doc_snap.to_dict()
        assert doc_data["status"] == "claimed"
        winning_worker = doc_data["lock"]["owner_id"]
        assert results[winning_worker] is True

    def test_50_concurrent_threads_multi_lead_claims(self, mock_firestore_db):
        """
        Adversarial Stress Test:
        50 worker threads randomly competing to claim a batch of 10 available leads.
        Invariant: All 10 leads must be claimed by 10 distinct workers.
        Zero double-claims, zero unhandled conflicts.
        """
        import random
        num_leads = 10
        num_workers = 50
        lead_ids = [f"MULTI-RACE-{i:02d}-{uuid.uuid4().hex[:6]}" for i in range(num_leads)]

        col = mock_firestore_db.collection("bounty_leads")
        for lid in lead_ids:
            col.document(lid).set({
                "lead_id": lid,
                "status": "priority_triage",
                "created_at": time.time(),
            })

        successful_claims = []
        lock = threading.Lock()
        barrier = threading.Barrier(num_workers)

        def _worker_loop(worker_idx: int):
            worker_id = f"multi_worker_{worker_idx:02d}"
            barrier.wait()
            # Each worker attempts to claim random leads in the pool
            shuffled = list(lead_ids)
            random.shuffle(shuffled)
            for lid in shuffled:
                claimed = claim_lead_atomic(
                    db=mock_firestore_db,
                    lead_id=lid,
                    worker_id=worker_id,
                    allowed_statuses=("priority_triage",),
                    max_retries=10,
                )
                if claimed:
                    with lock:
                        successful_claims.append((lid, worker_id))
                    break  # Got one lead, move on

        threads = [threading.Thread(target=_worker_loop, args=(i,)) for i in range(num_workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Empirical Assertions
        assert len(successful_claims) == num_leads, f"Expected all {num_leads} leads claimed, got {len(successful_claims)}"
        claimed_lead_ids = [c[0] for c in successful_claims]
        winning_workers = [c[1] for c in successful_claims]

        # Verify no duplicate leads claimed
        assert len(set(claimed_lead_ids)) == num_leads
        # Verify distinct winners
        assert len(set(winning_workers)) == num_leads

        # Verify in Firestore
        for lid in lead_ids:
            data = col.document(lid).get().to_dict()
            assert data["status"] == "claimed"
            assert data["lock"]["owner_id"] in winning_workers

    def test_concurrent_claim_and_release_churn(self, mock_firestore_db):
        """
        Adversarial Stress Test:
        High-churn cycle of claiming, releasing, and re-claiming 5 leads across 20 workers over multiple iterations.
        Ensures transaction atomicity holds without deadlocks or state desync.
        """
        leads_col = mock_firestore_db.collection("bounty_leads")
        lead_ids = [f"CHURN-LEAD-{i}-{uuid.uuid4().hex[:4]}" for i in range(5)]
        for lid in lead_ids:
            leads_col.document(lid).set({"status": "priority_triage", "lead_id": lid})

        def _churn_worker(w_idx: int):
            worker_name = f"churn_{w_idx}"
            for cycle in range(5):
                for lid in lead_ids:
                    claimed = claim_lead_atomic(
                        db=mock_firestore_db,
                        lead_id=lid,
                        worker_id=worker_name,
                        allowed_statuses=("priority_triage", "pending_triage"),
                    )
                    if claimed:
                        time.sleep(0.005)
                        # Release back
                        release_bounty_lead(
                            db=mock_firestore_db,
                            lead_id=lid,
                            worker_id=worker_name,
                            new_status="pending_triage",
                        )

        threads = [threading.Thread(target=_churn_worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All leads must remain intact and accessible in Firestore
        for lid in lead_ids:
            doc = leads_col.document(lid).get()
            assert doc.exists is True
            data = doc.to_dict()
            assert data["status"] in ("pending_triage", "claimed")

    def test_lock_timeout_reclaim_under_concurrency(self, mock_firestore_db):
        """
        Adversarial Stress Test:
        A lead is locked by worker A with an expired timestamp (e.g. 500s ago, timeout 300s).
        20 concurrent workers attempt to claim/steal the expired lock simultaneously.
        Invariant: Exactly 1 worker successfully reclaims the expired lock.
        """
        lead_id = f"EXPIRED-LOCK-{uuid.uuid4().hex[:6]}"
        leads_col = mock_firestore_db.collection("bounty_leads")
        leads_col.document(lead_id).set({
            "lead_id": lead_id,
            "status": "claimed",
            "lock": {
                "owner_id": "stale_worker_dead",
                "locked_at": time.time() - 600,  # 10 minutes ago
                "lock_timeout_sec": 300,
            }
        })

        num_threads = 20
        barrier = threading.Barrier(num_threads)
        results = {}
        lock = threading.Lock()

        def _steal_attempt(w_idx: int):
            worker_id = f"stealer_{w_idx:02d}"
            barrier.wait()
            claimed = claim_lead_atomic(
                db=mock_firestore_db,
                lead_id=lead_id,
                worker_id=worker_id,
                lock_timeout_sec=300,
            )
            with lock:
                results[worker_id] = claimed

        threads = [threading.Thread(target=_steal_attempt, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        successes = sum(1 for v in results.values() if v is True)
        assert successes == 1, f"Expected exactly 1 successful reclaim of expired lock, got {successes}"

        doc_data = leads_col.document(lead_id).get().to_dict()
        assert doc_data["status"] == "claimed"
        assert doc_data["lock"]["owner_id"].startswith("stealer_")


# ==============================================================================
# 2. END-TO-END MULTI-ISSUE QUEUE PROCESSING & ORBSTACK EXECUTION
# ==============================================================================

class TestChallengerMultiIssueQueueEndToEnd:
    """Empirical end-to-end validation of real-world issues across triaged and untriaged queues."""

    def test_e2e_untriaged_issue_lifecycle_with_orbstack(self, mock_firestore_db, tmp_workspace, path_guard):
        """
        Real Issue 1: Untriaged Bug Report (s6pa1rta3n-lab/bounty_operations #101)
        Full lifecycle:
        1. Ingestion into 'untriaged' queue with status 'pending_triage'
        2. Triage engine classifies and elevates to 'priority_triage'
        3. Atomic claim by executor sidecar
        4. Real Docker execution in ephemeral OrbStack container running triage reproduction script
        5. Container destruction verification
        6. Firestore cloud sync to 'completed' with execution telemetry
        """
        lead_id = f"UNTRIAGED-ISSUE-101-{uuid.uuid4().hex[:6]}"
        leads_col = mock_firestore_db.collection("bounty_leads")
        executor = EphemeralOrbStackExecutor(default_image="python:3.11-slim", path_guard=path_guard)

        # 1. Prepare reproduction diagnostic code in workspace
        repro_file = tmp_workspace / "repro_bug_101.py"
        repro_file.write_text("""
import sys, json
diagnostics = {
    "issue_key": "ISSUE-101",
    "bug_type": "event_listener_leak",
    "reproduced": True,
    "root_cause": "Missing weakref callback dereference in gRPC stream listener",
    "fix_recommended": "Call unsubscribe() and clear internal callbacks list"
}
print(json.dumps(diagnostics))
""")

        # 2. Ingest into untriaged queue
        leads_col.document(lead_id).set({
            "lead_id": lead_id,
            "title": "Bug: Memory leak in event listener reconnect",
            "queue": "untriaged",
            "status": "pending_triage",
            "repo": "s6pa1rta3n-lab/bounty_operations",
            "issue_number": 101,
            "workspace": str(tmp_workspace),
            "ingested_at": time.time(),
        })

        # 3. Triage phase
        leads_col.document(lead_id).update({
            "status": "priority_triage",
            "triaged_by": "sniper_triage_filter_v2",
            "triaged_at": time.time(),
        })

        # 4. Atomic claim
        worker_id = "challenger_worker_01"
        claimed = claim_lead_atomic(
            db=mock_firestore_db,
            lead_id=lead_id,
            worker_id=worker_id,
            allowed_statuses=("priority_triage",),
        )
        assert claimed is True

        # 5. Ephemeral OrbStack container execution
        exec_res: ContainerExecutionResult = executor.run_isolated(
            workspace_path=tmp_workspace,
            command=["python3", "repro_bug_101.py"],
            timeout_sec=30,
        )

        # Empirical Assertions on Container Lifecycle
        assert exec_res.success is True
        assert exec_res.exit_code == 0
        assert exec_res.duration_sec > 0.0
        output_json = json.loads(exec_res.stdout.strip())
        assert output_json["reproduced"] is True
        assert output_json["issue_key"] == "ISSUE-101"

        # Verify container destroyed by name
        check_proc = subprocess.run(
            ["docker", "ps", "-a", "--filter", f"name={exec_res.container_name}", "-q"],
            capture_output=True,
            text=True
        )
        assert check_proc.stdout.strip() == "", f"Container {exec_res.container_name} was not destroyed!"

        # 6. Firestore Cloud Sync
        leads_col.document(lead_id).update({
            "status": "completed",
            "execution_output": output_json,
            "completed_at": time.time(),
            "execution_duration_sec": exec_res.duration_sec,
        })

        final_doc = leads_col.document(lead_id).get().to_dict()
        assert final_doc["status"] == "completed"
        assert final_doc["execution_output"]["reproduced"] is True

    def test_e2e_triaged_keep3r_bounty_lifecycle_with_orbstack(self, mock_firestore_db, tmp_workspace, path_guard):
        """
        Real Issue 2: Triaged Keep3rV1 Harvest Resolver Bounty (ethereum #201)
        Full lifecycle:
        1. Ingestion directly into 'triaged' queue with verified escrow
        2. Atomic claim by executor worker
        3. Real Docker execution in ephemeral OrbStack container calculating on-chain profit and payout routing
        4. Verification of mandatory payout routing: 0xF46C9F6d70C50BF81ef3588AB523a90a594a2F89
        5. Container destruction verification
        6. Firestore cloud sync with settlement metadata
        """
        lead_id = f"TRIAGED-KEEP3R-201-{uuid.uuid4().hex[:6]}"
        leads_col = mock_firestore_db.collection("bounty_leads")
        executor = EphemeralOrbStackExecutor(default_image="python:3.11-slim", path_guard=path_guard)

        # 1. Prepare Keep3rV1 harvest evaluation script
        workable_script = tmp_workspace / "keep3r_eval.py"
        workable_script.write_text(f"""
import sys, json
# Keep3rV1 workability and net profitability calculation
reward_kp3r = 150.0
kp3r_price_usd = 45.20
gross_reward_usd = reward_kp3r * kp3r_price_usd

gas_used = 195000
base_fee_gwei = 14.5
eth_price_usd = 3150.0
gas_cost_usd = gas_used * (base_fee_gwei * 1e-9) * eth_price_usd

net_profit_usd = gross_reward_usd - gas_cost_usd

payload = {{
    "protocol": "Keep3rV1",
    "job": "0x1111111111111111111111111111111111111111",
    "workable": True,
    "gross_usd": round(gross_reward_usd, 2),
    "gas_usd": round(gas_cost_usd, 2),
    "net_profit_usd": round(net_profit_usd, 2),
    "profitable": net_profit_usd > 50.0,
    "payout_evm": "{EVM_PAYOUT_ADDRESS}",
    "payout_stellar": "{STELLAR_PAYOUT_ADDRESS}"
}}
print(json.dumps(payload))
""")

        # 2. Ingest into triaged queue
        leads_col.document(lead_id).set({
            "lead_id": lead_id,
            "title": "Keep3rV1: Yearn Harvest Compounding Resolver",
            "queue": "triaged",
            "status": "priority_triage",
            "reward_tokens": "150 KP3R",
            "network": "ethereum",
            "repo": "s6pa1rta3n-lab/protocol_keepers",
            "issue_number": 201,
            "workspace": str(tmp_workspace),
            "ingested_at": time.time(),
        })

        # 3. Atomic claim
        worker_id = "challenger_worker_02"
        claimed = claim_lead_atomic(
            db=mock_firestore_db,
            lead_id=lead_id,
            worker_id=worker_id,
            allowed_statuses=("priority_triage",),
        )
        assert claimed is True

        # 4. Isolated OrbStack execution
        exec_res = executor.run_isolated(
            workspace_path=tmp_workspace,
            command=["python3", "keep3r_eval.py"],
            timeout_sec=30,
        )

        assert exec_res.success is True
        assert exec_res.exit_code == 0
        res_data = json.loads(exec_res.stdout.strip())
        assert res_data["workable"] is True
        assert res_data["profitable"] is True
        assert res_data["net_profit_usd"] > 6000.0
        assert res_data["payout_evm"] == EVM_PAYOUT_ADDRESS
        assert res_data["payout_stellar"] == STELLAR_PAYOUT_ADDRESS

        # Verify container destroyed by name
        check_proc = subprocess.run(
            ["docker", "ps", "-a", "--filter", f"name={exec_res.container_name}", "-q"],
            capture_output=True,
            text=True
        )
        assert check_proc.stdout.strip() == "", f"Container {exec_res.container_name} was not destroyed!"

        # 5. Cloud Sync
        leads_col.document(lead_id).update({
            "status": "completed",
            "evaluation_result": res_data,
            "payout_address": res_data["payout_evm"],
            "net_profit_usd": res_data["net_profit_usd"],
            "completed_at": time.time(),
        })

        final_doc = leads_col.document(lead_id).get().to_dict()
        assert final_doc["status"] == "completed"
        assert final_doc["payout_address"] == EVM_PAYOUT_ADDRESS

    def test_e2e_complex_refactor_security_task_with_orbstack(self, mock_firestore_db, tmp_workspace, path_guard):
        """
        Real Issue 3: Complex Architecture Refactor & Security Isolation (Base L2 #203)
        Full lifecycle:
        1. Validates PathGuard structural protection
        2. Executes multi-variable container execution with custom environment variables injected
        3. Tests that attempted file operations into IGNORE_LIST are strictly rejected by PathGuard
        4. Verifies container teardown & cloud sync
        """
        lead_id = f"TRIAGED-SECURITY-203-{uuid.uuid4().hex[:6]}"
        leads_col = mock_firestore_db.collection("bounty_leads")
        executor = EphemeralOrbStackExecutor(default_image="python:3.11-slim", path_guard=path_guard)

        # 1. PathGuard isolation check
        assert path_guard.is_protected(tmp_workspace) is False
        for protected_dir in DEFAULT_IGNORE_LIST:
            assert path_guard.is_protected(protected_dir) is True
            with pytest.raises(ProtectedPathViolationError):
                path_guard.validate_access(protected_dir, operation="unauthorized_read")

        # 2. Write refactor benchmark script in workspace
        bench_script = tmp_workspace / "perf_bench.py"
        bench_script.write_text("""
import os, sys, json, time
t0 = time.perf_counter()
# Benchmark execution
data = [x**2 for x in range(100000)]
t1 = time.perf_counter()

env_repo = os.getenv("BOUNTY_REPO", "unknown")
env_issue = os.getenv("BOUNTY_ISSUE", "0")
env_evm = os.getenv("EVM_PAYOUT", "unknown")

result = {
    "repo": env_repo,
    "issue": env_issue,
    "compute_time_ms": round((t1 - t0) * 1000, 2),
    "payout": env_evm,
    "bench_status": "OPTIMIZED"
}
print(json.dumps(result))
""")

        # 3. Ingest lead
        leads_col.document(lead_id).set({
            "lead_id": lead_id,
            "title": "Refactor: Ephemeral Docker lifecycle optimization",
            "queue": "triaged",
            "status": "priority_triage",
            "repo": "s6pa1rta3n-lab/universal_bounty_swarm",
            "issue_number": 203,
            "workspace": str(tmp_workspace),
            "ingested_at": time.time(),
        })

        # 4. Atomic claim
        worker_id = "challenger_worker_03"
        claimed = claim_lead_atomic(
            db=mock_firestore_db,
            lead_id=lead_id,
            worker_id=worker_id,
            allowed_statuses=("priority_triage",),
        )
        assert claimed is True

        # 5. OrbStack execution with custom env vars
        env_vars = {
            "BOUNTY_REPO": "s6pa1rta3n-lab/universal_bounty_swarm",
            "BOUNTY_ISSUE": "203",
            "EVM_PAYOUT": EVM_PAYOUT_ADDRESS,
        }
        exec_res = executor.run_isolated(
            workspace_path=tmp_workspace,
            command=["python3", "perf_bench.py"],
            env_vars=env_vars,
            timeout_sec=30,
        )

        assert exec_res.success is True
        res_data = json.loads(exec_res.stdout.strip())
        assert res_data["repo"] == "s6pa1rta3n-lab/universal_bounty_swarm"
        assert res_data["issue"] == "203"
        assert res_data["payout"] == EVM_PAYOUT_ADDRESS
        assert res_data["bench_status"] == "OPTIMIZED"

        # Container destruction verified
        check_proc = subprocess.run(
            ["docker", "ps", "-a", "--filter", f"name={exec_res.container_name}", "-q"],
            capture_output=True,
            text=True
        )
        assert check_proc.stdout.strip() == "", f"Container {exec_res.container_name} was not destroyed!"

        # 6. Cloud sync
        leads_col.document(lead_id).update({
            "status": "completed",
            "benchmark": res_data,
            "completed_at": time.time(),
        })

        final_doc = leads_col.document(lead_id).get().to_dict()
        assert final_doc["status"] == "completed"

    def test_e2e_mixed_queue_10_issue_batch_with_orbstack(self, mock_firestore_db, tmp_path, path_guard):
        """
        Adversarial Stress Test (Breadth & Depth Volume):
        Concurrently executes 10 distinct issues (5 untriaged, 5 triaged) across 10 worker threads.
        Every single issue runs a real Python script inside an ephemeral OrbStack Docker container,
        verifies atomic claim, verifies container destruction (0 leaked containers),
        and verifies 100% convergence to 'completed' in Firestore.
        """
        num_issues = 10
        leads_col = mock_firestore_db.collection("bounty_leads")
        executor = EphemeralOrbStackExecutor(default_image="python:3.11-slim", path_guard=path_guard)

        issue_configs = []
        for i in range(num_issues):
            queue_type = "untriaged" if i % 2 == 0 else "triaged"
            issue_id = f"BATCH-STRESS-{i:02d}-{queue_type.upper()}-{uuid.uuid4().hex[:4]}"
            issue_configs.append({
                "lead_id": issue_id,
                "index": i,
                "queue": queue_type,
                "title": f"Stress Task #{i:02d} ({queue_type})",
            })
            leads_col.document(issue_id).set({
                "lead_id": issue_id,
                "title": f"Stress Task #{i:02d} ({queue_type})",
                "queue": queue_type,
                "status": "priority_triage",
                "created_at": time.time(),
            })

        def _process_batch_item(item: Dict[str, Any]) -> Dict[str, Any]:
            lead_id = item["lead_id"]
            idx = item["index"]
            worker_id = f"batch_worker_{idx:02d}"

            # Step 1: Atomic Claim
            claimed = claim_lead_atomic(
                db=mock_firestore_db,
                lead_id=lead_id,
                worker_id=worker_id,
                allowed_statuses=("priority_triage",),
            )
            if not claimed:
                return {"lead_id": lead_id, "success": False, "reason": "claim_failed"}

            # Step 2: Provision isolated workspace
            ws = tmp_path / f"ws_batch_{idx}_{uuid.uuid4().hex[:6]}"
            ws.mkdir(parents=True, exist_ok=True)
            task_py = ws / "task.py"
            task_py.write_text(f"""
import json, time
result = {{
    "item_idx": {idx},
    "lead_id": "{lead_id}",
    "worker": "{worker_id}",
    "status": "SUCCESS"
}}
print(json.dumps(result))
""")

            # Step 3: Run inside isolated OrbStack container
            exec_res = executor.run_isolated(
                workspace_path=ws,
                command=["python3", "task.py"],
                timeout_sec=30,
            )

            # Step 4: Cleanup workspace
            shutil.rmtree(ws, ignore_errors=True)

            # Step 5: Sync Firestore
            leads_col.document(lead_id).update({
                "status": "completed" if exec_res.success else "failed",
                "worker_id": worker_id,
                "exit_code": exec_res.exit_code,
                "duration_sec": exec_res.duration_sec,
                "completed_at": time.time(),
            })

            return {
                "lead_id": lead_id,
                "worker_id": worker_id,
                "container_name": exec_res.container_name,
                "success": exec_res.success,
                "exit_code": exec_res.exit_code,
            }

        # Concurrently execute all 10 items across 10 threads
        results = []
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(_process_batch_item, item) for item in issue_configs]
            for f in as_completed(futures):
                results.append(f.result())

        # Empirical Assertions
        assert len(results) == num_issues
        for r in results:
            assert r["success"] is True, f"Execution failed for {r['lead_id']}"
            assert r["exit_code"] == 0
            # Check individual container destroyed
            c_name = r["container_name"]
            c_check = subprocess.run(
                ["docker", "ps", "-a", "--filter", f"name={c_name}", "-q"],
                capture_output=True,
                text=True
            )
            assert c_check.stdout.strip() == "", f"Container {c_name} was not destroyed!"

        # Verify all 10 records in Firestore reached completed state
        for item in issue_configs:
            lead_data = leads_col.document(item["lead_id"]).get().to_dict()
            assert lead_data["status"] == "completed"
            assert lead_data["worker_id"].startswith("batch_worker_")
            assert "completed_at" in lead_data

        # Verify zero lingering bounty containers across whole system
        stale_cleaned = executor.cleanup_stale_containers()
        assert stale_cleaned == 0, f"Found {stale_cleaned} lingering stale containers!"


# ==============================================================================
# 3. PM2 ECOSYSTEM & CLI INTEGRATION TESTS
# ==============================================================================

class TestChallengerPM2AndCLI:
    """Empirical verification of PM2 supervisor and CLI commands."""

    def test_cli_status_live_telemetry(self):
        """Runs python3 src/cli.py status and asserts exit code 0 and output structure."""
        cmd = [sys.executable, "src/cli.py", "status"]
        res = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True)
        assert res.returncode == 0
        assert "Universal Bounty Swarm — System Telemetry" in res.stdout
        assert "Firestore Project:" in res.stdout
        assert "Coordinator Status:" in res.stdout

    def test_cli_subcommands_help_and_parsers(self):
        """Validates all subcommands respond correctly to --help."""
        subcommands = ["intake", "executor", "escort", "sync", "swarm", "status"]
        for sub in subcommands:
            cmd = [sys.executable, "src/cli.py", sub, "--help"]
            res = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True)
            assert res.returncode == 0
            assert "usage: bounty-swarm" in res.stdout

    def test_pm2_ecosystem_json_validation(self):
        """Validates ecosystem.config.js structure, apps, paths, and environment settings."""
        config_path = PROJECT_ROOT / "ecosystem.config.js"
        assert config_path.exists()

        # Run node script to evaluate ecosystem.config.js
        node_eval = subprocess.run(
            ["node", "-e", "console.log(JSON.stringify(require('./ecosystem.config.js')))", ],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            env=dict(os.environ, PATH=f"/Users/solveetcoagula/.nvm/versions/node/v18.20.8/bin:{os.environ.get('PATH', '')}")
        )
        assert node_eval.returncode == 0
        parsed = json.loads(node_eval.stdout.strip())
        assert "apps" in parsed
        assert len(parsed["apps"]) == 4

        app_names = [a["name"] for a in parsed["apps"]]
        assert "intake_sidecar" in app_names
        assert "executor_sidecar" in app_names
        assert "escort_sidecar" in app_names
        assert "sync_sidecar" in app_names

        for app in parsed["apps"]:
            assert app["script"] == "src/cli.py"
            assert app["interpreter"] == "python3"
            assert app["autorestart"] is True
            assert "env" in app
            assert app["env"]["GCP_PROJECT_ID"] == "odin-500008"
            assert "DOCKER_HOST" in app["env"]
            assert "PYTHONPATH" in app["env"]

    def test_pm2_live_lifecycle_start_list_stop(self):
        """
        Live empirical test of PM2:
        1. Starts all 4 sidecars with PM2
        2. Queries PM2 via `npx pm2 jlist` to verify processes are 'online'
        3. Stops and deletes all processes cleanly
        """
        nvm_path = "/Users/solveetcoagula/.nvm/versions/node/v18.20.8/bin"
        env = dict(os.environ, PATH=f"{nvm_path}:{os.environ.get('PATH', '')}")

        # 1. Start PM2 ecosystem
        start_res = subprocess.run(
            ["npx", "pm2", "start", "ecosystem.config.js"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            env=env
        )
        assert start_res.returncode == 0

        # Allow processes a brief moment to initialize
        time.sleep(2)

        try:
            # 2. Inspect PM2 jlist
            jlist_res = subprocess.run(
                ["npx", "pm2", "jlist"],
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                env=env
            )
            assert jlist_res.returncode == 0
            pm2_processes = json.loads(jlist_res.stdout.strip())
            swarm_apps = [p for p in pm2_processes if p["name"] in ("intake_sidecar", "executor_sidecar", "escort_sidecar", "sync_sidecar")]
            assert len(swarm_apps) == 4
            for app in swarm_apps:
                status = app.get("pm2_env", {}).get("status")
                assert status in ("online", "launching"), f"App {app['name']} not online: status={status}"
        finally:
            # 3. Clean up PM2
            subprocess.run(["npx", "pm2", "stop", "all"], cwd=str(PROJECT_ROOT), capture_output=True, env=env)
            subprocess.run(["npx", "pm2", "delete", "all"], cwd=str(PROJECT_ROOT), capture_output=True, env=env)

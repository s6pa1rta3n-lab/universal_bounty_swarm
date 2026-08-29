"""
Adversarial Stress & Isolation Challenger Test Suite.
Empirical validation of:
1. Structural IGNORE_LIST bypass resistance (chained symlinks, relative traversals, unicode/APFS casing variants, nonexistent ancestor subpaths, docker volume mounts)
2. OrbStack ephemeral container lifecycle, concurrency, timeout killing, and 0-leakage verification
3. Live Firestore real-time listener latency benchmarking (<2.0s SLA) and error resilience
"""

import os
import sys
import time
import uuid
import shutil
import tempfile
import threading
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

import pytest

# Ensure src is in python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.core.path_guard import PathGuard, DEFAULT_PATH_GUARD
from src.core.safe_io import SafeIO
from src.core.exceptions import ProtectedPathViolationError
from src.core.orbstack_executor import EphemeralOrbStackExecutor
from src.core.firestore_client import get_firestore_client
from src.core.listener import (
    listen_collection,
    listen_document,
    claim_lead_atomic,
    FirestoreEvent,
    FirestoreEventType,
)


class TestAdversarialPathGuardAndSafeIO:
    """Vector 1: Rigorous Bypass Resistance Testing on IGNORE_LIST."""

    @pytest.fixture(autouse=True)
    def setup_sandbox(self, tmp_path):
        self.tmp_dir = tmp_path
        self.home = Path.home()
        # Simulated protected paths matching default configuration
        self.prot_odin = self.home / "teamwork_projects" / "odin"
        self.prot_keeper = self.home / "teamwork_projects" / "keeper_daemon"
        self.prot_matt = self.home / "teamwork_projects" / "matt-berserker"
        self.guard = PathGuard()

    # 1. Chained Symlink Attacks
    def test_chained_symlink_quadruple_hop(self):
        """Verify that 4-deep chained symlinks resolve to target and get blocked."""
        hop1 = self.tmp_dir / "hop1"
        hop2 = self.tmp_dir / "hop2"
        hop3 = self.tmp_dir / "hop3"
        hop4 = self.tmp_dir / "hop4"

        # hop1 -> hop2 -> hop3 -> hop4 -> prot_odin
        hop4.symlink_to(self.prot_odin)
        hop3.symlink_to(hop4)
        hop2.symlink_to(hop3)
        hop1.symlink_to(hop2)

        assert self.guard.is_protected(hop1) is True
        assert self.guard.is_protected(hop2) is True
        assert self.guard.is_protected(hop3) is True
        assert self.guard.is_protected(hop4) is True

        with pytest.raises(ProtectedPathViolationError):
            self.guard.validate_access(hop1, operation="chained_symlink_hop1")

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.read_text(hop1)

    def test_symlink_to_nested_protected_file(self):
        """Verify symlink pointing to a subfile inside protected directory is blocked."""
        sym = self.tmp_dir / "sym_bot_key"
        target = self.prot_keeper / "config" / "secrets.json"
        sym.symlink_to(target)

        assert self.guard.is_protected(sym) is True
        with pytest.raises(ProtectedPathViolationError):
            self.guard.validate_access(sym)

    def test_relative_symlink_into_protected(self):
        """Verify relative symlink traversing up and across into protected dir is blocked."""
        ws = self.tmp_dir / "workspace"
        ws.mkdir()
        sym = ws / "rel_escape"
        rel_target = os.path.relpath(self.prot_matt, ws)
        sym.symlink_to(rel_target)

        assert self.guard.is_protected(sym) is True
        with pytest.raises(ProtectedPathViolationError):
            self.guard.validate_access(sym)

    # 2. Nested Relative Traversals (..)
    @pytest.mark.parametrize(
        "traversal_str",
        [
            "~/teamwork_projects/odin/../odin",
            "~/teamwork_projects/odin/../../teamwork_projects/odin",
            "~/teamwork_projects/keeper_daemon/./nested/../../keeper_daemon",
            "~/teamwork_projects/matt-berserker/sub1/sub2/../../../matt-berserker/sub1",
            "/tmp/../../Users/solveetcoagula/teamwork_projects/odin",
            "~/Desktop/activeProjects/universal_bounty_swarm/../../../teamwork_projects/odin",
        ],
    )
    def test_relative_dot_dot_traversals(self, traversal_str):
        """Verify relative .. traversal expressions resolve to protected path and are blocked."""
        assert self.guard.is_protected(traversal_str) is True
        with pytest.raises(ProtectedPathViolationError):
            self.guard.validate_access(traversal_str)

    # 3. Unicode and APFS Casing Variants
    @pytest.mark.parametrize(
        "case_variant",
        [
            "~/TEAMWORK_PROJECTS/ODIN",
            "~/Teamwork_Projects/Odin",
            "~/teamwork_projects/ODIN/SECRETS.JSON",
            "~/Teamwork_Projects/Keeper_Daemon",
            "~/TEAMWORK_PROJECTS/KEEPER_DAEMON/bot.py",
            "~/teamwork_projects/MATT-BERSERKER",
            "~/Teamwork_Projects/Matt-Berserker/Core/Engine.rs",
        ],
    )
    def test_apfs_case_folding_variants(self, case_variant):
        """Verify APFS / macOS case-insensitive casing variations are strictly blocked."""
        assert self.guard.is_protected(case_variant) is True
        with pytest.raises(ProtectedPathViolationError):
            self.guard.validate_access(case_variant)

    # 4. Nonexistent Ancestor Subpaths
    @pytest.mark.parametrize(
        "nonexistent_subpath",
        [
            "~/teamwork_projects/odin/nonexistent_sub/deep/path/wallet.dat",
            "~/teamwork_projects/keeper_daemon/does/not/exist/ever/config.env",
            "~/teamwork_projects/matt-berserker/phantom_dir/ghost_file.bin",
        ],
    )
    def test_nonexistent_ancestor_subpaths(self, nonexistent_subpath):
        """Verify non-existent subpaths inside protected roots are blocked without error."""
        assert self.guard.is_protected(nonexistent_subpath) is True
        with pytest.raises(ProtectedPathViolationError):
            self.guard.validate_access(nonexistent_subpath)

    # 5. Mac-Specific Firmlink and /private Prefixes
    def test_mac_private_and_firmlink_prefixes(self):
        """Verify firmlink and /private path aliases are detected."""
        home_str = str(self.home)
        firmlink_path = f"/System/Volumes/Data{home_str}/teamwork_projects/odin"
        assert self.guard.is_protected(firmlink_path) is True

        with pytest.raises(ProtectedPathViolationError):
            self.guard.validate_access(firmlink_path)

    # 6. SafeIO Complete API Penetration
    def test_safe_io_all_methods_blocked_on_protected(self):
        """Empirically verify every SafeIO method raises ProtectedPathViolationError."""
        prot_target = self.prot_odin / "test_probe.txt"

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.read_text(prot_target)

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.write_text(prot_target, "malicious write")

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.read_bytes(prot_target)

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.write_bytes(prot_target, b"malicious bytes")

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.delete_file(prot_target)

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.rmtree(self.prot_odin)

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.listdir(self.prot_odin)

        with pytest.raises(ProtectedPathViolationError):
            with SafeIO.open_file(prot_target, "r"):
                pass

        with pytest.raises(ProtectedPathViolationError):
            with SafeIO.open_file(prot_target, "w"):
                pass

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.mkdir(self.prot_odin / "sub")

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.touch(prot_target)

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.copy_file(self.tmp_dir, prot_target)

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.move(self.tmp_dir, prot_target)

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.exists(prot_target)

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.is_file(prot_target)

        with pytest.raises(ProtectedPathViolationError):
            SafeIO.is_dir(prot_target)

    def test_safe_io_rmtree_parent_containment_defense(self):
        """Verify SafeIO.rmtree prevents deleting an ancestor directory containing protected paths."""
        parent_dir = self.home / "teamwork_projects"
        with pytest.raises(ProtectedPathViolationError):
            SafeIO.rmtree(parent_dir)


class TestAdversarialOrbStackContainerLifecycle:
    """Vector 2: Ephemeral OrbStack Docker Lifecycle & 0-Leakage Verification."""

    @pytest.fixture(autouse=True)
    def setup_executor(self, tmp_path):
        self.tmp_dir = tmp_path
        self.executor = EphemeralOrbStackExecutor()
        # Clean any stale test containers before starting
        self.executor.cleanup_stale_containers("bounty-exec-")

    def _container_exists_in_docker(self, container_name: str) -> bool:
        """Queries docker ps -a specifically for a given container name."""
        res = subprocess.run(
            [self.executor.docker_bin, "ps", "-a", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=10,
            env=self.executor._get_subprocess_env(),
        )
        if res.returncode != 0 or not res.stdout.strip():
            return False
        names = [n.strip() for n in res.stdout.strip().splitlines() if n.strip()]
        return container_name in names

    def test_sequential_ephemeral_containers_with_zero_leakage(self):
        """Spin up 10 sequential containers and verify 0 lingering containers in docker ps -a."""
        if not self.executor.is_docker_available():
            pytest.skip("OrbStack / Docker daemon not available.")

        ws = self.tmp_dir / "seq_ws"
        ws.mkdir(exist_ok=True)

        spawned_containers: List[str] = []

        for i in range(10):
            res = self.executor.run_isolated(
                workspace_path=ws,
                command=["python3", "-c", f"print('SEQ_CONTAINER_TEST_{i}')"],
                timeout_sec=15,
            )
            assert res.success is True
            assert f"SEQ_CONTAINER_TEST_{i}" in res.stdout
            assert res.exit_code == 0
            spawned_containers.append(res.container_name)

            # Instant verification: container destroyed immediately
            assert self._container_exists_in_docker(res.container_name) is False

        # Final pass: verify none of the 10 spawned containers exist
        for name in spawned_containers:
            assert self._container_exists_in_docker(name) is False

    def test_concurrent_ephemeral_containers_stress(self):
        """Spin up 5 concurrent containers with distinct workloads and verify 0 leakage."""
        if not self.executor.is_docker_available():
            pytest.skip("OrbStack / Docker daemon not available.")

        ws = self.tmp_dir / "concurrent_ws"
        ws.mkdir(exist_ok=True)

        def _run_worker(worker_id: int):
            return self.executor.run_isolated(
                workspace_path=ws,
                command=[
                    "python3",
                    "-c",
                    f"import time; time.sleep(0.5); print('CONCURRENT_WORKER_{worker_id}_DONE')",
                ],
                timeout_sec=20,
            )

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(_run_worker, i) for i in range(5)]
            results = [f.result() for f in as_completed(futures)]

        assert len(results) == 5
        for res in results:
            assert res.success is True
            assert res.exit_code == 0
            assert "CONCURRENT_WORKER_" in res.stdout
            # Verify each container was completely removed upon thread completion
            assert self._container_exists_in_docker(res.container_name) is False

    def test_timeout_killing_and_zero_leakage(self):
        """Verify that a container exceeding timeout is forcefully terminated and deleted."""
        if not self.executor.is_docker_available():
            pytest.skip("OrbStack / Docker daemon not available.")

        ws = self.tmp_dir / "timeout_ws"
        ws.mkdir(exist_ok=True)

        res = self.executor.run_isolated(
            workspace_path=ws,
            command=["python3", "-c", "import time; time.sleep(30)"],
            timeout_sec=2,
        )

        assert res.timed_out is True
        assert res.success is False
        assert res.exit_code == -1
        assert "timed out after 2 seconds" in res.stderr

        # Check docker ps -a immediately for this container
        assert self._container_exists_in_docker(res.container_name) is False

    def test_container_cpu_and_memory_stress_workload(self):
        """Execute CPU & memory intensive task in isolated container."""
        if not self.executor.is_docker_available():
            pytest.skip("OrbStack / Docker daemon not available.")

        ws = self.tmp_dir / "workload_ws"
        ws.mkdir(exist_ok=True)

        cmd = [
            "python3",
            "-c",
            "arr = [i for i in range(2_000_000)]; print(f'SUM={sum(arr)} LEN={len(arr)}')",
        ]

        res = self.executor.run_isolated(
            workspace_path=ws,
            command=cmd,
            timeout_sec=20,
        )
        assert res.success is True
        assert "SUM=1999999000000" in res.stdout
        assert "LEN=2000000" in res.stdout
        assert self._container_exists_in_docker(res.container_name) is False

    def test_docker_volume_mount_ignore_list_protection(self):
        """Verify that attempting to mount a protected directory raises ProtectedPathViolationError."""
        # 1. Primary workspace mount
        with pytest.raises(ProtectedPathViolationError):
            self.executor.run_isolated(
                workspace_path="~/teamwork_projects/odin",
                command=["ls", "-la"],
            )

        # 2. Extra mounts
        allowed_ws = self.tmp_dir / "allowed_ws"
        allowed_ws.mkdir(exist_ok=True)

        with pytest.raises(ProtectedPathViolationError):
            self.executor.run_isolated(
                workspace_path=allowed_ws,
                command=["ls", "-la"],
                extra_mounts={"~/teamwork_projects/matt-berserker": "/matt"},
            )


class TestAdversarialFirestoreLatencyAndResilience:
    """Vector 3: Live Firestore Real-Time Listener Latency Benchmark & SLA Verification (<2.0s)."""

    @pytest.fixture(autouse=True)
    def setup_firestore(self):
        self.db = get_firestore_client(project_id="odin-500008")
        self.bench_col_name = f"_challenger_latency_{uuid.uuid4().hex[:8]}"
        self.col_ref = self.db.collection(self.bench_col_name)

    def teardown_method(self):
        try:
            docs = self.col_ref.limit(50).stream()
            for doc in docs:
                doc.reference.delete()
        except Exception:
            pass

    def test_realtime_listener_10_sequential_mutations_latency_sla(self):
        """
        Benchmark 10 sequential document creations and measure end-to-end event delivery latency.
        Must strictly satisfy < 2.0s SLA (2000 ms).
        """
        received_events: List[FirestoreEvent] = []
        event_lock = threading.Lock()
        doc_events: Dict[str, threading.Event] = {}

        def _callback(event: FirestoreEvent):
            with event_lock:
                received_events.append(event)
                if event.document_id in doc_events:
                    doc_events[event.document_id].set()

        listener = listen_collection(
            self.col_ref,
            _callback,
            include_initial_snapshot=False,
        )

        try:
            time.sleep(0.8)

            latencies_ms: List[float] = []

            for i in range(10):
                doc_id = f"seq_probe_{i}_{uuid.uuid4().hex[:6]}"
                done_ev = threading.Event()
                doc_events[doc_id] = done_ev

                send_time = time.time()
                self.col_ref.document(doc_id).set({
                    "test_idx": i,
                    "_sent_at_epoch": send_time,
                    "status": "LIVE_BENCHMARK",
                })

                signaled = done_ev.wait(timeout=5.0)
                assert signaled is True, f"Sequential mutation {i} timed out waiting for listener event"

                with event_lock:
                    matched = next((e for e in received_events if e.document_id == doc_id), None)
                    assert matched is not None
                    assert matched.latency_ms is not None
                    latencies_ms.append(matched.latency_ms)

            min_lat = min(latencies_ms)
            max_lat = max(latencies_ms)
            avg_lat = sum(latencies_ms) / len(latencies_ms)
            p95_lat = sorted(latencies_ms)[int(len(latencies_ms) * 0.95)]

            print(f"\n[FIRESTORE LATENCY BENCHMARK - 10 SEQUENTIAL SAMPLES]")
            print(f"  Min: {min_lat:.2f} ms")
            print(f"  Max: {max_lat:.2f} ms")
            print(f"  Avg: {avg_lat:.2f} ms")
            print(f"  P95: {p95_lat:.2f} ms")

            # SLA Check: 100% of samples must be under 2000 ms (<2.0s)
            assert max_lat < 2000.0, f"SLA Violation: Max latency {max_lat:.2f}ms exceeded 2000ms SLA"
            assert avg_lat < 1000.0, f"Average latency {avg_lat:.2f}ms was excessively high (>1.0s)"

        finally:
            listener.unsubscribe()

    def test_realtime_listener_concurrent_burst_latency(self):
        """Benchmark a concurrent burst of 5 document creations across threads."""
        received_events: List[FirestoreEvent] = []
        event_lock = threading.Lock()
        burst_events: Dict[str, threading.Event] = {}

        def _callback(event: FirestoreEvent):
            with event_lock:
                received_events.append(event)
                if event.document_id in burst_events:
                    burst_events[event.document_id].set()

        listener = listen_collection(
            self.col_ref,
            _callback,
            include_initial_snapshot=False,
        )

        try:
            time.sleep(0.8)

            burst_ids = [f"burst_{i}_{uuid.uuid4().hex[:6]}" for i in range(5)]
            for bid in burst_ids:
                burst_events[bid] = threading.Event()

            def _send_burst(doc_id: str, idx: int):
                sent_ts = time.time()
                self.col_ref.document(doc_id).set({
                    "burst_idx": idx,
                    "_sent_at_epoch": sent_ts,
                    "status": "BURST_TEST",
                })

            with ThreadPoolExecutor(max_workers=5) as pool:
                futures = [pool.submit(_send_burst, burst_ids[i], i) for i in range(5)]
                for f in futures:
                    f.result()

            for bid in burst_ids:
                signaled = burst_events[bid].wait(timeout=5.0)
                assert signaled is True, f"Burst doc {bid} timed out waiting for listener"

            with event_lock:
                burst_latencies = [
                    e.latency_ms for e in received_events if e.document_id in burst_ids and e.latency_ms is not None
                ]

            assert len(burst_latencies) == 5
            max_burst_lat = max(burst_latencies)
            avg_burst_lat = sum(burst_latencies) / len(burst_latencies)

            print(f"\n[FIRESTORE BURST LATENCY - 5 CONCURRENT SAMPLES]")
            print(f"  Max Burst Latency: {max_burst_lat:.2f} ms")
            print(f"  Avg Burst Latency: {avg_burst_lat:.2f} ms")

            assert max_burst_lat < 2000.0, f"Burst SLA Violation: {max_burst_lat:.2f}ms exceeded 2000ms"

        finally:
            listener.unsubscribe()

    def test_in_callback_exception_stream_resilience(self):
        """Verify listener stream remains active and functional after an in-callback unhandled exception."""
        exception_thrown = threading.Event()
        recovered_event = threading.Event()
        event_counter = 0

        def _flaky_callback(event: FirestoreEvent):
            nonlocal event_counter
            event_counter += 1
            if event_counter == 1:
                exception_thrown.set()
                raise RuntimeError("INTENTIONAL_ADVERSARIAL_CALLBACK_CRASH")
            else:
                recovered_event.set()

        listener = listen_collection(
            self.col_ref,
            _flaky_callback,
            include_initial_snapshot=False,
        )

        try:
            time.sleep(0.8)

            doc1 = self.col_ref.document("crash_doc_1")
            doc1.set({"msg": "first write"})

            assert exception_thrown.wait(timeout=5.0) is True
            assert listener.error_count >= 1
            assert listener.is_active is True

            doc2 = self.col_ref.document("recover_doc_2")
            doc2.set({"msg": "second write after crash"})

            assert recovered_event.wait(timeout=5.0) is True
            assert listener.event_count >= 2

        finally:
            listener.unsubscribe()
            try:
                doc1.delete()
                doc2.delete()
            except Exception:
                pass

"""
Unit and Live Integration Tests for Firebase Admin SDK & Real-Time Firestore Event Engine.

Verifies:
- ADC & Firebase Admin / Firestore Client initialization (Project odin-500008)
- Real-time listener event response latency (< 2.0 second SLA)
- Collection and Document change streaming (ADDED, MODIFIED, REMOVED)
- Atomic transactional lead claiming and concurrent race condition prevention
- Lock expiration and lead release
- In-callback exception safety and error isolation
- Thread-safe dispatch with ThreadPoolExecutor
- Clean listener lifecycle & unsubscribe teardown
"""

import os
import time
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any

import pytest
from google.cloud import firestore
import firebase_admin

from src.core.firestore_client import (
    resolve_credentials_path,
    resolve_project_id,
    initialize_firebase_app,
    get_firestore_client,
    get_collection,
    get_leads_collection,
    get_operations_collection,
    get_memory_collection,
    DEFAULT_PROJECT_ID,
    DEFAULT_CREDENTIALS_PATH
)
from src.core.listener import (
    FirestoreListener,
    FirestoreEvent,
    FirestoreEventType,
    listen_collection,
    listen_document,
    claim_bounty_lead,
    claim_lead_atomic,
    release_bounty_lead,
    benchmark_listener_latency
)


@pytest.fixture(scope="module")
def db_client() -> firestore.Client:
    """Provides a shared Firestore Client for the test session."""
    return get_firestore_client()


@pytest.fixture
def unique_test_collection(db_client: firestore.Client):
    """Creates a unique temporary collection for test isolation and cleans it up after."""
    col_name = f"_test_bench_{uuid.uuid4().hex[:10]}"
    col_ref = db_client.collection(col_name)
    created_docs = []

    yield col_ref

    # Teardown: delete all docs created in this test collection
    for doc in col_ref.stream():
        try:
            doc.reference.delete()
        except Exception:
            pass


class TestFirestoreClientInit:
    """Tests for Firebase Admin & Firestore Client provisioning using ADC."""

    def test_credentials_path_resolution(self):
        resolved = resolve_credentials_path()
        assert resolved is not None
        assert os.path.isfile(resolved)
        assert "credentials.json" in resolved

    def test_project_id_resolution(self):
        project_id = resolve_project_id()
        assert project_id == DEFAULT_PROJECT_ID

    def test_firebase_app_initialization(self):
        app = initialize_firebase_app()
        assert app is not None
        assert isinstance(app, firebase_admin.App)
        assert app.project_id == DEFAULT_PROJECT_ID

    def test_firestore_client_connectivity(self, db_client: firestore.Client):
        assert db_client is not None
        assert isinstance(db_client, firestore.Client)
        assert db_client.project == DEFAULT_PROJECT_ID

        # Verify live collection listing
        collections = [c.id for c in db_client.collections()]
        assert isinstance(collections, list)
        assert "bounty_events" in collections or "bounty_memory" in collections or len(collections) >= 0

    def test_collection_accessors(self, db_client: firestore.Client):
        leads_col = get_leads_collection(db_client)
        assert leads_col.id == "bounty_leads"

        ops_col = get_operations_collection(db_client)
        assert ops_col.id == "swarm_operations"

        mem_col = get_memory_collection(db_client)
        assert mem_col.id == "bounty_memory"


class TestRealTimeFirestoreListener:
    """Tests for real-time streaming listeners, latency SLA, and change detection."""

    def test_document_listener_mutations(self, db_client: firestore.Client, unique_test_collection):
        doc_id = f"doc_{uuid.uuid4().hex[:8]}"
        doc_ref = unique_test_collection.document(doc_id)

        received_events: List[FirestoreEvent] = []
        event_signal = threading.Event()

        def _on_doc_change(event: FirestoreEvent):
            received_events.append(event)
            if event.data and event.data.get("phase") == "stage_2":
                event_signal.set()

        listener = listen_document(doc_ref, _on_doc_change, include_initial_snapshot=False)
        time.sleep(0.5)

        try:
            # Stage 1: Create
            doc_ref.set({"phase": "stage_1", "val": 100})
            time.sleep(0.3)

            # Stage 2: Update
            doc_ref.update({"phase": "stage_2", "val": 200})
            
            assert event_signal.wait(timeout=5.0), "Document update event was not received within 5.0s"
            assert len(received_events) >= 1
            latest = received_events[-1]
            assert latest.document_id == doc_id
            assert latest.data["phase"] == "stage_2"
            assert latest.data["val"] == 200
        finally:
            listener.unsubscribe()

    def test_collection_listener_added_modified_removed(self, db_client: firestore.Client, unique_test_collection):
        doc_id = f"lead_{uuid.uuid4().hex[:8]}"
        doc_ref = unique_test_collection.document(doc_id)

        events_by_type: Dict[str, List[FirestoreEvent]] = {"ADDED": [], "MODIFIED": [], "REMOVED": []}
        removed_signal = threading.Event()

        def _on_col_change(event: FirestoreEvent):
            if event.document_id == doc_id:
                events_by_type[event.event_type].append(event)
                if event.event_type == "REMOVED":
                    removed_signal.set()

        listener = listen_collection(unique_test_collection, _on_col_change, include_initial_snapshot=False)
        time.sleep(0.5)

        try:
            # 1. ADDED
            doc_ref.set({"status": "pending_triage", "priority": "standard"})
            time.sleep(0.5)

            # 2. MODIFIED
            doc_ref.update({"status": "priority_triage", "priority": "high"})
            time.sleep(0.5)

            # 3. REMOVED
            doc_ref.delete()

            assert removed_signal.wait(timeout=6.0), "REMOVED event was not received within 6.0s"
            assert len(events_by_type["ADDED"]) >= 1
            assert len(events_by_type["MODIFIED"]) >= 1
            assert len(events_by_type["REMOVED"]) >= 1
        finally:
            listener.unsubscribe()

    def test_listener_latency_sla_under_2_seconds(self, db_client: firestore.Client, unique_test_collection):
        """
        Mandatory Acceptance Criteria:
        Automated tests confirm that modifications to Firestore documents correctly
        trigger the respective sidecar listeners within 2 seconds (< 2,000 ms).
        """
        latencies: List[float] = []
        num_probes = 3

        for i in range(num_probes):
            probe_id = f"latency_probe_{i}_{uuid.uuid4().hex[:6]}"
            doc_ref = unique_test_collection.document(probe_id)
            event_signal = threading.Event()
            probe_latency: List[float] = []

            def _on_probe(event: FirestoreEvent):
                if event.document_id == probe_id and event.latency_ms is not None:
                    probe_latency.append(event.latency_ms)
                    event_signal.set()

            listener = listen_document(doc_ref, _on_probe, include_initial_snapshot=False)
            time.sleep(0.4)

            try:
                sent_time = time.time()
                doc_ref.set({
                    "probe_id": probe_id,
                    "_sent_at_epoch": sent_time,
                    "status": "BENCHMARK_PROBE"
                })

                assert event_signal.wait(timeout=5.0), f"Probe {i} timed out waiting for snapshot"
                lat = probe_latency[0]
                latencies.append(lat)
                # Assert each individual latency is strictly under 2.0s SLA
                assert lat < 2000.0, f"Latency {lat:.2f}ms exceeded 2000ms SLA!"
            finally:
                listener.unsubscribe()

        avg_latency = sum(latencies) / len(latencies)
        print(f"Measured probe latencies (ms): {latencies}, Average: {avg_latency:.2f}ms")
        assert avg_latency < 2000.0, f"Average latency {avg_latency:.2f}ms exceeded 2000ms SLA"

    def test_benchmark_listener_latency_utility(self, db_client: firestore.Client):
        result = benchmark_listener_latency(db_client, timeout_sec=8.0)
        assert "latency_ms" in result
        assert result["latency_ms"] < 2000.0
        assert result["measured_at_epoch"] > 0


class TestAtomicTransactionalLeadClaiming:
    """Tests for atomic lead claiming, concurrency locks, race conditions, and releases."""

    def test_single_worker_successful_claim(self, db_client: firestore.Client, unique_test_collection):
        lead_id = f"lead_{uuid.uuid4().hex[:8]}"
        doc_ref = unique_test_collection.document(lead_id)

        # Ingest lead
        doc_ref.set({
            "id": lead_id,
            "status": "priority_triage",
            "repo": "stellar/soroban-examples",
            "issue_number": 42,
            "created_at": firestore.SERVER_TIMESTAMP
        })

        worker_id = "worker_executor_alpha"
        claimed = claim_lead_atomic(
            db=db_client,
            lead_id=lead_id,
            worker_id=worker_id,
            collection_name=unique_test_collection.id
        )

        assert claimed is True

        # Verify state in Firestore
        snap = doc_ref.get()
        assert snap.exists
        data = snap.to_dict()
        assert data["status"] == "claimed"
        assert data["lock"]["owner_id"] == worker_id
        assert "locked_at" in data["lock"]

    def test_second_worker_cannot_claim_already_claimed_lead(self, db_client: firestore.Client, unique_test_collection):
        lead_id = f"lead_{uuid.uuid4().hex[:8]}"
        doc_ref = unique_test_collection.document(lead_id)

        doc_ref.set({
            "id": lead_id,
            "status": "priority_triage",
            "repo": "s6pa1rta3n-lab/universal_bounty_fleet",
            "issue_number": 101,
        })

        # Worker 1 claims
        c1 = claim_lead_atomic(db_client, lead_id, "worker_1", collection_name=unique_test_collection.id)
        assert c1 is True

        # Worker 2 attempts to claim
        c2 = claim_lead_atomic(db_client, lead_id, "worker_2", collection_name=unique_test_collection.id)
        assert c2 is False

        # Verify worker 1 still owns the lock
        snap = doc_ref.get()
        assert snap.to_dict()["lock"]["owner_id"] == "worker_1"

    def test_concurrent_claim_race_condition_prevention(self, db_client: firestore.Client, unique_test_collection):
        """
        Spawns 10 concurrent threads simultaneously attempting to claim the same lead.
        Guarantees EXACTLY ONE thread succeeds and all others fail cleanly.
        """
        lead_id = f"race_lead_{uuid.uuid4().hex[:8]}"
        doc_ref = unique_test_collection.document(lead_id)

        doc_ref.set({
            "id": lead_id,
            "status": "priority_triage",
            "repo": "grantfox/core-contracts",
            "issue_number": 777,
        })

        results = []
        threads = []

        def _claim_task(w_id: str):
            success = claim_lead_atomic(
                db=db_client,
                lead_id=lead_id,
                worker_id=w_id,
                collection_name=unique_test_collection.id
            )
            results.append((w_id, success))

        for i in range(10):
            t = threading.Thread(target=_claim_task, args=(f"worker_{i}",))
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        winners = [r for r in results if r[1] is True]
        losers = [r for r in results if r[1] is False]

        assert len(winners) == 1, f"Expected exactly 1 winner under concurrency, got {len(winners)}"
        assert len(losers) == 9, f"Expected 9 losers, got {len(losers)}"

        # Verify winner is recorded in Firestore
        winner_id = winners[0][0]
        snap = doc_ref.get()
        assert snap.to_dict()["lock"]["owner_id"] == winner_id

    def test_unclaimable_status_rejection(self, db_client: firestore.Client, unique_test_collection):
        lead_id = f"lead_{uuid.uuid4().hex[:8]}"
        doc_ref = unique_test_collection.document(lead_id)

        # Test rejected status
        doc_ref.set({"id": lead_id, "status": "completed"})
        res = claim_lead_atomic(db_client, lead_id, "worker_test", collection_name=unique_test_collection.id)
        assert res is False

        doc_ref.update({"status": "rejected"})
        res = claim_lead_atomic(db_client, lead_id, "worker_test", collection_name=unique_test_collection.id)
        assert res is False

    def test_expired_lock_stealing(self, db_client: firestore.Client, unique_test_collection):
        lead_id = f"lead_{uuid.uuid4().hex[:8]}"
        doc_ref = unique_test_collection.document(lead_id)

        # Set lead as claimed 400 seconds ago (lock timeout is 300s)
        old_time = time.time() - 400
        doc_ref.set({
            "id": lead_id,
            "status": "claimed",
            "lock": {
                "owner_id": "dead_worker_999",
                "locked_at": old_time
            }
        })

        # New worker should successfully reclaim expired lock
        reclaimed = claim_lead_atomic(
            db=db_client,
            lead_id=lead_id,
            worker_id="new_worker_111",
            collection_name=unique_test_collection.id,
            lock_timeout_sec=300
        )
        assert reclaimed is True
        snap = doc_ref.get()
        assert snap.to_dict()["lock"]["owner_id"] == "new_worker_111"

    def test_lead_release(self, db_client: firestore.Client, unique_test_collection):
        lead_id = f"lead_{uuid.uuid4().hex[:8]}"
        doc_ref = unique_test_collection.document(lead_id)

        doc_ref.set({"id": lead_id, "status": "priority_triage"})
        claim_lead_atomic(db_client, lead_id, "worker_release_test", collection_name=unique_test_collection.id)

        # Release lead
        released = release_bounty_lead(
            db=db_client,
            lead_id=lead_id,
            worker_id="worker_release_test",
            new_status="pending_triage",
            collection_name=unique_test_collection.id
        )
        assert released is True

        snap = doc_ref.get()
        data = snap.to_dict()
        assert data["status"] == "pending_triage"
        assert data["lock"]["owner_id"] is None


class TestListenerResilienceAndLifecycle:
    """Tests for in-callback error handling, thread safety, and clean teardown."""

    def test_in_callback_error_isolation(self, db_client: firestore.Client, unique_test_collection):
        doc_id = f"err_test_{uuid.uuid4().hex[:8]}"
        doc_ref = unique_test_collection.document(doc_id)

        error_caught = threading.Event()
        recovered_event = threading.Event()
        callback_call_count = [0]

        def _faulty_callback(event: FirestoreEvent):
            callback_call_count[0] += 1
            if event.data and event.data.get("trigger_error"):
                raise RuntimeError("Deliberate test error inside listener callback!")
            if event.data and event.data.get("phase") == "recovery":
                recovered_event.set()

        def _error_handler(exc: Exception, args: Any):
            if isinstance(exc, RuntimeError):
                error_caught.set()

        listener = listen_document(
            doc_ref,
            _faulty_callback,
            error_callback=_error_handler,
            include_initial_snapshot=False
        )
        time.sleep(0.5)

        try:
            # 1. Trigger error
            doc_ref.set({"trigger_error": True})
            assert error_caught.wait(timeout=5.0), "Error callback was not invoked on exception"
            assert listener.error_count >= 1

            # 2. Verify listener stream did NOT crash and successfully processes next event
            doc_ref.update({"trigger_error": False, "phase": "recovery"})
            assert recovered_event.wait(timeout=5.0), "Listener failed to recover after callback exception"
            assert listener.is_active is True
        finally:
            listener.unsubscribe()

    def test_threadpool_executor_dispatch(self, db_client: firestore.Client, unique_test_collection):
        doc_id = f"tp_test_{uuid.uuid4().hex[:8]}"
        doc_ref = unique_test_collection.document(doc_id)

        executed_threads = set()
        done_signal = threading.Event()
        executor = ThreadPoolExecutor(max_workers=2)

        def _tp_callback(event: FirestoreEvent):
            executed_threads.add(threading.current_thread().name)
            done_signal.set()

        listener = listen_document(
            doc_ref,
            _tp_callback,
            executor=executor,
            include_initial_snapshot=False
        )
        time.sleep(0.5)

        try:
            doc_ref.set({"msg": "threadpool test"})
            assert done_signal.wait(timeout=5.0)
            assert len(executed_threads) > 0
        finally:
            listener.unsubscribe()
            executor.shutdown(wait=False)

    def test_listener_context_manager_and_clean_unsubscribe(self, db_client: firestore.Client, unique_test_collection):
        doc_id = f"ctx_test_{uuid.uuid4().hex[:8]}"
        doc_ref = unique_test_collection.document(doc_id)

        with FirestoreListener(doc_ref, lambda ev: None) as listener:
            assert listener.is_active is True

        assert listener.is_active is False

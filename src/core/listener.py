"""
Real-Time Firestore Event Engine & Atomic Transaction Locking.

Provides:
- High-performance, low-latency Firestore listeners (< 2.0s SLA)
- Collection, Query, and Document streaming watches with on_snapshot
- Thread-safe callback dispatch and in-callback error isolation
- Atomic transactional lead claiming to eliminate multi-worker race conditions
- Latency benchmarking and telemetry utilities
"""

import time
import inspect
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from google.cloud import firestore

logger = logging.getLogger("UniversalBountySwarm.FirestoreListener")


class FirestoreEventType(str, Enum):
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    REMOVED = "REMOVED"
    DOCUMENT = "DOCUMENT"


@dataclass
class FirestoreEvent:
    """Structured representation of a real-time Firestore event."""
    document_id: str
    data: Optional[Dict[str, Any]]
    event_type: str  # 'ADDED', 'MODIFIED', 'REMOVED', 'DOCUMENT'
    old_index: int = -1
    new_index: int = -1
    read_time: Any = None
    received_at: float = field(default_factory=time.time)
    latency_ms: Optional[float] = None
    snapshot: Any = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "document_id": self.document_id,
            "data": self.data,
            "event_type": self.event_type,
            "old_index": self.old_index,
            "new_index": self.new_index,
            "read_time": str(self.read_time) if self.read_time else None,
            "received_at": self.received_at,
            "latency_ms": self.latency_ms,
        }


class FirestoreListener:
    """
    Robust, thread-safe real-time listener for Firestore Collections, Queries, and Documents.
    
    Guarantees:
    - In-callback exception safety (listener stream never crashes on handler errors)
    - Sub-second propagation latency
    - Clean lifecycle & thread termination via unsubscribe()
    - Latency benchmarking and telemetry
    """

    def __init__(
        self,
        target: Union[firestore.CollectionReference, firestore.Query, firestore.DocumentReference],
        callback: Callable[..., Any],
        error_callback: Optional[Callable[[Exception, Any], None]] = None,
        executor: Optional[ThreadPoolExecutor] = None,
        include_initial_snapshot: bool = True,
        auto_start: bool = False,
    ):
        self.target = target
        self.callback = callback
        self.error_callback = error_callback
        self.executor = executor
        self.include_initial_snapshot = include_initial_snapshot

        self._watch: Optional[Any] = None
        self._is_active: bool = False
        self._lock = threading.RLock()
        self._event_count: int = 0
        self._error_count: int = 0
        self._initial_snapshot_delivered: bool = False
        self._latency_history: List[float] = []

        self._is_document: bool = isinstance(target, firestore.DocumentReference)

        if auto_start:
            self.start()

    @property
    def is_active(self) -> bool:
        with self._lock:
            return self._is_active

    @property
    def event_count(self) -> int:
        with self._lock:
            return self._event_count

    @property
    def error_count(self) -> int:
        with self._lock:
            return self._error_count

    @property
    def average_latency_ms(self) -> Optional[float]:
        with self._lock:
            if not self._latency_history:
                return None
            return sum(self._latency_history) / len(self._latency_history)

    def start(self) -> "FirestoreListener":
        """Starts the real-time gRPC snapshot stream."""
        with self._lock:
            if self._is_active:
                logger.warning("FirestoreListener is already running.")
                return self

            logger.info(f"Starting FirestoreListener on target: {self.target}")
            if self._is_document:
                self._watch = self.target.on_snapshot(self._handle_document_snapshot)
            else:
                self._watch = self.target.on_snapshot(self._handle_collection_snapshot)

            self._is_active = True
            return self

    def stop(self) -> None:
        """Stops the real-time listener and cleans up stream resources."""
        self.unsubscribe()

    def unsubscribe(self) -> None:
        """Unsubscribes from the Firestore snapshot watch stream."""
        with self._lock:
            if not self._is_active:
                return

            self._is_active = False
            if self._watch is not None:
                try:
                    self._watch.unsubscribe()
                except Exception as e:
                    logger.debug(f"Error unsubscribing watch: {e}")
                self._watch = None

            logger.info("FirestoreListener successfully unsubscribed.")

    def __enter__(self) -> "FirestoreListener":
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()

    def _calculate_latency(self, data: Optional[Dict[str, Any]], received_at: float) -> Optional[float]:
        """Extracts benchmark timestamps from document data and computes latency in ms."""
        if not data or not isinstance(data, dict):
            return None

        # Check for benchmark timestamp fields
        for field_name in ["_sent_at_epoch", "benchmark_ts", "created_at_epoch", "updated_at_epoch"]:
            val = data.get(field_name)
            if isinstance(val, (int, float)) and val > 0:
                diff = (received_at - val) * 1000.0
                if 0 <= diff <= 60000:  # Sensible range (< 60s)
                    return round(diff, 2)

        return None

    def _dispatch_callback(self, handler_args: tuple, kwargs: Optional[dict] = None) -> None:
        """Safely dispatches the user callback either via executor or inline."""
        kwargs = kwargs or {}

        def _safe_run():
            try:
                # Inspect callback signature to pass expected arguments
                sig = inspect.signature(self.callback)
                num_params = len(sig.parameters)
                
                # Support:
                # 1 param: callback(event) or callback(changes)
                # 2 params: callback(snapshot, changes)
                # 3 params: callback(snapshot, changes, read_time)
                if num_params == 1:
                    self.callback(handler_args[0])
                elif num_params == 2 and len(handler_args) >= 2:
                    self.callback(handler_args[0], handler_args[1])
                elif num_params >= 3 and len(handler_args) >= 3:
                    self.callback(handler_args[0], handler_args[1], handler_args[2])
                else:
                    self.callback(*handler_args, **kwargs)
            except Exception as e:
                with self._lock:
                    self._error_count += 1
                logger.error(f"[!] Exception in FirestoreListener callback: {e}", exc_info=True)
                if self.error_callback:
                    try:
                        self.error_callback(e, handler_args)
                    except Exception as err_cb_err:
                        logger.error(f"[!] Exception in FirestoreListener error_callback: {err_cb_err}")

        if self.executor:
            self.executor.submit(_safe_run)
        else:
            _safe_run()

    def _handle_collection_snapshot(self, col_snapshot: Any, changes: Any, read_time: Any) -> None:
        """Internal handler for collection and query snapshots."""
        received_at = time.time()
        with self._lock:
            if not self._is_active:
                return

            if not self._initial_snapshot_delivered:
                self._initial_snapshot_delivered = True
                if not self.include_initial_snapshot and not changes:
                    return

            events: List[FirestoreEvent] = []
            for change in changes:
                doc = getattr(change, "document", getattr(change, "doc", None))
                if doc is None:
                    continue
                data = doc.to_dict() if getattr(doc, "exists", False) else None
                c_type = change.type.name if hasattr(change.type, "name") else str(change.type)
                latency = self._calculate_latency(data, received_at)

                if latency is not None:
                    self._latency_history.append(latency)
                    if len(self._latency_history) > 100:
                        self._latency_history.pop(0)

                event = FirestoreEvent(
                    document_id=doc.id,
                    data=data,
                    event_type=c_type,
                    old_index=change.old_index,
                    new_index=change.new_index,
                    read_time=read_time,
                    received_at=received_at,
                    latency_ms=latency,
                    snapshot=doc,
                )
                events.append(event)
                self._event_count += 1

            if not events and not self.include_initial_snapshot:
                return

        # Check callback signature: if single param and expects event, dispatch per event or full list
        try:
            sig = inspect.signature(self.callback)
            num_params = len(sig.parameters)
        except Exception:
            num_params = 1

        if num_params == 1 and events:
            for ev in events:
                self._dispatch_callback((ev,))
        else:
            self._dispatch_callback((col_snapshot, changes, read_time))

    def _handle_document_snapshot(self, doc_snapshots: Any, previous_flags: Any, read_time: Any) -> None:
        """Internal handler for document snapshots."""
        received_at = time.time()
        with self._lock:
            if not self._is_active:
                return

            # For single doc on_snapshot, first arg is usually a list with one DocumentSnapshot
            doc_snap = doc_snapshots[0] if isinstance(doc_snapshots, list) and len(doc_snapshots) > 0 else doc_snapshots

            if not self._initial_snapshot_delivered:
                self._initial_snapshot_delivered = True
                if not self.include_initial_snapshot and not getattr(doc_snap, "exists", False):
                    return

            data = doc_snap.to_dict() if getattr(doc_snap, "exists", False) else None
            doc_id = getattr(doc_snap, "id", "unknown")
            latency = self._calculate_latency(data, received_at)

            if latency is not None:
                self._latency_history.append(latency)
                if len(self._latency_history) > 100:
                    self._latency_history.pop(0)

            event = FirestoreEvent(
                document_id=doc_id,
                data=data,
                event_type=FirestoreEventType.DOCUMENT.value,
                read_time=read_time,
                received_at=received_at,
                latency_ms=latency,
                snapshot=doc_snap,
            )
            self._event_count += 1

        try:
            sig = inspect.signature(self.callback)
            num_params = len(sig.parameters)
        except Exception:
            num_params = 1

        if num_params == 1:
            self._dispatch_callback((event,))
        else:
            self._dispatch_callback((doc_snapshots, previous_flags, read_time))


# Top-level helper functions matching Interface Contracts
def listen_collection(
    col_ref: Union[firestore.CollectionReference, firestore.Query],
    callback: Callable[..., Any],
    error_callback: Optional[Callable[[Exception, Any], None]] = None,
    executor: Optional[ThreadPoolExecutor] = None,
    include_initial_snapshot: bool = True
) -> FirestoreListener:
    """
    Starts listening to a Firestore Collection or Query in real-time.

    Args:
        col_ref: Firestore CollectionReference or Query.
        callback: Function invoked when changes arrive.
        error_callback: Optional error handler function.
        executor: Optional ThreadPoolExecutor for asynchronous callback processing.
        include_initial_snapshot: Whether to trigger callback for existing docs on start.

    Returns:
        Active FirestoreListener instance (call .unsubscribe() to stop).
    """
    listener = FirestoreListener(
        target=col_ref,
        callback=callback,
        error_callback=error_callback,
        executor=executor,
        include_initial_snapshot=include_initial_snapshot,
        auto_start=True
    )
    return listener


def listen_document(
    doc_ref: firestore.DocumentReference,
    callback: Callable[..., Any],
    error_callback: Optional[Callable[[Exception, Any], None]] = None,
    executor: Optional[ThreadPoolExecutor] = None,
    include_initial_snapshot: bool = True
) -> FirestoreListener:
    """
    Starts listening to a single Firestore Document in real-time.

    Args:
        doc_ref: Firestore DocumentReference.
        callback: Function invoked when document mutations arrive.
        error_callback: Optional error handler function.
        executor: Optional ThreadPoolExecutor.
        include_initial_snapshot: Whether to trigger on initial snapshot.

    Returns:
        Active FirestoreListener instance (call .unsubscribe() to stop).
    """
    listener = FirestoreListener(
        target=doc_ref,
        callback=callback,
        error_callback=error_callback,
        executor=executor,
        include_initial_snapshot=include_initial_snapshot,
        auto_start=True
    )
    return listener


# Atomic Transactional Lead Claiming
@firestore.transactional
def claim_bounty_lead(
    transaction: firestore.Transaction,
    doc_ref: firestore.DocumentReference,
    worker_id: str,
    allowed_statuses: Sequence[str] = ("priority_triage", "pending_triage"),
    lock_timeout_sec: int = 300
) -> bool:
    """
    Atomically claims a bounty lead in Firestore using an ACID transaction.
    
    Prevents race conditions across parallel sidecars.
    If already claimed by another worker, checks whether the lock has expired (> lock_timeout_sec).

    Args:
        transaction: Firestore ACID transaction object.
        doc_ref: DocumentReference pointing to bounty_leads/{lead_id}.
        worker_id: Unique identifier of the requesting worker process/thread.
        allowed_statuses: Tuple/list of statuses eligible for claiming.
        lock_timeout_sec: Number of seconds before a stale lock can be stolen/reclaimed.

    Returns:
        True if the lead was successfully claimed and locked, False otherwise.
    """
    snapshot = doc_ref.get(transaction=transaction)
    if not snapshot.exists:
        logger.debug(f"Lead doc {doc_ref.id} does not exist for claim.")
        return False

    data = snapshot.to_dict() or {}
    current_status = data.get("status")

    # Check existing status
    if current_status == "claimed":
        # Check lock expiration
        lock_info = data.get("lock") or {}
        locked_at = lock_info.get("locked_at")
        is_expired = False

        if locked_at is not None:
            now_ts = time.time()
            if hasattr(locked_at, "timestamp"):
                age_sec = now_ts - locked_at.timestamp()
            elif isinstance(locked_at, (int, float)):
                age_sec = now_ts - locked_at
            else:
                age_sec = 0.0

            if age_sec > lock_timeout_sec:
                logger.warning(
                    f"Lead {doc_ref.id} lock expired (held by {lock_info.get('owner_id')} for {age_sec:.1f}s). Reclaiming for {worker_id}."
                )
                is_expired = True

        if not is_expired:
            return False  # Active lock held by someone else
    elif current_status not in allowed_statuses:
        logger.debug(f"Lead {doc_ref.id} in non-claimable status '{current_status}'.")
        return False

    # Atomically lock and update
    transaction.update(doc_ref, {
        "status": "claimed",
        "lock": {
            "owner_id": worker_id,
            "locked_at": firestore.SERVER_TIMESTAMP,
            "lock_timeout_sec": lock_timeout_sec
        },
        "updated_at": firestore.SERVER_TIMESTAMP
    })
    return True


def claim_lead_atomic(
    db: firestore.Client,
    lead_id: str,
    worker_id: str,
    collection_name: str = "bounty_leads",
    lock_timeout_sec: int = 300,
    allowed_statuses: Sequence[str] = ("priority_triage", "pending_triage"),
    max_retries: int = 5
) -> bool:
    """
    Executes claim_bounty_lead inside an ACID transaction with exponential backoff retries.

    Args:
        db: Firestore Client.
        lead_id: Document ID in bounty_leads.
        worker_id: Worker process/thread identifier.
        collection_name: Collection name (defaults to 'bounty_leads').
        lock_timeout_sec: Lock expiry duration in seconds.
        allowed_statuses: Eligible claim statuses.
        max_retries: Maximum transactional retries on contention.

    Returns:
        True if claimed, False if conflict or unclaimable.
    """
    doc_ref = db.collection(collection_name).document(lead_id)
    transaction = db.transaction(max_attempts=max_retries)

    try:
        claimed = claim_bounty_lead(
            transaction=transaction,
            doc_ref=doc_ref,
            worker_id=worker_id,
            allowed_statuses=allowed_statuses,
            lock_timeout_sec=lock_timeout_sec
        )
        return bool(claimed)
    except Exception as e:
        logger.warning(f"Transaction failed while claiming lead {lead_id} for {worker_id}: {e}")
        return False


def release_bounty_lead(
    db: firestore.Client,
    lead_id: str,
    worker_id: str,
    new_status: str = "pending_triage",
    collection_name: str = "bounty_leads"
) -> bool:
    """
    Releases a claimed lead back to available status or updates its status.
    Only the owning worker (or force release) can release.
    """
    doc_ref = db.collection(collection_name).document(lead_id)
    transaction = db.transaction()

    @firestore.transactional
    def _release(tx, ref):
        snap = ref.get(transaction=tx)
        if not snap.exists:
            return False
        data = snap.to_dict() or {}
        lock_info = data.get("lock") or {}
        owner = lock_info.get("owner_id")
        if owner and owner != worker_id and worker_id != "FORCE_OVERRIDE":
            return False

        tx.update(ref, {
            "status": new_status,
            "lock": {
                "owner_id": None,
                "locked_at": None,
                "released_at": firestore.SERVER_TIMESTAMP
            },
            "updated_at": firestore.SERVER_TIMESTAMP
        })
        return True

    try:
        return bool(_release(transaction, doc_ref))
    except Exception as e:
        logger.warning(f"Failed to release lead {lead_id}: {e}")
        return False


def benchmark_listener_latency(
    db: firestore.Client,
    collection_name: str = "_bounty_latency_benchmark",
    timeout_sec: float = 10.0
) -> Dict[str, float]:
    """
    Measures empirical real-time event delivery latency against live Firestore.

    Returns:
        Dict with 'latency_ms' and 'measured_at_epoch'.
    """
    import uuid
    test_id = f"bench_{uuid.uuid4().hex[:8]}"
    col_ref = db.collection(collection_name)
    doc_ref = col_ref.document(test_id)

    event_received = threading.Event()
    measured_latency: List[float] = []

    def _bench_cb(event: FirestoreEvent):
        if event.document_id == test_id and event.latency_ms is not None:
            measured_latency.append(event.latency_ms)
            event_received.set()

    listener = listen_collection(col_ref, _bench_cb, include_initial_snapshot=False)

    try:
        # Give listener connection a brief moment to stabilize
        time.sleep(0.5)

        sent_time = time.time()
        doc_ref.set({
            "test_id": test_id,
            "_sent_at_epoch": sent_time,
            "status": "BENCHMARK_PROBE"
        })

        success = event_received.wait(timeout=timeout_sec)
        if not success or not measured_latency:
            raise TimeoutError(f"Latency benchmark timed out after {timeout_sec}s")

        lat = measured_latency[0]
        logger.info(f"Benchmark measured live event latency: {lat:.2f} ms")
        return {
            "latency_ms": lat,
            "measured_at_epoch": sent_time
        }
    finally:
        listener.unsubscribe()
        try:
            doc_ref.delete()
        except Exception:
            pass

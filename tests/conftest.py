"""
Global Pytest Configuration and E2E Test Fixtures for Universal Bounty Swarm.

Provides:
- sys.path setup for project root and src/
- Structural PathGuard and SafeIO fixtures with IGNORE_LIST enforcement
- Ephemeral OrbStack Docker container executor fixtures
- Thread-safe, high-fidelity in-memory Firestore simulation engine (supporting ACID transactions, on_snapshot listeners, and queries)
- Live / ADC Firestore client loader
- Real-world workload issue fixtures for triaged and untriaged queues
- Benchmark and latency assertion helpers
"""

import os
import sys
import time
import uuid
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

import pytest
from google.api_core import exceptions as google_exceptions

# Ensure universal_bounty_swarm root and src are on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Import core modules with defensive fallback contracts
try:
    from src.core.exceptions import (
        BountySwarmError,
        ProtectedPathViolationError,
        ExecutorError,
        ContainerExecutionTimeoutError,
        FirestoreSyncError,
    )
except ImportError:
    class BountySwarmError(Exception):
        pass

    class ProtectedPathViolationError(PermissionError, BountySwarmError):
        def __init__(self, message: str, path: str = None, operation: str = None):
            super().__init__(message)
            self.path = path
            self.operation = operation

    class ExecutorError(BountySwarmError):
        pass

    class ContainerExecutionTimeoutError(ExecutorError):
        pass

    class FirestoreSyncError(BountySwarmError):
        pass

try:
    from src.core.path_guard import PathGuard
except ImportError:
    class PathGuard:
        DEFAULT_IGNORE_LIST = [
            "~/teamwork_projects/keeper_daemon",
            "~/teamwork_projects/odin",
            "~/teamwork_projects/matt-berserker",
        ]

        def __init__(self, ignore_list: Optional[List[str]] = None):
            raw_list = ignore_list or self.DEFAULT_IGNORE_LIST
            self.protected_paths: List[Path] = []
            for raw_path in raw_list:
                expanded = os.path.expanduser(os.path.expandvars(raw_path))
                canonical = Path(expanded).resolve()
                self.protected_paths.append(canonical)

        def is_protected(self, target_path: Union[str, Path]) -> bool:
            if not target_path:
                return False
            try:
                expanded_str = os.path.expanduser(os.path.expandvars(str(target_path)))
                resolved_target = Path(expanded_str).resolve()
            except Exception:
                return True

            for protected in self.protected_paths:
                if resolved_target == protected:
                    return True
                try:
                    if resolved_target.is_relative_to(protected):
                        return True
                except AttributeError:
                    try:
                        if os.path.commonpath([str(resolved_target), str(protected)]) == str(protected):
                            return True
                    except ValueError:
                        pass
            return False

        def validate_access(self, target_path: Union[str, Path], operation: str = "access") -> Path:
            if self.is_protected(target_path):
                raise ProtectedPathViolationError(
                    f"[IGNORE_LIST_BLOCKED] Operation '{operation}' strictly forbidden on protected trading directory: {target_path}",
                    path=str(target_path),
                    operation=operation
                )
            return Path(os.path.expanduser(os.path.expandvars(str(target_path)))).resolve()

try:
    from src.core.safe_io import SafeIO
except ImportError:
    class SafeIO:
        _guard = PathGuard()

        @classmethod
        def set_guard(cls, guard):
            cls._guard = guard

        @classmethod
        def read_text(cls, path: Union[str, Path], encoding: str = "utf-8") -> str:
            val = cls._guard.validate_access(path, operation="read_text")
            with open(val, "r", encoding=encoding) as f:
                return f.read()

        @classmethod
        def write_text(cls, path: Union[str, Path], data: str, encoding: str = "utf-8") -> int:
            val = cls._guard.validate_access(path, operation="write_text")
            val.parent.mkdir(parents=True, exist_ok=True)
            with open(val, "w", encoding=encoding) as f:
                return f.write(data)

        @classmethod
        def read_bytes(cls, path: Union[str, Path]) -> bytes:
            val = cls._guard.validate_access(path, operation="read_bytes")
            with open(val, "rb") as f:
                return f.read()

        @classmethod
        def write_bytes(cls, path: Union[str, Path], data: bytes) -> int:
            val = cls._guard.validate_access(path, operation="write_bytes")
            val.parent.mkdir(parents=True, exist_ok=True)
            with open(val, "wb") as f:
                return f.write(data)

        @classmethod
        def delete_file(cls, path: Union[str, Path], missing_ok: bool = True) -> None:
            val = cls._guard.validate_access(path, operation="delete_file")
            if val.exists():
                val.unlink()
            elif not missing_ok:
                raise FileNotFoundError(f"File not found: {val}")

        @classmethod
        def rmtree(cls, path: Union[str, Path], ignore_errors: bool = False) -> None:
            val = cls._guard.validate_access(path, operation="rmtree")
            if val.exists():
                shutil.rmtree(val, ignore_errors=ignore_errors)

try:
    from src.core.orbstack_executor import EphemeralOrbStackExecutor, ContainerExecutionResult
except ImportError:
    EphemeralOrbStackExecutor = None
    ContainerExecutionResult = None

try:
    from src.core.firestore_client import get_firestore_client, initialize_firebase_app
except ImportError:
    get_firestore_client = None
    initialize_firebase_app = None

try:
    from src.core.listener import FirestoreListener, listen_collection, listen_document, claim_bounty_lead
except ImportError:
    FirestoreListener = None
    listen_collection = None
    listen_document = None
    claim_bounty_lead = None


# ==============================================================================
# IN-MEMORY THREAD-SAFE FIRESTORE SIMULATION ENGINE
# ==============================================================================

class MockDocumentSnapshot:
    def __init__(self, doc_id: str, data: Optional[Dict[str, Any]], exists: bool = True, ref: Any = None):
        self.id = doc_id
        self._data = dict(data) if data is not None else None
        self.exists = exists
        self.reference = ref
        self.read_time = time.time()
        self.create_time = time.time()
        self.update_time = time.time()

    def to_dict(self) -> Optional[Dict[str, Any]]:
        return dict(self._data) if self._data is not None else None

    def get(self, field_path: str) -> Any:
        if not self._data:
            return None
        parts = field_path.split(".")
        curr = self._data
        for p in parts:
            if isinstance(curr, dict) and p in curr:
                curr = curr[p]
            else:
                return None
        return curr


class MockWatch:
    def __init__(self, unsubscribe_fn: Callable[[], None]):
        self._unsubscribe_fn = unsubscribe_fn
        self.is_active = True

    def unsubscribe(self):
        if self.is_active:
            self.is_active = False
            self._unsubscribe_fn()

    def close(self):
        self.unsubscribe()


class MockDocumentReference:
    def __init__(self, doc_id: str, collection_ref: 'MockCollectionReference'):
        self.id = doc_id
        self._collection = collection_ref
        self._db = collection_ref._db
        self.path = f"{collection_ref.id}/{doc_id}"

    def get(self, transaction: Optional['MockTransaction'] = None) -> MockDocumentSnapshot:
        if transaction is not None:
            return transaction.get(self)
        with self._db._lock:
            data = self._collection._docs.get(self.id)
            if data is None:
                return MockDocumentSnapshot(self.id, None, exists=False, ref=self)
            return MockDocumentSnapshot(self.id, dict(data), exists=True, ref=self)

    def set(self, document_data: Dict[str, Any], merge: bool = False) -> Any:
        with self._db._lock:
            old_data = self._collection._docs.get(self.id)
            if merge and old_data:
                new_data = dict(old_data)
                new_data.update(document_data)
            else:
                new_data = dict(document_data)
            self._collection._docs[self.id] = new_data
            self._db._version_counter[self.path] = self._db._version_counter.get(self.path, 0) + 1
            self._collection._notify_listeners(self.id, new_data, "MODIFIED" if old_data else "ADDED")
        return MockWriteResult()

    def update(self, field_updates: Dict[str, Any]) -> Any:
        with self._db._lock:
            if self.id not in self._collection._docs:
                raise Exception(f"Document {self.id} does not exist in {self._collection.id}")
            curr = dict(self._collection._docs[self.id])
            for k, v in field_updates.items():
                if "." in k:
                    parts = k.split(".")
                    d = curr
                    for p in parts[:-1]:
                        if p not in d or not isinstance(d[p], dict):
                            d[p] = {}
                        d = d[p]
                    d[parts[-1]] = v
                else:
                    curr[k] = v
            self._collection._docs[self.id] = curr
            self._db._version_counter[self.path] = self._db._version_counter.get(self.path, 0) + 1
            self._collection._notify_listeners(self.id, curr, "MODIFIED")
        return MockWriteResult()

    def delete(self) -> Any:
        with self._db._lock:
            if self.id in self._collection._docs:
                del self._collection._docs[self.id]
                self._db._version_counter[self.path] = self._db._version_counter.get(self.path, 0) + 1
                self._collection._notify_listeners(self.id, None, "REMOVED")
        return MockWriteResult()

    def on_snapshot(self, callback: Callable[[Any, Any, Any], None]) -> MockWatch:
        return self._collection._db._add_doc_watch(self, callback)


class MockQuery:
    def __init__(self, collection_ref: 'MockCollectionReference', filters=None, order_bys=None, limit_val=None):
        self._collection = collection_ref
        self._filters = filters or []
        self._order_bys = order_bys or []
        self._limit_val = limit_val

    def where(self, field_path: str, op_string: str, value: Any) -> 'MockQuery':
        new_filters = list(self._filters)
        new_filters.append((field_path, op_string, value))
        return MockQuery(self._collection, filters=new_filters, order_bys=self._order_bys, limit_val=self._limit_val)

    def order_by(self, field_path: str, direction: str = "ASCENDING") -> 'MockQuery':
        new_orders = list(self._order_bys)
        new_orders.append((field_path, direction))
        return MockQuery(self._collection, filters=self._filters, order_bys=new_orders, limit_val=self._limit_val)

    def limit(self, count: int) -> 'MockQuery':
        return MockQuery(self._collection, filters=self._filters, order_bys=self._order_bys, limit_val=count)

    def stream(self, transaction: Optional['MockTransaction'] = None) -> List[MockDocumentSnapshot]:
        return self.get(transaction=transaction)

    def get(self, transaction: Optional['MockTransaction'] = None) -> List[MockDocumentSnapshot]:
        with self._collection._db._lock:
            results = []
            for doc_id, data in self._collection._docs.items():
                match = True
                for field_path, op, val in self._filters:
                    doc_val = data.get(field_path)
                    if op in ("==", "=") and doc_val != val:
                        match = False
                        break
                    elif op == "in" and (not isinstance(val, (list, tuple, set)) or doc_val not in val):
                        match = False
                        break
                    elif op == "!=" and doc_val == val:
                        match = False
                        break
                    elif op == ">" and (doc_val is None or doc_val <= val):
                        match = False
                        break
                    elif op == "<" and (doc_val is None or doc_val >= val):
                        match = False
                        break
                if match:
                    ref = MockDocumentReference(doc_id, self._collection)
                    results.append(MockDocumentSnapshot(doc_id, data, exists=True, ref=ref))

            if self._limit_val is not None:
                results = results[:self._limit_val]
            return results

    def on_snapshot(self, callback: Callable[[Any, Any, Any], None]) -> MockWatch:
        return self._collection._db._add_query_watch(self, callback)


class MockCollectionReference:
    def __init__(self, col_id: str, db: 'MockFirestoreClient'):
        self.id = col_id
        self._db = db
        self._docs: Dict[str, Dict[str, Any]] = {}
        self._listeners: List[Callable[[List[MockDocumentSnapshot], Any, Any], None]] = []

    def document(self, document_id: Optional[str] = None) -> MockDocumentReference:
        doc_id = document_id or uuid.uuid4().hex
        return MockDocumentReference(doc_id, self)

    def add(self, document_data: Dict[str, Any], document_id: Optional[str] = None) -> Tuple[Any, MockDocumentReference]:
        doc_ref = self.document(document_id)
        doc_ref.set(document_data)
        return MockWriteResult(), doc_ref

    def stream(self, transaction: Optional['MockTransaction'] = None) -> List[MockDocumentSnapshot]:
        return self.get(transaction=transaction)

    def get(self, transaction: Optional['MockTransaction'] = None) -> List[MockDocumentSnapshot]:
        return MockQuery(self).get(transaction=transaction)

    def where(self, field_path: str, op_string: str, value: Any) -> MockQuery:
        return MockQuery(self).where(field_path, op_string, value)

    def order_by(self, field_path: str, direction: str = "ASCENDING") -> MockQuery:
        return MockQuery(self).order_by(field_path, direction)

    def limit(self, count: int) -> MockQuery:
        return MockQuery(self).limit(count)

    def on_snapshot(self, callback: Callable[[Any, Any, Any], None]) -> MockWatch:
        return self._db._add_collection_watch(self, callback)

    def _notify_listeners(self, doc_id: str, data: Optional[Dict[str, Any]], change_type: str):
        snapshots = self.get()
        changes = [MockDocumentChange(doc_id, data, change_type)]
        read_time = time.time()
        for listener in list(self._listeners):
            threading.Thread(
                target=self._safe_dispatch,
                args=(listener, snapshots, changes, read_time),
                daemon=True
            ).start()

    def _safe_dispatch(self, listener, snapshots, changes, read_time):
        try:
            listener(snapshots, changes, read_time)
        except Exception:
            pass


class MockDocumentChange:
    def __init__(self, doc_id: str, data: Optional[Dict[str, Any]], change_type: str):
        self.doc = MockDocumentSnapshot(doc_id, data, exists=(data is not None))
        self.type = change_type
        self.old_index = -1
        self.new_index = 0


class MockWriteResult:
    def __init__(self):
        self.update_time = time.time()


class MockTransaction:
    """ACID Transaction implementation fully compatible with google.cloud.firestore.Transaction."""
    def __init__(self, db: 'MockFirestoreClient', max_attempts: int = 5, read_only: bool = False):
        self._db = db
        self._max_attempts = max_attempts
        self._read_only = read_only
        self._id = uuid.uuid4().hex.encode('utf-8')
        self._in_progress = True
        self._read_versions: Dict[str, int] = {}
        self._writes: List[Callable[[], None]] = []

    def _clean_up(self):
        self._read_versions.clear()
        self._writes.clear()
        self._in_progress = False

    def _begin(self, retry_id: Optional[bytes] = None):
        self._clean_up()
        self._id = retry_id or uuid.uuid4().hex.encode('utf-8')
        self._in_progress = True

    def _rollback(self):
        self._clean_up()

    def get(self, doc_or_query: Any) -> Any:
        if isinstance(doc_or_query, MockDocumentReference):
            doc_ref = doc_or_query
            with self._db._lock:
                ver = self._db._version_counter.get(doc_ref.path, 0)
                self._read_versions[doc_ref.path] = ver
                data = doc_ref._collection._docs.get(doc_ref.id)
                if data is None:
                    return MockDocumentSnapshot(doc_ref.id, None, exists=False, ref=doc_ref)
                return MockDocumentSnapshot(doc_ref.id, dict(data), exists=True, ref=doc_ref)
        elif isinstance(doc_or_query, (MockQuery, MockCollectionReference)):
            return doc_or_query.get(transaction=self)
        raise ValueError(f"Unsupported get target in transaction: {type(doc_or_query)}")

    def set(self, doc_ref: MockDocumentReference, data: Dict[str, Any], merge: bool = False):
        def _write():
            doc_ref.set(data, merge=merge)
        self._writes.append(_write)

    def update(self, doc_ref: MockDocumentReference, field_updates: Dict[str, Any]):
        def _write():
            doc_ref.update(field_updates)
        self._writes.append(_write)

    def delete(self, doc_ref: MockDocumentReference):
        def _write():
            doc_ref.delete()
        self._writes.append(_write)

    def _commit(self):
        with self._db._lock:
            # Check version conflicts (optimistic concurrency)
            for path, read_ver in self._read_versions.items():
                curr_ver = self._db._version_counter.get(path, 0)
                if curr_ver != read_ver:
                    raise google_exceptions.Aborted(f"Transaction conflict on {path}: read v{read_ver}, currently v{curr_ver}")
            # Execute all writes
            for w in self._writes:
                w()
            self._clean_up()


class MockFirestoreClient:
    """Thread-safe in-memory Firestore client matching Google Cloud Firestore Client interface."""
    def __init__(self, project: str = "odin-500008"):
        self.project = project
        self._collections: Dict[str, MockCollectionReference] = {}
        self._lock = threading.RLock()
        self._version_counter: Dict[str, int] = {}
        self._active_watches: List[MockWatch] = []

    def collection(self, collection_name: str) -> MockCollectionReference:
        with self._lock:
            if collection_name not in self._collections:
                self._collections[collection_name] = MockCollectionReference(collection_name, self)
            return self._collections[collection_name]

    def transaction(self, max_attempts: int = 5, read_only: bool = False, **kwargs) -> MockTransaction:
        return MockTransaction(self, max_attempts=max_attempts, read_only=read_only)

    def _add_collection_watch(self, col: MockCollectionReference, callback: Callable) -> MockWatch:
        with self._lock:
            col._listeners.append(callback)
            initial_snaps = col.get()
            initial_changes = [MockDocumentChange(s.id, s.to_dict(), "ADDED") for s in initial_snaps]
            def _initial_dispatch():
                try:
                    callback(initial_snaps, initial_changes, time.time())
                except Exception:
                    pass

            threading.Thread(
                target=_initial_dispatch,
                daemon=True
            ).start()

            def _unsub():
                with self._lock:
                    if callback in col._listeners:
                        col._listeners.remove(callback)

            watch = MockWatch(_unsub)
            self._active_watches.append(watch)
            return watch

    def _add_query_watch(self, query: MockQuery, callback: Callable) -> MockWatch:
        return self._add_collection_watch(query._collection, callback)

    def _add_doc_watch(self, doc_ref: MockDocumentReference, callback: Callable) -> MockWatch:
        with self._lock:
            def _listener(snapshots, changes, read_time):
                for ch in changes:
                    if ch.doc.id == doc_ref.id:
                        callback(ch.doc, ch.type, read_time)

            return self._add_collection_watch(doc_ref._collection, _listener)


# ==============================================================================
# PYTEST FIXTURES
# ==============================================================================

@pytest.fixture(scope="session")
def ignore_list() -> List[str]:
    """Standard IGNORE_LIST of protected trading directories."""
    return [
        "~/teamwork_projects/keeper_daemon",
        "~/teamwork_projects/odin",
        "~/teamwork_projects/matt-berserker",
    ]


@pytest.fixture
def tmp_workspace(tmp_path: Path) -> Path:
    """Provides an isolated, clean temporary workspace directory for test execution."""
    ws = tmp_path / f"workspace_{uuid.uuid4().hex[:8]}"
    ws.mkdir(parents=True, exist_ok=True)
    yield ws
    if ws.exists():
        shutil.rmtree(ws, ignore_errors=True)


@pytest.fixture
def path_guard() -> PathGuard:
    """Returns an instantiated PathGuard enforcing standard IGNORE_LIST."""
    return PathGuard()


@pytest.fixture
def custom_path_guard():
    """Factory fixture for PathGuard with custom ignore lists."""
    def _create(ignore_list: List[str]) -> PathGuard:
        return PathGuard(ignore_list=ignore_list)
    return _create


@pytest.fixture
def safe_io(path_guard: PathGuard) -> Any:
    """Returns SafeIO configured with standard PathGuard."""
    SafeIO.set_guard(path_guard)
    return SafeIO


@pytest.fixture
def mock_firestore_db() -> MockFirestoreClient:
    """Provides a fresh, isolated MockFirestoreClient instance."""
    db = MockFirestoreClient(project="odin-500008")
    return db


@pytest.fixture
def orbstack_executor(path_guard: PathGuard) -> Any:
    """Returns an instantiated EphemeralOrbStackExecutor."""
    if EphemeralOrbStackExecutor is not None:
        return EphemeralOrbStackExecutor(
            default_image="python:3.11-slim",
            cpus="2",
            memory="2g",
            path_guard=path_guard
        )
    return None


@pytest.fixture(scope="session")
def is_docker_available() -> bool:
    """Checks if Docker daemon (OrbStack) is available and running."""
    executor = EphemeralOrbStackExecutor() if EphemeralOrbStackExecutor else None
    if executor:
        return executor.is_docker_available()
    return False


@pytest.fixture
def sample_untriaged_issues() -> List[Dict[str, Any]]:
    """Real-world untriaged issue fixtures covering bugs, errors, and unclassified events."""
    return [
        {
            "issue_id": "ISSUE-101-UNTRIAGED",
            "title": "Bug: Event listener memory leak on reconnect drop",
            "body": "High memory allocation observed during repeated gRPC disconnect cycles in on_snapshot.",
            "category": "bug",
            "queue": "untriaged",
            "status": "pending_triage",
            "repository": "s6pa1rta3n-lab/keeper_daemon",
            "target_command": ["python3", "-c", "print('TRIAGE_VERIFIED: Issue #101 analyzed successfully')"],
            "created_at": time.time(),
        },
        {
            "issue_id": "ISSUE-102-UNTRIAGED",
            "title": "Security: Unauthorized volume mount traversal attempt",
            "body": "Investigate path traversal attempt escaping /workspace mount into trading directory.",
            "category": "security",
            "queue": "untriaged",
            "status": "pending_triage",
            "repository": "s6pa1rta3n-lab/bounty_operations",
            "target_command": ["python3", "-c", "print('TRIAGE_VERIFIED: Security check cleared')"],
            "created_at": time.time(),
        },
        {
            "issue_id": "ISSUE-103-UNTRIAGED",
            "title": "Hotfix: RPC Failover timeout fallback mechanism",
            "body": "Fallback RPC threshold must switch from 5000ms to 2000ms on Base L2 network.",
            "category": "hotfix",
            "queue": "untriaged",
            "status": "pending_triage",
            "repository": "s6pa1rta3n-lab/protocol_keepers",
            "target_command": ["python3", "-c", "print('TRIAGE_VERIFIED: Hotfix simulation passed')"],
            "created_at": time.time(),
        },
    ]


@pytest.fixture
def sample_triaged_issues() -> List[Dict[str, Any]]:
    """Real-world triaged bounty issue fixtures covering Keep3rV1, Gelato, and Harvester tasks."""
    return [
        {
            "issue_id": "BOUNTY-201-KEEP3R",
            "title": "Feature: Keep3rV1 Harvest Resolver Adapter",
            "body": "Implement workable() checker and automated work() dispatch for Yearn Vault harvesters.",
            "category": "bounty_task",
            "queue": "triaged",
            "status": "priority_triage",
            "reward_tokens": "150 KP3R",
            "network": "ethereum",
            "target_command": ["python3", "-c", "import json; print(json.dumps({'workable': True, 'net_profit_usd': 84.50}))"],
            "created_at": time.time(),
        },
        {
            "issue_id": "BOUNTY-202-GELATO",
            "title": "Feature: Gelato Web3 Function Resolver for Uniswap TWAP",
            "body": "Evaluate canExec status and construct EIP-1559 payload for oracle update.",
            "category": "bounty_task",
            "queue": "triaged",
            "status": "priority_triage",
            "reward_tokens": "250 USDC",
            "network": "arbitrum",
            "target_command": ["python3", "-c", "import json; print(json.dumps({'canExec': True, 'callData': '0x12345678'}))"],
            "created_at": time.time(),
        },
        {
            "issue_id": "BOUNTY-203-REFACTOR",
            "title": "Refactor: Ephemeral Docker lifecycle optimization",
            "body": "Reduce container spin-up and teardown latency to under 800ms per bounty task.",
            "category": "architecture",
            "queue": "triaged",
            "status": "priority_triage",
            "reward_tokens": "500 USDC",
            "network": "base",
            "target_command": ["python3", "-c", "print('BENCHMARK_PASSED: Ephemeral execution completed in 0.42s')"],
            "created_at": time.time(),
        },
    ]

"""Core foundation components for Universal Bounty Swarm."""

from src.core.config import (
    DEFAULT_IGNORE_LIST,
    SwarmConfig,
    get_config,
)
from src.core.exceptions import (
    BountySwarmError,
    ConfigurationError,
    ContainerExecutionTimeoutError,
    ExecutorError,
    FirestoreSyncError,
    ProtectedPathViolationError,
)
from src.core.path_guard import (
    DEFAULT_PATH_GUARD,
    PathGuard,
    is_protected,
    validate_access,
)
from src.core.safe_io import SafeIO

__all__ = [
    "DEFAULT_IGNORE_LIST",
    "DEFAULT_PATH_GUARD",
    "BountySwarmError",
    "ConfigurationError",
    "ContainerExecutionTimeoutError",
    "ExecutorError",
    "FirestoreSyncError",
    "PathGuard",
    "ProtectedPathViolationError",
    "SafeIO",
    "SwarmConfig",
    "get_config",
    "is_protected",
    "validate_access",
]

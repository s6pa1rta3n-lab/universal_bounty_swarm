"""
Core Exceptions for Universal Bounty Swarm.
Defines system-wide exception hierarchy and specialized security errors.
"""


class BountySwarmError(Exception):
    """Base exception for all Universal Bounty Swarm errors."""

    pass


class ProtectedPathViolationError(PermissionError, BountySwarmError):
    """
    Raised when any filesystem, traversal, or container mount operation
    attempts to access a path contained within the IGNORE_LIST.
    Inherits from PermissionError to satisfy system-level security constraints.
    """

    def __init__(self, message: str, path: str = None, operation: str = None):
        super().__init__(message)
        self.path = path
        self.operation = operation


class ConfigurationError(BountySwarmError):
    """Raised when configuration values are missing, invalid, or conflicting."""

    pass


class ExecutorError(BountySwarmError):
    """Raised when isolated container execution fails or encounters runtime errors."""

    pass


class ContainerExecutionTimeoutError(ExecutorError):
    """Raised when an ephemeral container execution exceeds its allotted timeout."""

    pass


class FirestoreSyncError(BountySwarmError):
    """Raised when Firestore synchronization or document transaction fails."""

    pass

class FailedToLockError(Exception):
    """Raised when acquiring a lock fails (e.g. timeout elapsed)."""


class ReleaseError(Exception):
    """Raised when releasing a lock that the current session does not hold."""


class UnreachableError(Exception):
    """Raised when an internal invariant is violated."""

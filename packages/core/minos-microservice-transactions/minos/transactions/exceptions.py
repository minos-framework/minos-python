from minos.common import (
    MinosException,
)


class TransactionRepositoryException(MinosException):
    """Base transaction repository exception."""


class TransactionRepositoryConflictException(TransactionRepositoryException):
    """Exception to be raised when a transaction has invalid status."""


class TransactionNotFoundException(TransactionRepositoryException):
    """Exception to be raised when some transaction is not found on the repository."""

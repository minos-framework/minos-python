from ...exceptions import (
    MinosException,
)


class DatabaseClientException(MinosException):
    """Base exception for database client."""


class UnableToConnectException(DatabaseClientException):
    """Exception to be raised when database client is not able to connect to the database."""


class IntegrityException(DatabaseClientException):
    """Exception to be raised when an integrity check is not satisfied."""

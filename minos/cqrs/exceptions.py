"""minos.cqrs.exceptions module."""

from minos.common import (
    MinosException,
)


class MinosCqrsException(MinosException):
    """Base CQRS exception."""


class MinosQueryServiceException(MinosCqrsException):
    """Exception to be raised by query service when an internal error is raised."""


class MinosIllegalHandlingException(MinosException):
    """Exception to be raised when a service is trying to be used to handle improper message types."""


class MinosNotAnyMissingReferenceException(MinosQueryServiceException):
    """Exception to be raised when an aggregate diff does not have any missing reference."""

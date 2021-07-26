"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from minos.common import (
    MinosException,
)


class MinosCqrsException(MinosException):
    """Base CQRS exception."""


class MinosQueryServiceException(MinosCqrsException):
    """Exception to be raised by query service when an internal error is raised."""


class MinosIllegalHandlingException(MinosException):
    """Exception to be raised when a service is trying to be used to handle improper message types."""

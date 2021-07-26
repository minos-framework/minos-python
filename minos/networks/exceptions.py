"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from minos.common import (
    MinosException,
)


class MinosNetworkException(MinosException):
    """Base network exception."""


class MinosDiscoveryConnectorException(MinosNetworkException):
    """Exception to be raised when there is a failure while communicating with the discovery."""


class MinosHandlerException(MinosNetworkException):
    """Base handler exception."""


class MinosActionNotFoundException(MinosHandlerException):
    """Exception to be raised when an action cannot be found,"""


class MinosHandlerNotFoundEnoughEntriesException(MinosHandlerException):
    """Exception to be raised when not enough entries have been found by a handler."""


class MinosEnrouteDecoratorException(MinosNetworkException):
    """Base exception for enroute decorators."""


class MinosMultipleEnrouteDecoratorKindsException(MinosEnrouteDecoratorException):
    """Exception to be raised when multiple enroute decorator kinds are applied to the same function."""


class MinosRedefinedEnrouteDecoratorException(MinosEnrouteDecoratorException):
    """Exception to be raised when same enroute decorator is used by multiple handlers."""

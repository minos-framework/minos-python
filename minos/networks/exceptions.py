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


class MinosActionNotFoundException(MinosNetworkException):
    """Exception to be raised when an action cannot be found,"""

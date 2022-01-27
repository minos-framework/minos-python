from minos.common import (
    MinosException,
)


class MinosNetworkException(MinosException):
    """Base network exception."""


class MinosDiscoveryConnectorException(MinosNetworkException):
    """Exception to be raised when there is a failure while communicating with the discovery."""


class MinosInvalidDiscoveryClient(MinosNetworkException):
    """Exception raised when the configured Discovery Client does not implement de DiscoveryClient interface"""


class MinosHandlerException(MinosNetworkException):
    """Base handler exception."""


class MinosActionNotFoundException(MinosHandlerException):
    """Exception to be raised when an action cannot be found,"""


class MinosHandlerNotFoundEnoughEntriesException(MinosHandlerException):
    """Exception to be raised when not enough entries have been found by a handler."""


class NotSatisfiedCheckerException(MinosHandlerException):
    """Exception to be raised when some checkers are not validated."""


class MinosEnrouteDecoratorException(MinosNetworkException):
    """Base exception for enroute decorators."""


class MinosMultipleEnrouteDecoratorKindsException(MinosEnrouteDecoratorException):
    """Exception to be raised when multiple enroute decorator kinds are applied to the same function."""


class MinosRedefinedEnrouteDecoratorException(MinosEnrouteDecoratorException):
    """Exception to be raised when same enroute decorator is used by multiple actions."""


class RequestException(MinosNetworkException):
    """Base exception for requests."""


class NotHasContentException(RequestException):
    """Exception to be raised when request has not content."""


class NotHasParamsException(RequestException):
    """Exception to be raised when request has not params."""

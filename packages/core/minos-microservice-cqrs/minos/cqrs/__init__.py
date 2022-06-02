"""The CQRS pattern of the Minos Framework."""

__author__ = "Minos Framework Devs"
__email__ = "hey@minos.run"
__version__ = "0.8.0.dev2"

from .exceptions import (
    MinosCqrsException,
    MinosIllegalHandlingException,
    MinosNotAnyMissingReferenceException,
    MinosQueryServiceException,
)
from .handlers import (
    PreEventHandler,
)
from .services import (
    CommandService,
    QueryService,
    Service,
)

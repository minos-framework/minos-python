from abc import (
    ABC,
)
from functools import (
    partial,
)
from typing import (
    TypeVar,
)

from minos.aggregate import (
    Aggregate,
)
from minos.common import (
    Inject,
)
from minos.networks import (
    Request,
    WrappedRequest,
)

from .exceptions import (
    MinosIllegalHandlingException,
)
from .handlers import (
    PreEventHandler,
)

GenericAggregate = TypeVar("GenericAggregate", bound=Aggregate)


class Service(ABC):
    """Base Service class"""

    aggregate: GenericAggregate

    @Inject()
    def __init__(self, aggregate: Aggregate, **kwargs):
        self.aggregate = aggregate

    @staticmethod
    def _pre_event_handle(request: Request) -> Request:
        fn = partial(PreEventHandler.handle, user=request.user)
        return WrappedRequest(request, fn)


class CommandService(Service, ABC):
    """Command Service class"""

    @staticmethod
    def _pre_command_handle(request: Request) -> Request:
        return request

    @staticmethod
    def _pre_query_handle(request: Request) -> Request:
        raise MinosIllegalHandlingException("Queries cannot be handled by `CommandService` inherited classes.")


class QueryService(Service, ABC):
    """Query Service class"""

    @staticmethod
    def _pre_command_handle(request: Request) -> Request:
        raise MinosIllegalHandlingException("Commands cannot be handled by `QueryService` inherited classes.")

    @staticmethod
    def _pre_query_handle(request: Request) -> Request:
        return request

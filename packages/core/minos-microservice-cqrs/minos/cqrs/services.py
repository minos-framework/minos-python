from abc import (
    ABC,
)
from contextlib import (
    suppress,
)
from functools import (
    partial,
)
from inspect import (
    getmembers,
    isfunction,
    ismethod,
)
from typing import (
    Any,
)

from minos.common import (
    Config,
    Inject,
    NotProvidedException,
)
from minos.networks import (
    EnrouteDecorator,
    HandlerWrapper,
    Request,
    WrappedRequest,
)

from .exceptions import (
    MinosIllegalHandlingException,
)
from .handlers import (
    PreEventHandler,
)


class Service(ABC):
    """Base Service class"""

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __getattr__(self, item: str) -> Any:
        if item != "_kwargs" and item in self._kwargs:
            return self._kwargs[item]

        with suppress(NotProvidedException):
            return Inject.resolve_by_name(item)

        raise AttributeError(f"{type(self).__name__!r} does not contain the {item!r} field.")

    @classmethod
    def __get_enroute__(cls, config: Config) -> dict[str, set[EnrouteDecorator]]:
        result = dict()
        for name, fn in getmembers(cls, predicate=lambda x: ismethod(x) or isfunction(x)):
            if not isinstance(fn, HandlerWrapper):
                continue
            result[name] = fn.meta.decorators
        return result

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

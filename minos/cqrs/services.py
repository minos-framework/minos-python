from abc import (
    ABC,
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

from dependency_injector.containers import (
    Container,
)
from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    EnrouteDecorator,
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

    @inject
    def __init__(self, container: Container = Provide["<container>"], **kwargs):
        self._container = container
        self._kwargs = kwargs

    def __getattr__(self, item: str) -> Any:
        if item != "_kwargs" and item in self._kwargs:
            return self._kwargs[item]

        if item != "_container" and isinstance(self._container, Container) and item in self._container.providers:
            return self._container.providers[item]()

        raise AttributeError(f"{type(self).__name__!r} does not contain the {item!r} field.")

    @classmethod
    def __get_enroute__(cls, config: MinosConfig) -> dict[str, set[EnrouteDecorator]]:
        result = dict()
        for name, fn in getmembers(cls, predicate=lambda x: ismethod(x) or isfunction(x)):
            if not hasattr(fn, "__decorators__"):
                continue
            result[name] = fn.__decorators__
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

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

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    CommandBroker,
    DynamicHandlerPool,
    EnrouteDecorator,
    Request,
    WrappedRequest,
)
from minos.saga import (
    SagaManager,
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
    def __init__(
        self,
        *args,
        config: MinosConfig = Provide["config"],
        saga_manager: SagaManager = Provide["saga_manager"],
        dynamic_handler_pool: DynamicHandlerPool = Provide["dynamic_handler_pool"],
        command_broker: CommandBroker = Provide["command_broker"],
        **kwargs,
    ):
        self.config = config
        self.saga_manager = saga_manager

        self.dynamic_handler_pool = dynamic_handler_pool
        self.command_broker = command_broker

    @classmethod
    def __get_enroute__(cls, config: MinosConfig) -> dict[str, set[EnrouteDecorator]]:
        result = dict()
        for name, fn in getmembers(cls, predicate=lambda x: ismethod(x) or isfunction(x)):
            if not hasattr(fn, "__decorators__"):
                continue
            result[name] = fn.__decorators__
        return result


class CommandService(Service, ABC):
    """Command Service class"""

    @staticmethod
    def _pre_command_handle(request: Request) -> Request:
        return request

    @staticmethod
    def _pre_query_handle(request: Request) -> Request:
        raise MinosIllegalHandlingException("Queries cannot be handled by `CommandService` inherited classes.")

    def _pre_event_handle(self, request: Request) -> Request:
        fn = partial(
            PreEventHandler.handle,
            dynamic_handler_pool=self.dynamic_handler_pool,
            command_broker=self.command_broker,
            user=request.user,
        )
        return WrappedRequest(request, fn)


class QueryService(Service, ABC):
    """Query Service class"""

    @staticmethod
    def _pre_command_handle(request: Request) -> Request:
        raise MinosIllegalHandlingException("Commands cannot be handled by `QueryService` inherited classes.")

    @staticmethod
    def _pre_query_handle(request: Request) -> Request:
        return request

    def _pre_event_handle(self, request: Request) -> Request:
        fn = partial(
            PreEventHandler.handle,
            dynamic_handler_pool=self.dynamic_handler_pool,
            command_broker=self.command_broker,
            user=request.user,
        )
        return WrappedRequest(request, fn)

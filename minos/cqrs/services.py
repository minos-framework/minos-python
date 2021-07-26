"""
Copyright (C) 2021 Clariteia SL
This file is part of minos framework.
Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    ABC,
)
from functools import (
    partial,
)
from typing import (
    Optional,
)

from dependency_injector.wiring import (
    Provide,
)

from minos.common import (
    MinosConfig,
    MinosSagaManager,
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


class Service(ABC):
    """Base Service class"""

    config: MinosConfig = Provide["config"]
    saga_manager: MinosSagaManager = Provide["saga_manager"]

    def __init__(
        self, *args, config: Optional[MinosConfig] = None, saga_manager: Optional[MinosSagaManager] = None, **kwargs,
    ):
        if config is not None:
            self.config = config
        if saga_manager is not None:
            self.saga_manager = saga_manager


class CommandService(Service, ABC):
    """Command Service class"""

    def _pre_query_handle(self, request: Request) -> Request:
        raise MinosIllegalHandlingException("Queries cannot be handled by `CommandService` inherited classes.")

    def _pre_event_handle(self, request: Request) -> Request:
        raise MinosIllegalHandlingException("Events cannot be handled by `CommandService` inherited classes.")


class QueryService(Service, ABC):
    """Query Service class"""

    def _pre_command_handle(self, request: Request) -> Request:
        raise MinosIllegalHandlingException("Commands cannot be handled by `QueryService` inherited classes.")

    def _pre_event_handle(self, request: Request) -> Request:
        fn = partial(PreEventHandler.handle, saga_manager=self.saga_manager)
        return WrappedRequest(request, fn)

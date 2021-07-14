"""
Copyright (C) 2021 Clariteia SL
This file is part of minos framework.
Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    ABC,
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


class CommandService(Service):
    """Command Service class"""


class QueryService(Service):
    """Query Service class"""

"""
Copyright (C) 2021 Clariteia SL
This file is part of minos framework.
Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    ABC,
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


class CommandService(Service):
    """Command Service class"""


class QueryService(Service):
    """Query Service class"""

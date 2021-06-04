"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from dependency_injector.wiring import (
    Provide,
)

from .configuration import (
    MinosConfig,
)
from .saga import (
    MinosSagaManager,
)


class Service:
    """Service class"""

    config: MinosConfig = Provide["config"]
    saga_manager: MinosSagaManager = Provide["saga_manager"]

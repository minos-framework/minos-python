"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from __future__ import (
    annotations,
)

from typing import (
    Any,
    Optional,
)

from cached_property import (
    cached_property,
)
from dependency_injector import (
    containers,
    providers,
)

from .configuration import (
    MinosConfig,
)
from .networks import (
    MinosBroker,
)
from .repository import (
    MinosRepository,
)
from .saga import (
    MinosSagaManager,
)


class MinosDependencyContainer(object):
    """TODO"""

    def __init__(
        self,
        modules: list[Any],
        config: MinosConfig,
        repository_cls: Optional[type[MinosRepository]] = None,
        event_broker_cls: Optional[type[MinosBroker]] = None,
        command_broker_cls: Optional[type[MinosBroker]] = None,
        command_reply_broker_cls: Optional[type[MinosBroker]] = None,
        saga_manager_cls: Optional[type[MinosSagaManager]] = None,
    ):
        """TODO
        :param modules: TODO
        :param config: TODO
        :param repository_cls: TODO
        :param event_broker_cls: TODO
        :param command_broker_cls: TODO
        :param command_reply_broker_cls: TODO
        :param saga_manager_cls: TODO
        :return: TODO
        """
        self.modules = modules
        self.config = config
        self.repository_cls = repository_cls
        self.event_broker_cls = event_broker_cls
        self.command_broker_cls = command_broker_cls
        self.command_reply_broker_cls = command_reply_broker_cls
        self.saga_manager_cls = saga_manager_cls

    @cached_property
    def container(self):
        """TODO
        :return: TODO
        """
        container = containers.DynamicContainer()

        container.config = providers.Object(self.config)

        if self.repository_cls is not None:
            container.repository = providers.Singleton(self.repository_cls.from_config, config=self.config)
        if self.event_broker_cls is not None:
            container.event_broker = providers.Singleton(self.event_broker_cls)
        if self.command_broker_cls is not None:
            container.command_broker = providers.Singleton(self.command_broker_cls)
        if self.command_reply_broker_cls is not None:
            container.command_reply_broker = providers.Singleton(self.command_reply_broker_cls)
        if self.saga_manager_cls is not None:
            container.saga_manager = providers.Singleton(self.saga_manager_cls.from_config, config=self.config)

        return container

    async def wire(self):
        """TODO

        :return: TODO
        """
        self.container.wire(modules=self.modules)
        if self.repository_cls is not None:
            await self.container.repository().setup()

    async def unwire(self) -> None:
        """TODO

        :return: TODO
        """
        self.container.unwire()
        if self.repository_cls is not None:
            await self.container.repository().destroy()

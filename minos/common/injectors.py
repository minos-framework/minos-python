"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from __future__ import (
    annotations,
)

from typing import (
    NoReturn,
    Optional,
    Type,
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


class MinosDependencyInjector(object):
    """Wrapper class for ``dependency_injector.containers.Container``. """

    def __init__(
        self,
        config: MinosConfig,
        repository_cls: Optional[Type[MinosRepository]] = None,
        event_broker_cls: Optional[Type[MinosBroker]] = None,
        command_broker_cls: Optional[Type[MinosBroker]] = None,
        command_reply_broker_cls: Optional[Type[MinosBroker]] = None,
        saga_manager_cls: Optional[Type[MinosSagaManager]] = None,
    ):
        self.config = config
        self.repository_cls = repository_cls
        self.event_broker_cls = event_broker_cls
        self.command_broker_cls = command_broker_cls
        self.command_reply_broker_cls = command_reply_broker_cls
        self.saga_manager_cls = saga_manager_cls

    @cached_property
    def container(self) -> containers.Container:
        """Get the dependencies container.

        :return: A ``Container`` instance.
        """
        container = containers.DynamicContainer()
        container.config = providers.Object(self.config)

        if self.repository_cls is not None:
            container.repository = providers.Object(self.repository_cls.from_config(config=self.config))
        if self.event_broker_cls is not None:
            container.event_broker = providers.Object(self.event_broker_cls.from_config(config=self.config))
        if self.command_broker_cls is not None:
            container.command_broker = providers.Object(self.command_broker_cls.from_config(config=self.config))
        if self.command_reply_broker_cls is not None:
            container.command_reply_broker = providers.Object(
                self.command_reply_broker_cls.from_config(config=self.config)
            )
        if self.saga_manager_cls is not None:
            container.saga_manager = providers.Object(self.saga_manager_cls.from_config(config=self.config))

        return container

    async def wire(self, *args, **kwargs) -> NoReturn:
        """Connect the configuration.

        :return: This method does not return anything.
        """
        self.container.wire(*args, **kwargs)

        if self.repository_cls is not None:
            await self.container.repository().setup()

        if self.event_broker_cls is not None:
            await self.container.event_broker().setup()

        if self.command_broker_cls is not None:
            await self.container.command_broker().setup()

        if self.command_reply_broker_cls is not None:
            await self.container.command_reply_broker().setup()

        if self.saga_manager_cls is not None:
            await self.container.saga_manager().setup()

    async def unwire(self) -> NoReturn:
        """Disconnect the configuration.

        :return: This method does not return anything.
        """
        self.container.unwire()

        if self.repository_cls is not None:
            await self.container.repository().destroy()

        if self.repository_cls is not None:
            await self.container.repository().destroy()

        if self.event_broker_cls is not None:
            await self.container.event_broker().destroy()

        if self.command_broker_cls is not None:
            await self.container.command_broker().destroy()

        if self.command_reply_broker_cls is not None:
            await self.container.command_reply_broker().destroy()

        if self.saga_manager_cls is not None:
            await self.container.saga_manager().destroy()

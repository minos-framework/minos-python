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


class DependencyInjector:
    """Async wrapper of ``dependency_injector.containers.Container``. """

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

    async def wire(self, *args, **kwargs) -> NoReturn:
        """Connect the configuration.

        :return: This method does not return anything.
        """
        self.container.wire(*args, **kwargs)

        if self.repository is not None:
            await self.repository.setup()

        if self.event_broker is not None:
            await self.event_broker.setup()

        if self.command_broker is not None:
            await self.command_broker.setup()

        if self.command_reply_broker is not None:
            await self.command_reply_broker.setup()

        if self.saga_manager is not None:
            await self.saga_manager.setup()

    async def unwire(self) -> NoReturn:
        """Disconnect the configuration.

        :return: This method does not return anything.
        """
        self.container.unwire()

        if self.repository is not None:
            await self.repository.destroy()

        if self.event_broker is not None:
            await self.event_broker.destroy()

        if self.command_broker is not None:
            await self.command_broker.destroy()

        if self.command_reply_broker is not None:
            await self.command_reply_broker.destroy()

        if self.saga_manager_cls is not None:
            await self.saga_manager.destroy()

    @cached_property
    def container(self) -> containers.Container:
        """Get the dependencies container.

        :return: A ``Container`` instance.
        """
        container = containers.DynamicContainer()
        container.config = providers.Object(self.config)
        container.repository = providers.Object(self.repository)
        container.event_broker = providers.Object(self.event_broker)
        container.command_broker = providers.Object(self.command_broker)
        container.command_reply_broker = providers.Object(self.command_reply_broker)
        container.saga_manager = providers.Object(self.saga_manager)
        return container

    @cached_property
    def repository(self) -> Optional[MinosRepository]:
        """Get the Repository instance to be injected.

        :return: A `` MinosRepository`` instance or ``None``.
        """
        if self.repository_cls is None:
            return None
        return self.repository_cls.from_config(config=self.config)

    @cached_property
    def event_broker(self) -> Optional[MinosBroker]:
        """Get the Event Broker instance to be injected.

        :return: A `` MinosBroker`` instance or ``None``.
        """
        if self.event_broker_cls is None:
            return None
        return self.event_broker_cls.from_config(config=self.config)

    @cached_property
    def command_broker(self) -> Optional[MinosBroker]:
        """Get the Command Broker instance to be injected.

        :return: A `` MinosBroker`` instance or ``None``.
        """
        if self.command_broker_cls is None:
            return None
        return self.command_broker_cls.from_config(config=self.config)

    @cached_property
    def command_reply_broker(self) -> Optional[MinosBroker]:
        """Get the Command Reply Broker instance to be injected.

        :return: A `` MinosBroker`` instance or ``None``.
        """
        if self.command_reply_broker_cls is None:
            return None
        return self.command_reply_broker_cls.from_config(config=self.config)

    @cached_property
    def saga_manager(self) -> Optional[MinosSagaManager]:
        """Get the Saga Manager instance to be injected.

        :return: A `` MinosSagaManager`` instance or ``None``.
        """
        if self.saga_manager_cls is None:
            return None
        return self.saga_manager_cls.from_config(config=self.config)

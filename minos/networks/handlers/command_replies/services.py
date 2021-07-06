"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from typing import (
    Any,
)

from aiomisc.service.periodic import (
    PeriodicService,
    Service,
)
from cached_property import (
    cached_property,
)

from .consumers import (
    CommandReplyConsumer,
)
from .handlers import (
    CommandReplyHandler,
)


class CommandReplyConsumerService(Service):
    """Minos QueueDispatcherService class."""

    async def start(self) -> None:
        """Method to be called at the startup by the internal ``aiomisc`` loigc.

        :return: This method does not return anything.
        """
        await self.dispatcher.setup()
        await self.dispatcher.dispatch()

    async def stop(self, exception: Exception = None) -> Any:
        """Stop the service execution.

        :param exception: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await self.dispatcher.destroy()

    @cached_property
    def dispatcher(self) -> CommandReplyConsumer:
        """Get the service dispatcher.

        :return: A ``CommandReplyConsumer`` instance.
        """
        return CommandReplyConsumer.from_config()


class CommandReplyHandlerService(PeriodicService):
    """Minos QueueDispatcherService class."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_kwargs = kwargs

    async def start(self) -> None:
        """Method to be called at the startup by the internal ``aiomisc`` loigc.

        :return: This method does not return anything.
        """
        await self.dispatcher.setup()
        await super().start()

    async def callback(self) -> None:
        """Method to be called periodically by the internal ``aiomisc`` logic.

        :return:This method does not return anything.
        """
        await self.dispatcher.dispatch()

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await super().stop(err)
        await self.dispatcher.destroy()

    @cached_property
    def dispatcher(self) -> CommandReplyHandler:
        """Get the service dispatcher.

        :return: A ``CommandReplyHandler`` instance.
        """
        return CommandReplyHandler.from_config(**self._init_kwargs)

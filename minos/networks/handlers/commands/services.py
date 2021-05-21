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

from minos.common import (
    MinosConfig,
)

from .consumers import (
    CommandConsumer,
)
from .handlers import (
    CommandHandler,
)


class CommandConsumerService(Service):
    """Minos QueueDispatcherService class."""

    def __init__(self, config: MinosConfig = None, **kwargs):
        super().__init__(**kwargs)
        self.dispatcher = CommandConsumer.from_config(config=config)

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


class CommandHandlerService(PeriodicService):
    """Minos QueueDispatcherService class."""

    def __init__(self, config: MinosConfig = None, **kwargs):
        super().__init__(**kwargs)
        self.dispatcher = CommandHandler.from_config(config=config)

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

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

from .consumer import (
    CommandReplyConsumer,
)
from .dispatcher import (
    CommandReplyHandler,
)


class CommandReplyConsumerService(Service):
    """Minos QueueDispatcherService class."""

    def __init__(self, config: MinosConfig = None, **kwargs):
        super().__init__(**kwargs)
        self.dispatcher = CommandReplyConsumer.from_config(config=config)
        self.consumer = None

    async def start(self) -> None:
        """Method to be called at the startup by the internal ``aiomisc`` loigc.

        :return: This method does not return anything.
        """
        await self.dispatcher.setup()

        self.consumer = await self.dispatcher.kafka_consumer(
            self.dispatcher._topics, self.dispatcher._broker_group_name, self.dispatcher._kafka_conn_data
        )
        await self.dispatcher.handle_message(self.consumer)

    async def stop(self, exception: Exception = None) -> Any:
        """Stop the service execution.

        :param exception: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        if self.consumer is not None:
            await self.consumer.stop()

        await self.dispatcher.destroy()


class CommandReplyHandlerService(PeriodicService):
    """Minos QueueDispatcherService class."""

    def __init__(self, config: MinosConfig = None, **kwargs):
        super().__init__(**kwargs)
        self.dispatcher = CommandReplyHandler.from_config(config=config)

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
        await self.dispatcher.queue_checker()

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await super().stop(err)
        await self.dispatcher.destroy()

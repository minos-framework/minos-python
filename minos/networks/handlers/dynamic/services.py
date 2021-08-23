"""minos.networks.handlers.dynamic.services module."""

import logging
from typing import (
    Any,
)

from aiomisc import (
    Service,
)
from dependency_injector.wiring import (
    Provide,
)

from .consumers import (
    DynamicConsumer,
)

logger = logging.getLogger(__name__)


class DynamicConsumerService(Service):
    """Minos QueueDispatcherService class."""

    dispatcher: DynamicConsumer = Provide["dynamic_consumer"]

    async def start(self) -> None:
        """Method to be called at the startup by the internal ``aiomisc`` loigc.

        :return: This method does not return anything.
        """
        await self.dispatcher.setup()

        try:
            self.start_event.set()
        except RuntimeError:
            logger.warning("Runtime is not properly setup.")

        await self.dispatcher.dispatch()

    async def stop(self, exception: Exception = None) -> Any:
        """Stop the service execution.

        :param exception: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await self.dispatcher.destroy()

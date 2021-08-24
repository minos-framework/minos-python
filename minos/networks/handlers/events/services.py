"""minos.networks.events.services module."""

import logging
from typing import (
    Any,
)

from aiomisc import (
    Service,
)
from cached_property import (
    cached_property,
)

from .consumers import (
    EventConsumer,
)
from .handlers import (
    EventHandler,
)

logger = logging.getLogger(__name__)


class EventConsumerService(Service):
    """Minos QueueDispatcherService class."""

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

    @cached_property
    def dispatcher(self) -> EventConsumer:
        """Get the service dispatcher.

        :return: A ``EventConsumer`` instance.
        """
        return EventConsumer.from_config()


class EventHandlerService(Service):
    """Minos QueueDispatcherService class."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_kwargs = kwargs

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

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await self.dispatcher.destroy()

    @cached_property
    def dispatcher(self) -> EventHandler:
        """Get the service dispatcher.

        :return: A ``EventHandler`` instance.
        """
        return EventHandler.from_config(**self._init_kwargs)

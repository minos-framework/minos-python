"""minos.networks.handlers.services module."""

import logging
from typing import (
    Any,
    Optional,
)

from aiomisc import (
    Service,
)
from dependency_injector.wiring import (
    Provide,
)

from .consumers import (
    Consumer,
)

logger = logging.getLogger(__name__)


class ConsumerService(Service):
    """Minos QueueDispatcherService class."""

    dispatcher: Consumer = Provide["consumer"]

    def __init__(self, dispatcher: Optional[Consumer] = None, **kwargs):
        super().__init__(**kwargs)

        if dispatcher is not None:
            self.dispatcher = dispatcher

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

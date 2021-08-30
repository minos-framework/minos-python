"""minos.networks.handlers.command_replies.services module."""

import logging

from aiomisc import (
    Service,
)
from cached_property import (
    cached_property,
)

from .handlers import (
    CommandReplyHandler,
)

logger = logging.getLogger(__name__)


class CommandReplyHandlerService(Service):
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

        await self.dispatcher.dispatch_forever()

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await self.dispatcher.destroy()

    @cached_property
    def dispatcher(self) -> CommandReplyHandler:
        """Get the service dispatcher.

        :return: A ``CommandReplyHandler`` instance.
        """
        return CommandReplyHandler.from_config(**self._init_kwargs)

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
from dependency_injector.wiring import (
    Provide,
    inject,
)

from .consumers import (
    BrokerConsumer,
)
from .handlers import (
    BrokerHandler,
)

logger = logging.getLogger(__name__)


class BrokerConsumerService(Service):
    """Minos QueueDispatcherService class."""

    @inject
    def __init__(self, dispatcher: BrokerConsumer = Provide["consumer"], **kwargs):
        super().__init__(**kwargs)
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


class BrokerHandlerService(Service):
    """TODO"""

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
    def dispatcher(self) -> BrokerHandler:
        """Get the service dispatcher.

        :return: A ``Handler`` instance.
        """
        return BrokerHandler.from_config(**self._init_kwargs)

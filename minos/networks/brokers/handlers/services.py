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

from minos.common import (
    NotProvidedException,
)

from .consumers import (
    BrokerConsumer,
)
from .handlers import (
    BrokerHandler,
)

logger = logging.getLogger(__name__)


class BrokerConsumerService(Service):
    """Broker Consumer Service class."""

    @inject
    def __init__(self, consumer: BrokerConsumer = Provide["broker_consumer"], **kwargs):
        super().__init__(**kwargs)
        if consumer is None or isinstance(consumer, Provide):
            raise NotProvidedException(f"A {BrokerConsumer!r} object must be provided.")
        self.consumer = consumer

    async def start(self) -> None:
        """Start the service execution.

        :return: This method does not return anything.
        """
        await self.consumer.setup()

        try:
            self.start_event.set()
        except RuntimeError:
            logger.warning("Runtime is not properly setup.")

        await self.consumer.dispatch()

    async def stop(self, exception: Exception = None) -> Any:
        """Stop the service execution.

        :param exception: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await self.consumer.destroy()


class BrokerHandlerService(Service):
    """Broker Handler Service class."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_kwargs = kwargs

    async def start(self) -> None:
        """Start the service execution.

        :return: This method does not return anything.
        """
        await self.handler.setup()

        try:
            self.start_event.set()
        except RuntimeError:
            logger.warning("Runtime is not properly setup.")

        await self.handler.dispatch_forever()

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await self.handler.destroy()

    @cached_property
    def handler(self) -> BrokerHandler:
        """Get the service handler.

        :return: A ``Handler`` instance.
        """
        return BrokerHandler.from_config(**self._init_kwargs)

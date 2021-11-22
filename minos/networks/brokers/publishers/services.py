from __future__ import (
    annotations,
)

import logging

from aiomisc import (
    Service,
)
from cached_property import (
    cached_property,
)

from .producers import (
    BrokerProducer,
)

logger = logging.getLogger(__name__)


class BrokerProducerService(Service):
    """Broker Producer Service class."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_kwargs = kwargs

    async def start(self) -> None:
        """Start the service execution.

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
    def dispatcher(self) -> BrokerProducer:
        """Get the service dispatcher.

        :return: A ``Producer`` instance.
        """
        return BrokerProducer.from_config(**self._init_kwargs)

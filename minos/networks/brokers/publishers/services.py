from __future__ import (
    annotations,
)

import logging

from aiomisc import (
    Service,
)
from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    NotProvidedException,
)

from .abc import (
    BrokerPublisher,
)
from .queued import (
    QueuedBrokerPublisher,
)

logger = logging.getLogger(__name__)


class BrokerPublisherService(Service):
    """Broker Producer Service class."""

    @inject
    def __init__(self, impl: BrokerPublisher = Provide["broker_publisher"], **kwargs):
        super().__init__(**kwargs)

        if impl is None or isinstance(impl, Provide):
            raise NotProvidedException(f"A {BrokerPublisher!r} object must be provided.")

        self.impl = impl

    async def start(self) -> None:
        """Start the service execution.

        :return: This method does not return anything.
        """
        await self.impl.setup()

        try:
            self.start_event.set()
        except RuntimeError:
            logger.warning("Runtime is not properly setup.")

        if isinstance(self.impl, QueuedBrokerPublisher):
            await self.impl.run()

    async def stop(self, err: Exception = None) -> None:
        """Stop the service execution.

        :param err: Optional exception that stopped the execution.
        :return: This method does not return anything.
        """
        await self.impl.destroy()

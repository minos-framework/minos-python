from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
)

from minos.common import (
    DatabaseClient,
)

from .....collections import (
    DatabaseBrokerQueue,
    DatabaseBrokerQueueBuilder,
)
from ..abc import (
    BrokerSubscriberQueue,
    BrokerSubscriberQueueBuilder,
)
from .factories import (
    BrokerSubscriberQueueDatabaseOperationFactory,
)

logger = logging.getLogger(__name__)


class DatabaseBrokerSubscriberQueue(
    DatabaseBrokerQueue[BrokerSubscriberQueueDatabaseOperationFactory], BrokerSubscriberQueue
):
    """Database Broker Subscriber Queue class."""

    async def _get_count(self) -> int:
        # noinspection PyTypeChecker
        operation = self.database_operation_factory.build_count(self._retry, self.topics)
        row = await self.execute_on_database_and_fetch_one(operation)
        count = row[0]
        return count

    async def _dequeue_rows(self, client: DatabaseClient) -> list[Any]:
        operation = self.database_operation_factory.build_query(self._retry, self._records, self.topics)
        await client.execute(operation)
        return [row async for row in client.fetch_all()]


class DatabaseBrokerSubscriberQueueBuilder(
    BrokerSubscriberQueueBuilder[DatabaseBrokerSubscriberQueue], DatabaseBrokerQueueBuilder
):
    """Database Broker Subscriber Queue Builder class."""


DatabaseBrokerSubscriberQueue.set_builder(DatabaseBrokerSubscriberQueueBuilder)

from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
    Optional,
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
    AiopgBrokerSubscriberQueueDatabaseOperationFactory,
)

logger = logging.getLogger(__name__)


class DatabaseBrokerSubscriberQueue(DatabaseBrokerQueue, BrokerSubscriberQueue):
    """PostgreSql Broker Subscriber Queue class."""

    def __init__(
        self,
        topics: set[str],
        *args,
        query_factory: Optional[AiopgBrokerSubscriberQueueDatabaseOperationFactory] = None,
        **kwargs,
    ):
        if query_factory is None:
            query_factory = AiopgBrokerSubscriberQueueDatabaseOperationFactory()
        super().__init__(topics, *args, query_factory=query_factory, **kwargs)

    async def _get_count(self) -> int:
        # noinspection PyTypeChecker
        operation = self._query_factory.build_count_not_processed(self._retry, self.topics)
        row = await self.submit_query_and_fetchone(operation)
        count = row[0]
        return count

    async def _dequeue_rows(self, client: DatabaseClient) -> list[Any]:
        operation = self._query_factory.build_select_not_processed(self._retry, self._records, self.topics)
        await client.execute(operation)
        return [row async for row in client.fetch_all()]


class DatabaseBrokerSubscriberQueueBuilder(
    BrokerSubscriberQueueBuilder[DatabaseBrokerSubscriberQueue], DatabaseBrokerQueueBuilder
):
    """PostgreSql Broker Subscriber Queue Builder class."""


DatabaseBrokerSubscriberQueue.set_builder(DatabaseBrokerSubscriberQueueBuilder)

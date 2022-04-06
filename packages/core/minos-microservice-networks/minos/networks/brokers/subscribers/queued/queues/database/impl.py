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
    PostgreSqlBrokerQueue,
    PostgreSqlBrokerQueueBuilder,
)
from ..abc import (
    BrokerSubscriberQueue,
    BrokerSubscriberQueueBuilder,
)
from .factories import (
    PostgreSqlBrokerSubscriberQueueQueryFactory,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerSubscriberQueue(PostgreSqlBrokerQueue, BrokerSubscriberQueue):
    """PostgreSql Broker Subscriber Queue class."""

    def __init__(
        self,
        topics: set[str],
        *args,
        query_factory: Optional[PostgreSqlBrokerSubscriberQueueQueryFactory] = None,
        **kwargs,
    ):
        if query_factory is None:
            query_factory = PostgreSqlBrokerSubscriberQueueQueryFactory()
        super().__init__(topics, *args, query_factory=query_factory, **kwargs)

    async def _get_count(self) -> int:
        # noinspection PyTypeChecker
        operation = self._query_factory.build_count_not_processed(self._retry, self.topics)
        row = await self.submit_query_and_fetchone(operation)
        count = row[0]
        return count

    async def _dequeue_rows(self, client: DatabaseClient) -> list[Any]:
        # noinspection PyTypeChecker
        await client.execute(self._query_factory.build_select_not_processed(self._retry, self._records, self.topics))
        return [row async for row in client.fetch_all()]


class PostgreSqlBrokerSubscriberQueueBuilder(
    BrokerSubscriberQueueBuilder[PostgreSqlBrokerSubscriberQueue], PostgreSqlBrokerQueueBuilder
):
    """PostgreSql Broker Subscriber Queue Builder class."""


PostgreSqlBrokerSubscriberQueue.set_builder(PostgreSqlBrokerSubscriberQueueBuilder)

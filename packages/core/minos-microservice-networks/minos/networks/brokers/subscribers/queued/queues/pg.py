from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
    Optional,
)

from psycopg2.sql import (
    SQL,
)

from minos.common import (
    DatabaseClient,
)

from ....collections import (
    PostgreSqlBrokerQueue,
    PostgreSqlBrokerQueueBuilder,
    PostgreSqlBrokerQueueQueryFactory,
)
from .abc import (
    BrokerSubscriberQueue,
    BrokerSubscriberQueueBuilder,
)

logger = logging.getLogger(__name__)


class PostgreSqlBrokerSubscriberQueue(PostgreSqlBrokerQueue, BrokerSubscriberQueue):
    """PostgreSql Broker Subscriber Queue class."""

    def __init__(
        self, topics: set[str], *args, query_factory: Optional[PostgreSqlBrokerQueueQueryFactory] = None, **kwargs
    ):
        if query_factory is None:
            query_factory = PostgreSqlBrokerSubscriberQueueQueryFactory()
        super().__init__(topics, *args, query_factory=query_factory, **kwargs)

    async def _get_count(self) -> int:
        # noinspection PyTypeChecker
        row = await self.submit_query_and_fetchone(
            self._query_factory.build_count_not_processed(), (self._retry, tuple(self.topics))
        )
        count = row[0]
        return count

    async def _dequeue_rows(self, client: DatabaseClient) -> list[Any]:
        # noinspection PyTypeChecker
        await client.execute(
            self._query_factory.build_select_not_processed(), (self._retry, tuple(self.topics), self._records)
        )
        return [row async for row in client.fetch_all()]


# noinspection SqlNoDataSourceInspection,SqlResolve
class PostgreSqlBrokerSubscriberQueueQueryFactory(PostgreSqlBrokerQueueQueryFactory):
    """PostgreSql Broker Subscriber Queue Query Factory class."""

    def build_table_name(self) -> str:
        """Get the table name.

        :return: A ``str`` value.
        """
        return "broker_subscriber_queue"

    def build_count_not_processed(self) -> SQL:
        """Build the "count not processed" query.

        :return:
        """
        return SQL(
            f"SELECT COUNT(*) FROM (SELECT id FROM {self.build_table_name()} "
            "WHERE NOT processing AND retry < %s AND topic IN %s FOR UPDATE SKIP LOCKED) s"
        )

    def build_select_not_processed(self) -> SQL:
        """Build the "select not processed" query.

        :return: A ``SQL`` instance.
        """
        return SQL(
            "SELECT id, data "
            f"FROM {self.build_table_name()} "
            "WHERE NOT processing AND retry < %s AND topic IN %s "
            "ORDER BY created_at "
            "LIMIT %s "
            "FOR UPDATE SKIP LOCKED"
        )


class PostgreSqlBrokerSubscriberQueueBuilder(
    BrokerSubscriberQueueBuilder[PostgreSqlBrokerSubscriberQueue], PostgreSqlBrokerQueueBuilder
):
    """PostgreSql Broker Subscriber Queue Builder class."""


PostgreSqlBrokerSubscriberQueue.set_builder(PostgreSqlBrokerSubscriberQueueBuilder)

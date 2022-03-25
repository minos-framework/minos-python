from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
    Optional,
)

from aiopg import (
    Cursor,
)
from psycopg2.sql import (
    SQL,
    Identifier,
)

from ....collections import (
    PostgreSqlBrokerQueue,
    PostgreSqlBrokerQueueBuilder,
    PostgreSqlBrokerQueueQueryFactory,
)
from ....messages import (
    BrokerMessage,
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

    async def _notify_enqueued(self, message: BrokerMessage) -> None:
        await self.submit_query(self._query_factory.build_notify().format(Identifier(message.topic)))

    async def _listen_entries(self, cursor: Cursor) -> None:
        for topic in self.topics:
            await cursor.execute(self._query_factory.build_listen().format(Identifier(topic)))

    async def _unlisten_entries(self, cursor: Cursor) -> None:
        if not cursor.closed:
            for topic in self.topics:
                await cursor.execute(self._query_factory.build_unlisten().format(Identifier(topic)))

    async def _get_count(self, cursor: Cursor) -> int:
        # noinspection PyTypeChecker
        await cursor.execute(self._query_factory.build_count_not_processed(), (self._retry, tuple(self.topics)))
        count = (await cursor.fetchone())[0]
        return count

    async def _dequeue_rows(self, cursor: Cursor) -> list[Any]:
        # noinspection PyTypeChecker
        await cursor.execute(
            self._query_factory.build_select_not_processed(), (self._retry, tuple(self.topics), self._records)
        )
        return await cursor.fetchall()


class PostgreSqlBrokerSubscriberQueueQueryFactory(PostgreSqlBrokerQueueQueryFactory):
    """PostgreSql Broker Subscriber Queue Query Factory class."""

    def build_table_name(self) -> str:
        """Get the table name.

        :return: A ``str`` value.
        """
        return "broker_subscriber_queue"

    def build_notify(self) -> SQL:
        """Build the "notify" query.

        :return: A ``SQL`` instance.
        """
        return SQL("NOTIFY {}")

    def build_listen(self) -> SQL:
        """Build the "listen" query.

        :return: A ``SQL`` instance.
        """
        return SQL("LISTEN {}")

    def build_unlisten(self) -> SQL:
        """Build the "unlisten" query.

        :return: A ``SQL`` instance.
        """
        return SQL("UNLISTEN {}")

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

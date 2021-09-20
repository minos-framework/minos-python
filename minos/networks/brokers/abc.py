from abc import (
    ABC,
)

from psycopg2.sql import (
    SQL,
)

from minos.common import (
    MinosBroker,
    PostgreSqlMinosDatabase,
)


class BrokerSetup(PostgreSqlMinosDatabase):
    """Minos Broker Setup Class"""

    async def _setup(self) -> None:
        await self._create_broker_table()

    async def _create_broker_table(self) -> None:
        await self.submit_query(_CREATE_TABLE_QUERY, lock=hash("producer_queue"))


class Broker(MinosBroker, BrokerSetup, ABC):
    """Minos Broker Class."""

    ACTION: str

    async def enqueue(self, topic: str, raw: bytes) -> int:
        """Send a sequence of bytes to the given topic.

        :param topic: Topic in which the bytes will be send.
        :param raw: Bytes sequence to be send.
        :return: The identifier of the message in the queue.
        """
        params = (topic, raw, self.ACTION)
        raw = await self.submit_query_and_fetchone(_INSERT_ENTRY_QUERY, params)
        await self.submit_query(_NOTIFY_QUERY)
        return raw[0]


_CREATE_TABLE_QUERY = SQL(
    "CREATE TABLE IF NOT EXISTS producer_queue ("
    "id BIGSERIAL NOT NULL PRIMARY KEY, "
    "topic VARCHAR(255) NOT NULL, "
    "data BYTEA NOT NULL, "
    "action VARCHAR(255) NOT NULL, "
    "retry INTEGER NOT NULL DEFAULT 0, "
    "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), "
    "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
)

_INSERT_ENTRY_QUERY = SQL("INSERT INTO producer_queue (topic, data, action) VALUES (%s, %s, %s) RETURNING id")

_NOTIFY_QUERY = SQL("NOTIFY producer_queue")

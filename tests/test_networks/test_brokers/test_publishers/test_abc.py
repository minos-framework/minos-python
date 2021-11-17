import unittest
from unittest.mock import (
    AsyncMock,
    call,
)

import aiopg
from psycopg2.sql import (
    SQL,
)

from minos.common import (
    Model,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerPublisher,
    BrokerPublisherSetup,
)
from tests.utils import (
    BASE_PATH,
)


class _FakeBrokerPublisher(BrokerPublisher):
    ACTION = "fake"

    async def send(self, items: list[Model], **kwargs) -> None:
        """For testing purposes"""


class TestBrokerSetup(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.broker_setup = BrokerPublisherSetup(**self.config.broker.queue._asdict())

    async def test_setup(self):
        async with self.broker_setup:
            pass

        async with aiopg.connect(**self.broker_queue_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT 1 "
                    "FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = 'producer_queue';"
                )
                ret = []
                async for row in cursor:
                    ret.append(row)

        assert ret == [(1,)]


class TestBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.broker = _FakeBrokerPublisher(**self.broker_queue_db)

    async def test_enqueue(self):
        query = SQL("INSERT INTO producer_queue (topic, data, action) VALUES (%s, %s, %s) RETURNING id")

        mock = AsyncMock(return_value=(56,))

        async with self.broker:
            self.broker.submit_query_and_fetchone = mock

            identifier = await self.broker.enqueue("test_topic", b"test")

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        self.assertEqual(call(query, ("test_topic", b"test", "fake")), mock.call_args)


if __name__ == "__main__":
    unittest.main()

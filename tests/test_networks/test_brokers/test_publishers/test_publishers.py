import unittest
from unittest.mock import (
    AsyncMock,
    call,
)

from psycopg2.sql import (
    SQL,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerPublisher,
)
from tests.utils import (
    BASE_PATH,
)


class TestBrokerPublisher(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.broker = BrokerPublisher.from_config(self.config)

    async def test_enqueue(self):
        query = SQL("INSERT INTO producer_queue (topic, data, action) VALUES (%s, %s, %s) RETURNING id")

        mock = AsyncMock(return_value=(56,))

        async with self.broker:
            self.broker.submit_query_and_fetchone = mock

            identifier = await self.broker.enqueue("test_topic", b"test")

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        self.assertEqual(call(query, ("test_topic", b"test", "command")), mock.call_args)


if __name__ == "__main__":
    unittest.main()

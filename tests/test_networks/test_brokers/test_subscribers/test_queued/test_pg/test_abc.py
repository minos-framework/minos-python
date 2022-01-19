import unittest

import aiopg

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks.brokers.subscribers.queued.repositories.pg.abc import (
    BrokerHandlerSetup,
)
from tests.utils import (
    BASE_PATH,
)


class _FakeBrokerHandlerSetup(BrokerHandlerSetup):
    TABLE_NAME = "fake"


class TestHandlerSetup(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def test_if_queue_table_exists(self):
        async with _FakeBrokerHandlerSetup(**self.broker_queue_db):
            pass

        async with aiopg.connect(**self.broker_queue_db) as connect:
            async with connect.cursor() as cur:
                await cur.execute(
                    "SELECT 1 "
                    "FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = 'consumer_queue';"
                )
                ret = []
                async for row in cur:
                    ret.append(row)

        assert ret == [(1,)]


if __name__ == "__main__":
    unittest.main()

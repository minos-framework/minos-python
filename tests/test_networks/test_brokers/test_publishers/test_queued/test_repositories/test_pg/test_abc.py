import unittest

import aiopg

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
)


@unittest.skip("FIXME")
class TestBrokerSetup(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.broker_setup = PostgreSqlBrokerPublisherRepositorySetup(**self.config.broker.queue._asdict())  # noqa: F821

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


if __name__ == "__main__":
    unittest.main()

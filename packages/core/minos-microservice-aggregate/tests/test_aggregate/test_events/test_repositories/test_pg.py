import unittest

from minos.aggregate import (
    EventRepository,
    DatabaseEventRepository,
)
from minos.common import (
    AiopgDatabaseClient,
    DatabaseClientPool,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.testcases import (
    EventRepositorySelectTestCase,
    EventRepositorySubmitTestCase,
)


# noinspection SqlNoDataSourceInspection
class TestDatabaseEventRepositorySubmit(EventRepositorySubmitTestCase, PostgresAsyncTestCase):
    __test__ = True

    @staticmethod
    def build_event_repository() -> EventRepository:
        """Fort testing purposes."""
        return DatabaseEventRepository()

    def test_constructor(self):
        pool = DatabaseClientPool.from_config(self.config)
        repository = DatabaseEventRepository(pool)
        self.assertIsInstance(repository, DatabaseEventRepository)
        self.assertIsInstance(repository.pool, DatabaseClientPool)

    def test_from_config(self):
        repository = DatabaseEventRepository.from_config(self.config)
        self.assertIsInstance(repository.pool, DatabaseClientPool)

    async def test_setup(self):
        async with AiopgDatabaseClient(**self.config.get_default_database()) as client:
            await client.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_event');"
            )
            response = (await client.fetch_one())[0]
        self.assertTrue(response)


class TestPostgreSqlRepositorySelect(EventRepositorySelectTestCase, PostgresAsyncTestCase):
    __test__ = True

    @staticmethod
    def build_event_repository() -> EventRepository:
        """Fort testing purposes."""
        return DatabaseEventRepository()


if __name__ == "__main__":
    unittest.main()

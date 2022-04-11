import unittest

from minos.aggregate import (
    DatabaseEventRepository,
    EventRepository,
)
from minos.common import (
    DatabaseClientPool,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
)
from tests.testcases import (
    EventRepositorySelectTestCase,
    EventRepositorySubmitTestCase,
)


# noinspection SqlNoDataSourceInspection
@unittest.skip
class TestDatabaseEventRepositorySubmit(EventRepositorySubmitTestCase, DatabaseMinosTestCase):
    __test__ = True

    @staticmethod
    def build_event_repository() -> EventRepository:
        """Fort testing purposes."""
        return DatabaseEventRepository()

    def test_constructor(self):
        pool = DatabaseClientPool.from_config(self.config)
        repository = DatabaseEventRepository(pool)
        self.assertIsInstance(repository, DatabaseEventRepository)
        self.assertIsInstance(repository.database_pool, DatabaseClientPool)

    def test_from_config(self):
        repository = DatabaseEventRepository.from_config(self.config)
        self.assertIsInstance(repository.database_pool, DatabaseClientPool)

    async def test_setup(self):
        async with self.get_client() as client:
            from minos.plugins.aiopg import (
                AiopgDatabaseOperation,
            )

            operation = AiopgDatabaseOperation(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_event');"
            )
            await client.execute(operation)
            response = (await client.fetch_one())[0]
        self.assertTrue(response)


@unittest.skip
class TestDatabaseEventRepositorySelect(EventRepositorySelectTestCase, DatabaseMinosTestCase):
    __test__ = True

    @staticmethod
    def build_event_repository() -> EventRepository:
        """Fort testing purposes."""
        return DatabaseEventRepository()


if __name__ == "__main__":
    unittest.main()

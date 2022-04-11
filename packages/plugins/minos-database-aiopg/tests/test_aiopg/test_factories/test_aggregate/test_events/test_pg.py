import unittest

from minos.aggregate import (
    DatabaseEventRepository,
    EventRepository,
)
from minos.aggregate.testing import (
    EventRepositorySelectTestCase,
    EventRepositorySubmitTestCase,
)
from minos.common import (
    DatabaseClientPool,
)
from tests.utils import (
    AiopgTestCase,
)


# noinspection SqlNoDataSourceInspection
class TestDatabaseEventRepositorySubmit(AiopgTestCase, EventRepositorySubmitTestCase):
    __test__ = True

    def build_event_repository(self) -> EventRepository:
        """Fort testing purposes."""
        return DatabaseEventRepository.from_config(self.config)

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


class TestDatabaseEventRepositorySelect(AiopgTestCase, EventRepositorySelectTestCase):
    __test__ = True

    def build_event_repository(self) -> EventRepository:
        """Fort testing purposes."""
        return DatabaseEventRepository.from_config(self.config)


if __name__ == "__main__":
    unittest.main()

import unittest

from minos.aggregate import (
    DatabaseDeltaRepository,
    DeltaRepository,
)
from minos.aggregate.testing import (
    DeltaRepositoryTestCase,
)
from minos.common import (
    DatabaseClientPool,
)
from minos.plugins.aiopg import (
    AiopgDatabaseClient,
    AiopgDatabaseOperation,
)
from tests.utils import (
    AiopgTestCase,
)


# noinspection SqlNoDataSourceInspection
class TestDatabaseDeltaRepositorySubmit(AiopgTestCase, DeltaRepositoryTestCase):
    __test__ = True

    def build_delta_repository(self) -> DeltaRepository:
        """Fort testing purposes."""
        return DatabaseDeltaRepository.from_config(self.config)

    def test_constructor(self):
        pool = DatabaseClientPool.from_config(self.config)
        repository = DatabaseDeltaRepository(pool)
        self.assertIsInstance(repository, DatabaseDeltaRepository)
        self.assertIsInstance(repository.database_pool, DatabaseClientPool)

    def test_from_config(self):
        repository = DatabaseDeltaRepository.from_config(self.config)
        self.assertIsInstance(repository.database_pool, DatabaseClientPool)

    async def test_setup(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            operation = AiopgDatabaseOperation(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_event');"
            )
            await client.execute(operation)
            response = (await client.fetch_one())[0]
        self.assertTrue(response)


if __name__ == "__main__":
    unittest.main()

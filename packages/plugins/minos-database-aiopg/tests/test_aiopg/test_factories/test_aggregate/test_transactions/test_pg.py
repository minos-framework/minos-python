import unittest

from minos.aggregate import (
    DatabaseTransactionRepository,
    TransactionRepository,
)
from minos.aggregate.testing import (
    TransactionRepositoryTestCase,
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
class TestDatabaseTransactionRepository(AiopgTestCase, TransactionRepositoryTestCase):
    __test__ = True

    def build_transaction_repository(self) -> TransactionRepository:
        return DatabaseTransactionRepository.from_config(self.config)

    def test_constructor(self):
        pool = DatabaseClientPool.from_config(self.config)
        repository = DatabaseTransactionRepository(pool)
        self.assertIsInstance(repository, DatabaseTransactionRepository)
        self.assertEqual(pool, repository.database_pool)

    def test_from_config(self):
        repository = DatabaseTransactionRepository.from_config(self.config)
        self.assertIsInstance(repository.database_pool, DatabaseClientPool)

    async def test_setup(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            operation = AiopgDatabaseOperation(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_transaction');"
            )
            await client.execute(operation)
            response = (await client.fetch_one())[0]
        self.assertTrue(response)


if __name__ == "__main__":
    unittest.main()

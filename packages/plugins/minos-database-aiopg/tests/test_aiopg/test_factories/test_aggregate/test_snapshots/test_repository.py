import unittest

from minos.aggregate import (
    DatabaseSnapshotRepository,
)
from minos.aggregate.testing import (
    SnapshotRepositoryTestCase,
)
from minos.aggregate.testing.snapshot_repository import (
    Car,
)
from minos.plugins.aiopg import (
    AiopgDatabaseClient,
    AiopgDatabaseOperation,
)
from tests.utils import (
    AiopgTestCase,
)


# noinspection SqlNoDataSourceInspection
class TestDatabaseSnapshotRepository(AiopgTestCase, SnapshotRepositoryTestCase):
    __test__ = True

    def build_snapshot_repository(self):
        return DatabaseSnapshotRepository.from_config(self.config)

    async def test_setup_snapshot_table(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            operation = AiopgDatabaseOperation(
                "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'snapshot');"
            )
            await client.execute(operation)
            observed = (await client.fetch_one())[0]
        self.assertEqual(True, observed)

    async def test_setup_snapshot_aux_offset_table(self):
        async with AiopgDatabaseClient.from_config(self.config) as client:
            operation = AiopgDatabaseOperation(
                "SELECT EXISTS (SELECT FROM pg_tables WHERE "
                "schemaname = 'public' AND tablename = 'snapshot_aux_offset');"
            )
            await client.execute(operation)
            observed = (await client.fetch_one())[0]
        self.assertEqual(True, observed)

    async def test_is_synced(self):
        await self.populate()
        self.assertFalse(await self.snapshot_repository.is_synced(Car))
        await self.snapshot_repository.synchronize()
        self.assertTrue(await self.snapshot_repository.is_synced(Car))


if __name__ == "__main__":
    unittest.main()

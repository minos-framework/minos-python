import unittest

from minos.aggregate import (
    DatabaseSnapshotSetup,
)
from minos.common import (
    AiopgDatabaseClient,
    AiopgDatabaseOperation,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    AggregateTestCase,
)


# noinspection SqlNoDataSourceInspection
class TestDatabaseSnapshotSetup(AggregateTestCase, PostgresAsyncTestCase):
    async def test_setup_snapshot_table(self):
        async with DatabaseSnapshotSetup.from_config(self.config):
            async with AiopgDatabaseClient(**self.config.get_default_database()) as client:
                operation = AiopgDatabaseOperation(
                    "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'snapshot');"
                )
                await client.execute(operation)
                observed = (await client.fetch_one())[0]
        self.assertEqual(True, observed)

    async def test_setup_snapshot_aux_offset_table(self):
        async with DatabaseSnapshotSetup.from_config(self.config):
            async with AiopgDatabaseClient(**self.config.get_default_database()) as client:
                operation = AiopgDatabaseOperation(
                    "SELECT EXISTS (SELECT FROM pg_tables WHERE "
                    "schemaname = 'public' AND tablename = 'snapshot_aux_offset');"
                )
                await client.execute(operation)
                observed = (await client.fetch_one())[0]
        self.assertEqual(True, observed)


if __name__ == "__main__":
    unittest.main()

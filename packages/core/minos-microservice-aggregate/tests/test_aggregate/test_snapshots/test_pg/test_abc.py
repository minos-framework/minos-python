import unittest

from minos.aggregate import (
    DatabaseSnapshotSetup,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
)
from tests.utils import (
    AggregateTestCase,
)


# noinspection SqlNoDataSourceInspection
@unittest.skip
class TestDatabaseSnapshotSetup(AggregateTestCase, DatabaseMinosTestCase):
    async def test_setup_snapshot_table(self):
        async with DatabaseSnapshotSetup.from_config(self.config):
            async with self.get_client() as client:
                from minos.plugins.aiopg import (
                    AiopgDatabaseOperation,
                )

                operation = AiopgDatabaseOperation(
                    "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'snapshot');"
                )
                await client.execute(operation)
                observed = (await client.fetch_one())[0]
        self.assertEqual(True, observed)

    async def test_setup_snapshot_aux_offset_table(self):
        async with DatabaseSnapshotSetup.from_config(self.config):
            async with self.get_client() as client:
                from minos.plugins.aiopg import (
                    AiopgDatabaseOperation,
                )

                operation = AiopgDatabaseOperation(
                    "SELECT EXISTS (SELECT FROM pg_tables WHERE "
                    "schemaname = 'public' AND tablename = 'snapshot_aux_offset');"
                )
                await client.execute(operation)
                observed = (await client.fetch_one())[0]
        self.assertEqual(True, observed)


if __name__ == "__main__":
    unittest.main()

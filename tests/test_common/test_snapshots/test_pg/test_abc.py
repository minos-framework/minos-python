import unittest

import aiopg

from minos.common import (
    PostgreSqlSnapshotSetup,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
)


class _PostgreSqlSnapshotSetup(PostgreSqlSnapshotSetup):
    @classmethod
    def _from_config(cls, *args, config, **kwargs):
        return cls(*args, **config.snapshot._asdict(), **kwargs)


class TestPostgreSqlSnapshotSetup(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def test_setup_snapshot_table(self):
        async with _PostgreSqlSnapshotSetup.from_config(config=self.config):
            async with aiopg.connect(**self.snapshot_db) as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        "SELECT EXISTS (SELECT FROM pg_tables "
                        "WHERE schemaname = 'public' AND tablename = 'snapshot');"
                    )
                    observed = (await cursor.fetchone())[0]
        self.assertEqual(True, observed)

    async def test_setup_snapshot_aux_offset_table(self):
        async with _PostgreSqlSnapshotSetup.from_config(config=self.config):
            async with aiopg.connect(**self.snapshot_db) as connection:
                async with connection.cursor() as cursor:
                    await cursor.execute(
                        "SELECT EXISTS (SELECT FROM pg_tables WHERE "
                        "schemaname = 'public' AND tablename = 'snapshot_aux_offset');"
                    )
                    observed = (await cursor.fetchone())[0]
        self.assertEqual(True, observed)


if __name__ == "__main__":
    unittest.main()

import unittest
from itertools import (
    starmap,
)
from pathlib import (
    Path,
)
from typing import (
    Any,
)
from uuid import (
    uuid4,
)

import aiopg

from .config import (
    Config,
)


class PostgresAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    CONFIG_FILE_PATH: Path

    def setUp(self) -> None:
        self._uuid = uuid4()
        self._config = Config(self.CONFIG_FILE_PATH)

        self._meta_repository_db = self._config.get_database_by_name("aggregate")

        self._meta_broker_queue_db = self._config.get_database_by_name("broker")

        self._meta_snapshot_db = self._config.get_database_by_name("aggregate")

        self._test_db = {"database": f"test_db_{self._uuid.hex}", "user": f"test_user_{self._uuid.hex}"}

        self.repository_db = self._meta_repository_db | self._test_db
        self.broker_queue_db = self._meta_broker_queue_db | self._test_db
        self.snapshot_db = self._meta_snapshot_db | self._test_db

        self.config = Config(
            self.CONFIG_FILE_PATH,
            repository_database=self.repository_db["database"],
            repository_user=self.repository_db["user"],
            broker_queue_database=self.broker_queue_db["database"],
            broker_queue_user=self.broker_queue_db["user"],
            snapshot_database=self.snapshot_db["database"],
            snapshot_user=self.snapshot_db["user"],
        )

    async def asyncSetUp(self):
        pairs = self._drop_duplicates(
            [
                (self._meta_repository_db, self.repository_db),
                (self._meta_broker_queue_db, self.broker_queue_db),
                (self._meta_snapshot_db, self.snapshot_db),
            ]
        )
        for meta, test in pairs:
            await self._setup_database(dict(meta), dict(test))

    async def _setup_database(self, meta: dict[str, Any], test: dict[str, Any]) -> None:
        await self._teardown_database(meta, test)

        async with aiopg.connect(**meta) as connection:
            async with connection.cursor() as cursor:
                template = "CREATE ROLE {user} WITH SUPERUSER CREATEDB LOGIN ENCRYPTED PASSWORD {password!r};"
                await cursor.execute(template.format(**test))

                template = "CREATE DATABASE {database} WITH OWNER = {user};"
                await cursor.execute(template.format(**test))

    async def asyncTearDown(self):
        pairs = self._drop_duplicates(
            [
                (self._meta_repository_db, self.repository_db),
                (self._meta_broker_queue_db, self.broker_queue_db),
                (self._meta_snapshot_db, self.snapshot_db),
            ]
        )

        for meta, test in pairs:
            await self._teardown_database(meta, test)

    @staticmethod
    def _drop_duplicates(items: list[(dict, dict)]):
        items = starmap(lambda a, b: (tuple(a.items()), tuple(b.items())), items)
        items = set(items)
        items = starmap(lambda a, b: (dict(a), dict(b)), items)
        items = list(items)
        return items

    @staticmethod
    async def _teardown_database(meta: dict[str, Any], test: dict[str, Any]) -> None:
        async with aiopg.connect(**meta) as connection:
            async with connection.cursor() as cursor:
                template = "DROP DATABASE IF EXISTS {database}"
                await cursor.execute(template.format(**test))

                template = "DROP ROLE IF EXISTS {user};"
                await cursor.execute(template.format(**test))

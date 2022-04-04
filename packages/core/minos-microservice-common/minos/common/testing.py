import unittest
from abc import (
    ABC,
)
from contextlib import (
    suppress,
)
from itertools import (
    starmap,
)
from pathlib import (
    Path,
)
from typing import (
    Any,
    Union,
)
from uuid import (
    uuid4,
)

import aiopg

from .config import (
    Config,
)
from .database import (
    DatabaseClientPool,
)
from .injections import (
    DependencyInjector,
    InjectableMixin,
)
from .pools import (
    PoolFactory,
)


class MinosTestCase(unittest.IsolatedAsyncioTestCase, ABC):
    CONFIG_FILE_PATH: Path

    def setUp(self) -> None:
        super().setUp()

        self.config = self.get_config()
        self.injector = DependencyInjector(self.config, self.get_injections())
        self.injector.wire_injections()

    def get_config(self) -> Config:
        return Config(self.get_config_file_path())

    def get_config_file_path(self) -> Path:
        return self.CONFIG_FILE_PATH

    def get_injections(self) -> list[Union[InjectableMixin, type[InjectableMixin], str]]:
        return []

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await self.injector.setup_injections()

    async def asyncTearDown(self) -> None:
        await self.injector.destroy_injections()
        await super().asyncTearDown()

    def tearDown(self) -> None:
        self.injector.unwire_injections()
        super().tearDown()

    def __getattr__(self, item: str) -> Any:
        if item != "injector":
            with suppress(Exception):
                return getattr(self.injector, item)
        raise AttributeError(f"{type(self).__name__!r} does not contain the {item!r} attribute.")


# noinspection SqlNoDataSourceInspection
class PostgresAsyncTestCase(MinosTestCase, ABC):
    def setUp(self):
        self._config = Config(self.get_config_file_path())
        self._uuid = uuid4()
        self._test_db = {"database": f"test_db_{self._uuid.hex}", "user": f"test_user_{self._uuid.hex}"}
        super().setUp()

    @property
    def repository_db(self) -> dict[str, Any]:
        return self.config.get_database_by_name("aggregate") | self._test_db

    @property
    def broker_queue_db(self) -> dict[str, Any]:
        return self.config.get_database_by_name("broker") | self._test_db

    @property
    def snapshot_db(self) -> dict[str, Any]:
        return self.config.get_database_by_name("aggregate") | self._test_db

    def get_config(self) -> Config:
        config = Config(self.get_config_file_path())

        base_fn = config.get_databases

        def _fn():
            return {k: (v | self._test_db) for k, v in base_fn().items()}

        config.get_databases = _fn
        return config

    def get_injections(self) -> list[Union[InjectableMixin, type[InjectableMixin], str]]:
        return [PoolFactory.from_config(self.config, default_classes={"database": DatabaseClientPool})]

    async def asyncSetUp(self):
        pairs = self._drop_duplicates(
            [(db, self._test_db) for db in self._config.get_databases().values() if "database" in db]
        )

        for meta, test in pairs:
            await self._setup_database(meta, test)

    async def _setup_database(self, meta: dict[str, Any], test: dict[str, Any]) -> None:
        await self._teardown_database(meta, test)

        async with aiopg.connect(**meta) as connection:
            async with connection.cursor() as cursor:
                template = "CREATE ROLE {user} WITH SUPERUSER CREATEDB LOGIN ENCRYPTED PASSWORD {password!r};"
                await cursor.execute(template.format(**(meta | test)))

                template = "CREATE DATABASE {database} WITH OWNER = {user};"
                await cursor.execute(template.format(**(meta | test)))

        await super().asyncSetUp()

    async def asyncTearDown(self):
        await super().asyncTearDown()

        pairs = self._drop_duplicates(
            [(db, self._test_db) for db in self._config.get_databases().values() if "database" in db]
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
                await cursor.execute(template.format(**(meta | test)))

                template = "DROP ROLE IF EXISTS {user};"
                await cursor.execute(template.format(**(meta | test)))

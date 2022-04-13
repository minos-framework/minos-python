from __future__ import (
    annotations,
)

import unittest
from abc import (
    ABC,
)
from contextlib import (
    suppress,
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

from ..config import (
    Config,
)
from ..database import (
    DatabaseClient,
    DatabaseClientPool,
    ManagementDatabaseOperationFactory,
)
from ..injections import (
    DependencyInjector,
    InjectableMixin,
)
from ..pools import (
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


class DatabaseMinosTestCase(MinosTestCase, ABC):
    def setUp(self):
        self.base_config = Config(self.get_config_file_path())
        self._uuid = uuid4()
        self._test_db = {"database": f"test_db_{self._uuid.hex}"}
        super().setUp()

    def get_client(self) -> DatabaseClient:
        default_config = self.base_config.get_default_database()
        client = default_config.get("client")
        return client.from_config(self.base_config)

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
        await self._create_database(self._test_db)
        await super().asyncSetUp()

    async def asyncTearDown(self):
        await super().asyncTearDown()
        await self._drop_database(self._test_db)

    async def _create_database(self, test: dict[str, Any]) -> None:
        await self._drop_database(test)

        async with self.get_client() as client:
            operation = client.get_factory(ManagementDatabaseOperationFactory).build_create(test["database"])
            await client.execute(operation)

    async def _drop_database(self, test: dict[str, Any]) -> None:
        async with self.get_client() as client:
            operation = client.get_factory(ManagementDatabaseOperationFactory).build_delete(test["database"])
            await client.execute(operation)

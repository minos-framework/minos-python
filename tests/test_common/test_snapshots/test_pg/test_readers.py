"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import sys
import unittest
from datetime import (
    datetime,
)

from dependency_injector import (
    containers,
    providers,
)

from minos.common import (
    MinosConfigException,
    PostgreSqlRepository,
    PostgreSqlSnapshot,
    PostgreSqlSnapshotBuilder,
    PostgreSqlSnapshotSetup,
    RepositoryEntry,
    SnapshotEntry,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
    FakeRepository,
    FakeSnapshot,
)


class TestPostgreSqlSnapshot(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.container = containers.DynamicContainer()
        self.container.event_broker = providers.Singleton(FakeBroker)
        self.container.repository = providers.Singleton(FakeRepository)
        self.container.snapshot = providers.Singleton(FakeSnapshot)
        self.container.wire(modules=[sys.modules[__name__]])

    def tearDown(self) -> None:
        self.container.unwire()
        super().tearDown()

    def test_type(self):
        self.assertTrue(issubclass(PostgreSqlSnapshot, PostgreSqlSnapshotSetup))

    def test_from_config(self):
        reader = PostgreSqlSnapshot.from_config(config=self.config)
        self.assertEqual(self.config.snapshot.host, reader.host)
        self.assertEqual(self.config.snapshot.port, reader.port)
        self.assertEqual(self.config.snapshot.database, reader.database)
        self.assertEqual(self.config.snapshot.user, reader.user)
        self.assertEqual(self.config.snapshot.password, reader.password)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            PostgreSqlSnapshot.from_config()

    async def test_get(self):
        async with await self._populate() as repository:
            async with PostgreSqlSnapshot.from_config(config=self.config, repository=repository) as snapshot:
                observed = [v async for v in snapshot.get("tests.aggregate_classes.Car", [2, 3])]

        expected = [Car(2, 2, 3, "blue"), Car(3, 1, 3, "blue")]
        self.assertEqual(expected, observed)

    async def test_get_raises(self):
        async with await self._populate() as repository:
            async with PostgreSqlSnapshot.from_config(config=self.config, repository=repository) as snapshot:
                with self.assertRaises(Exception):
                    # noinspection PyStatementEffect
                    [v async for v in snapshot.get("tests.aggregate_classes.Car", [1, 2, 3])]

    async def test_select(self):
        await self._populate()

        async with PostgreSqlSnapshot.from_config(config=self.config) as snapshot:
            observed = [v async for v in snapshot.select()]

        expected = [
            SnapshotEntry.from_aggregate(Car(2, 2, 3, "blue")),
            SnapshotEntry.from_aggregate(Car(3, 1, 3, "blue")),
        ]
        self._assert_equal_snapshot_entries(expected, observed)

    def _assert_equal_snapshot_entries(self, expected: list[SnapshotEntry], observed: list[SnapshotEntry]):
        self.assertEqual(len(expected), len(observed))
        for exp, obs in zip(expected, observed):
            self.assertEqual(exp.aggregate, obs.aggregate)
            self.assertIsInstance(obs.created_at, datetime)
            self.assertIsInstance(obs.updated_at, datetime)

    async def _populate(self):
        car = Car(1, 1, 3, "blue")
        # noinspection PyTypeChecker
        aggregate_name: str = car.classname
        async with PostgreSqlRepository.from_config(config=self.config) as repository:
            await repository.insert(RepositoryEntry(1, aggregate_name, 1, car.avro_bytes))
            await repository.update(RepositoryEntry(1, aggregate_name, 2, car.avro_bytes))
            await repository.insert(RepositoryEntry(2, aggregate_name, 1, car.avro_bytes))
            await repository.update(RepositoryEntry(1, aggregate_name, 3, car.avro_bytes))
            await repository.delete(RepositoryEntry(1, aggregate_name, 4))
            await repository.update(RepositoryEntry(2, aggregate_name, 2, car.avro_bytes))
            await repository.insert(RepositoryEntry(3, aggregate_name, 1, car.avro_bytes))
            async with PostgreSqlSnapshotBuilder.from_config(config=self.config, repository=repository) as dispatcher:
                await dispatcher.dispatch()
            return repository


if __name__ == "__main__":
    unittest.main()

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
    FieldsDiff,
    MinosConfigException,
    MinosRepositoryAggregateNotFoundException,
    MinosRepositoryDeletedAggregateException,
    ModelField,
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

        expected = [Car(3, "blue", id=2, version=2), Car(3, "blue", id=3, version=1)]
        self.assertEqual(expected, observed)

    async def test_get_raises(self):
        async with await self._populate() as repository:
            async with PostgreSqlSnapshot.from_config(config=self.config, repository=repository) as snapshot:
                with self.assertRaises(MinosRepositoryDeletedAggregateException):
                    # noinspection PyStatementEffect
                    [v async for v in snapshot.get("tests.aggregate_classes.Car", [1])]
                with self.assertRaises(MinosRepositoryAggregateNotFoundException):
                    # noinspection PyStatementEffect
                    [v async for v in snapshot.get("tests.aggregate_classes.Car", [4])]

    async def test_select(self):
        await self._populate()

        async with PostgreSqlSnapshot.from_config(config=self.config) as snapshot:
            observed = [v async for v in snapshot.select()]

        # noinspection PyTypeChecker
        expected = [
            SnapshotEntry(1, Car.classname, 4),
            SnapshotEntry.from_aggregate(Car(3, "blue", id=2, version=2)),
            SnapshotEntry.from_aggregate(Car(3, "blue", id=3, version=1)),
        ]
        self._assert_equal_snapshot_entries(expected, observed)

    def _assert_equal_snapshot_entries(self, expected: list[SnapshotEntry], observed: list[SnapshotEntry]):
        self.assertEqual(len(expected), len(observed))
        for exp, obs in zip(expected, observed):
            if exp.data is None:
                with self.assertRaises(MinosRepositoryDeletedAggregateException):
                    # noinspection PyStatementEffect
                    obs.aggregate
            else:
                self.assertEqual(exp.aggregate, obs.aggregate)
            self.assertIsInstance(obs.created_at, datetime)
            self.assertIsInstance(obs.updated_at, datetime)

    async def _populate(self):
        diff = FieldsDiff({"doors": ModelField("doors", int, 3), "color": ModelField("color", str, "blue")})
        # noinspection PyTypeChecker
        aggregate_name: str = Car.classname
        async with PostgreSqlRepository.from_config(config=self.config) as repository:
            await repository.create(RepositoryEntry(1, aggregate_name, 1, diff.avro_bytes))
            await repository.update(RepositoryEntry(1, aggregate_name, 2, diff.avro_bytes))
            await repository.create(RepositoryEntry(2, aggregate_name, 1, diff.avro_bytes))
            await repository.update(RepositoryEntry(1, aggregate_name, 3, diff.avro_bytes))
            await repository.delete(RepositoryEntry(1, aggregate_name, 4))
            await repository.update(RepositoryEntry(2, aggregate_name, 2, diff.avro_bytes))
            await repository.create(RepositoryEntry(3, aggregate_name, 1, diff.avro_bytes))
            async with PostgreSqlSnapshotBuilder.from_config(config=self.config, repository=repository) as dispatcher:
                await dispatcher.dispatch()
            return repository


if __name__ == "__main__":
    unittest.main()

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
from unittest.mock import (
    MagicMock,
    call,
    patch,
)

from dependency_injector import (
    containers,
    providers,
)

from minos.common import (
    MinosConfigException,
    MinosRepositoryNotProvidedException,
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


class TestPostgreSqlSnapshotBuilderWithoutContainer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config_without_repository(self):
        with self.assertRaises(MinosRepositoryNotProvidedException):
            PostgreSqlSnapshotBuilder.from_config(config=self.config)


class TestPostgreSqlSnapshotBuilder(PostgresAsyncTestCase):
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
        self.assertTrue(issubclass(PostgreSqlSnapshotBuilder, PostgreSqlSnapshotSetup))

    def test_from_config(self):
        dispatcher = PostgreSqlSnapshotBuilder.from_config(config=self.config)
        self.assertEqual(self.config.snapshot.host, dispatcher.host)
        self.assertEqual(self.config.snapshot.port, dispatcher.port)
        self.assertEqual(self.config.snapshot.database, dispatcher.database)
        self.assertEqual(self.config.snapshot.user, dispatcher.user)
        self.assertEqual(self.config.snapshot.password, dispatcher.password)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            PostgreSqlSnapshotBuilder.from_config()

    async def test_dispatch_select(self):
        async with await self._populate() as repository:
            async with PostgreSqlSnapshotBuilder.from_config(config=self.config, repository=repository) as dispatcher:
                await dispatcher.dispatch()

            async with PostgreSqlSnapshot.from_config(config=self.config, repository=repository) as snapshot:
                observed = [v async for v in snapshot.select()]

        expected = [
            SnapshotEntry.from_aggregate(Car(2, 2, 3, "blue")),
            SnapshotEntry.from_aggregate(Car(3, 1, 3, "blue")),
        ]
        self._assert_equal_snapshot_entries(expected, observed)

    async def test_are_synced(self):
        async with await self._populate() as repository:
            async with PostgreSqlSnapshotBuilder.from_config(config=self.config, repository=repository) as dispatcher:
                self.assertFalse(await dispatcher.are_synced("tests.aggregate_classes.Car", [1, 2]))
                await dispatcher.dispatch()
                self.assertTrue(await dispatcher.are_synced("tests.aggregate_classes.Car", [1, 2]))

    async def test_is_synced(self):
        async with await self._populate() as repository:
            async with PostgreSqlSnapshotBuilder.from_config(config=self.config, repository=repository) as dispatcher:
                self.assertFalse(await dispatcher.is_synced("tests.aggregate_classes.Car", 1))
                await dispatcher.dispatch()
                self.assertTrue(await dispatcher.is_synced("tests.aggregate_classes.Car", 1))

    async def test_dispatch_ignore_previous_version(self):
        car = Car(1, 2, 3, "blue")

        # noinspection PyTypeChecker
        aggregate_name: str = car.classname

        async def _fn(*args, **kwargs):
            yield RepositoryEntry(1, aggregate_name, 1, car.avro_bytes)
            yield RepositoryEntry(1, aggregate_name, 3, car.avro_bytes)
            yield RepositoryEntry(1, aggregate_name, 2, car.avro_bytes)

        async with await self._populate() as repository:
            with patch("minos.common.PostgreSqlRepository.select", _fn):
                async with PostgreSqlSnapshotBuilder.from_config(
                    config=self.config, repository=repository
                ) as dispatcher:
                    await dispatcher.dispatch()

            async with PostgreSqlSnapshot.from_config(config=self.config, repository=repository) as snapshot:
                observed = [v async for v in snapshot.select()]

        expected = [SnapshotEntry(1, aggregate_name, 3, car.avro_bytes)]
        self._assert_equal_snapshot_entries(expected, observed)

    def _assert_equal_snapshot_entries(self, expected: list[SnapshotEntry], observed: list[SnapshotEntry]):
        self.assertEqual(len(expected), len(observed))
        for exp, obs in zip(expected, observed):
            self.assertEqual(exp.aggregate, obs.aggregate)
            self.assertIsInstance(obs.created_at, datetime)
            self.assertIsInstance(obs.updated_at, datetime)

    async def test_dispatch_with_offset(self):
        async with await self._populate() as repository:
            async with PostgreSqlSnapshotBuilder.from_config(config=self.config, repository=repository) as dispatcher:
                mock = MagicMock(side_effect=dispatcher._repository.select)
                dispatcher._repository.select = mock

                await dispatcher.dispatch()
                self.assertEqual(1, mock.call_count)
                self.assertEqual(call(id_ge=0), mock.call_args)
                mock.reset_mock()

                # noinspection PyTypeChecker
                await repository.insert(RepositoryEntry(3, Car.classname, 1, Car(1, 1, 3, "blue").avro_bytes))

                await dispatcher.dispatch()
                self.assertEqual(1, mock.call_count)
                self.assertEqual(call(id_ge=7), mock.call_args)
                mock.reset_mock()

                await dispatcher.dispatch()
                self.assertEqual(1, mock.call_count)
                self.assertEqual(call(id_ge=8), mock.call_args)
                mock.reset_mock()

                await dispatcher.dispatch()
                self.assertEqual(1, mock.call_count)
                self.assertEqual(call(id_ge=8), mock.call_args)
                mock.reset_mock()

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
            return repository


if __name__ == "__main__":
    unittest.main()

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
from uuid import (
    uuid4,
)

from dependency_injector import (
    containers,
    providers,
)

from minos.common import (
    Action,
    Condition,
    FieldDiff,
    FieldDiffContainer,
    MinosRepositoryNotProvidedException,
    MinosSnapshotDeletedAggregateException,
    PostgreSqlRepository,
    PostgreSqlSnapshotReader,
    PostgreSqlSnapshotSetup,
    PostgreSqlSnapshotWriter,
    RepositoryEntry,
    SnapshotEntry,
    current_datetime,
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


class TestPostgreSqlSnapshotWriterWithoutContainer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config_without_repository(self):
        PostgreSqlSnapshotWriter._repository = None
        with self.assertRaises(MinosRepositoryNotProvidedException):
            PostgreSqlSnapshotWriter.from_config(config=self.config)


class TestPostgreSqlSnapshotWriter(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_3 = uuid4()

        self.container = containers.DynamicContainer()
        self.container.event_broker = providers.Singleton(FakeBroker)
        self.container.repository = providers.Singleton(FakeRepository)
        self.container.snapshot = providers.Singleton(FakeSnapshot)
        self.container.wire(modules=[sys.modules[__name__]])

    def tearDown(self) -> None:
        self.container.unwire()
        super().tearDown()

    def test_type(self):
        self.assertTrue(issubclass(PostgreSqlSnapshotWriter, PostgreSqlSnapshotSetup))

    def test_from_config(self):
        dispatcher = PostgreSqlSnapshotWriter.from_config(config=self.config, repository=FakeRepository())
        self.assertEqual(self.config.snapshot.host, dispatcher.host)
        self.assertEqual(self.config.snapshot.port, dispatcher.port)
        self.assertEqual(self.config.snapshot.database, dispatcher.database)
        self.assertEqual(self.config.snapshot.user, dispatcher.user)
        self.assertEqual(self.config.snapshot.password, dispatcher.password)

    async def test_dispatch(self):
        async with await self._populate() as repository:
            async with PostgreSqlSnapshotWriter.from_config(config=self.config, repository=repository) as dispatcher:
                await dispatcher.dispatch()

            async with PostgreSqlSnapshotReader.from_config(config=self.config, repository=repository) as snapshot:
                observed = [v async for v in snapshot.find_entries(Car.classname, Condition.TRUE)]

        # noinspection PyTypeChecker
        expected = [
            SnapshotEntry(self.uuid_1, Car.classname, 4),
            SnapshotEntry.from_aggregate(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_2,
                    version=2,
                    created_at=observed[1].created_at,
                    updated_at=observed[1].updated_at,
                )
            ),
            SnapshotEntry.from_aggregate(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_3,
                    version=1,
                    created_at=observed[2].created_at,
                    updated_at=observed[2].updated_at,
                )
            ),
        ]
        self._assert_equal_snapshot_entries(expected, observed)

    async def test_is_synced(self):
        async with await self._populate() as repository:
            async with PostgreSqlSnapshotWriter.from_config(config=self.config, repository=repository) as dispatcher:
                self.assertFalse(await dispatcher.is_synced("tests.aggregate_classes.Car"))
                await dispatcher.dispatch()
                self.assertTrue(await dispatcher.is_synced("tests.aggregate_classes.Car"))

    async def test_dispatch_ignore_previous_version(self):
        diff = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        # noinspection PyTypeChecker
        aggregate_name: str = Car.classname
        condition = Condition.EQUAL("uuid", self.uuid_1)

        async def _fn(*args, **kwargs):
            yield RepositoryEntry(self.uuid_1, aggregate_name, 1, diff.avro_bytes, 1, Action.CREATE, current_datetime())
            yield RepositoryEntry(self.uuid_1, aggregate_name, 3, diff.avro_bytes, 2, Action.CREATE, current_datetime())
            yield RepositoryEntry(self.uuid_1, aggregate_name, 2, diff.avro_bytes, 3, Action.CREATE, current_datetime())

        async with PostgreSqlRepository.from_config(config=self.config) as r:
            with patch("minos.common.PostgreSqlRepository.select", _fn):
                async with PostgreSqlSnapshotWriter.from_config(config=self.config, repository=r) as dispatcher:
                    await dispatcher.dispatch()
            async with PostgreSqlSnapshotReader.from_config(config=self.config, repository=r) as snapshot:
                observed = [v async for v in snapshot.find_entries(aggregate_name, condition)]

        # noinspection PyTypeChecker
        expected = [
            SnapshotEntry(
                aggregate_uuid=self.uuid_1,
                aggregate_name=aggregate_name,
                version=3,
                schema=Car.avro_schema,
                data=Car(3, "blue", uuid=self.uuid_1, version=1).avro_data,
                created_at=observed[0].created_at,
                updated_at=observed[0].updated_at,
            )
        ]
        self._assert_equal_snapshot_entries(expected, observed)

    def _assert_equal_snapshot_entries(self, expected: list[SnapshotEntry], observed: list[SnapshotEntry]):
        self.assertEqual(len(expected), len(observed))
        for exp, obs in zip(expected, observed):
            if exp.data is None:
                with self.assertRaises(MinosSnapshotDeletedAggregateException):
                    # noinspection PyStatementEffect
                    obs.build_aggregate()
            else:
                self.assertEqual(exp.build_aggregate(), obs.build_aggregate())
            self.assertIsInstance(obs.created_at, datetime)
            self.assertIsInstance(obs.updated_at, datetime)

    async def test_dispatch_with_offset(self):
        async with await self._populate() as repository:
            async with PostgreSqlSnapshotWriter.from_config(config=self.config, repository=repository) as dispatcher:
                mock = MagicMock(side_effect=dispatcher._repository.select)
                dispatcher._repository.select = mock

                await dispatcher.dispatch()
                self.assertEqual(1, mock.call_count)
                self.assertEqual(call(id_gt=0), mock.call_args)
                mock.reset_mock()

                # noinspection PyTypeChecker
                entry = RepositoryEntry(
                    aggregate_uuid=self.uuid_3,
                    aggregate_name=Car.classname,
                    version=1,
                    data=FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")]).avro_bytes,
                )
                await repository.create(entry)

                await dispatcher.dispatch()
                self.assertEqual(1, mock.call_count)
                self.assertEqual(call(id_gt=7), mock.call_args)
                mock.reset_mock()

                await dispatcher.dispatch()
                self.assertEqual(1, mock.call_count)
                self.assertEqual(call(id_gt=8), mock.call_args)
                mock.reset_mock()

                await dispatcher.dispatch()
                self.assertEqual(1, mock.call_count)
                self.assertEqual(call(id_gt=8), mock.call_args)
                mock.reset_mock()

    async def _populate(self):
        diff = FieldDiffContainer([FieldDiff("doors", int, 3), FieldDiff("color", str, "blue")])
        # noinspection PyTypeChecker
        aggregate_name: str = Car.classname
        async with PostgreSqlRepository.from_config(config=self.config) as repository:
            await repository.create(RepositoryEntry(self.uuid_1, aggregate_name, 1, diff.avro_bytes))
            await repository.update(RepositoryEntry(self.uuid_1, aggregate_name, 2, diff.avro_bytes))
            await repository.create(RepositoryEntry(self.uuid_2, aggregate_name, 1, diff.avro_bytes))
            await repository.update(RepositoryEntry(self.uuid_1, aggregate_name, 3, diff.avro_bytes))
            await repository.delete(RepositoryEntry(self.uuid_1, aggregate_name, 4))
            await repository.update(RepositoryEntry(self.uuid_2, aggregate_name, 2, diff.avro_bytes))
            await repository.create(RepositoryEntry(self.uuid_3, aggregate_name, 1, diff.avro_bytes))
            return repository


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import (
    MagicMock,
    patch,
)

from minos.aggregate import (
    DatabaseSnapshotRepository,
    SnapshotEntry,
    SnapshotRepository,
)
from minos.aggregate.testing import (
    SnapshotRepositoryTestCase,
)
from minos.aggregate.testing.snapshot_repository import (
    Car,
)
from minos.common import (
    DatabaseClient,
    NotProvidedException,
    classname,
    current_datetime,
)
from tests.utils import (
    AggregateTestCase,
    FakeAsyncIterator,
)


class TestDatabaseSnapshotRepository(AggregateTestCase, SnapshotRepositoryTestCase):
    __test__ = True

    def test_constructor_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            DatabaseSnapshotRepository(event_repository=None)

        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            DatabaseSnapshotRepository(transaction_repository=None)

    async def test_is_synced(self):
        self.event_repository.select = MagicMock(side_effect=[FakeAsyncIterator([1]), FakeAsyncIterator([])])

        with patch.object(
            DatabaseClient,
            "fetch_one",
            return_value=(0,)
        ):
            self.assertFalse(await self.snapshot_repository.is_synced(Car))
            self.assertTrue(await self.snapshot_repository.is_synced(Car))

    def build_snapshot_repository(self) -> SnapshotRepository:
        return DatabaseSnapshotRepository.from_config(self.config)

    async def synchronize(self):
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                StopAsyncIteration,
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
            ],
        ):
            with patch.object(DatabaseClient, "fetch_all", return_value=FakeAsyncIterator([])):
                await super().synchronize()

    async def test_dispatch(self):
        entries = [
            SnapshotEntry(self.uuid_1, classname(Car), 4, created_at=current_datetime(), updated_at=current_datetime()),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_2,
                    version=2,
                    created_at=current_datetime(),
                    updated_at=current_datetime(),
                )
            ),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_3,
                    version=1,
                    created_at=current_datetime(),
                    updated_at=current_datetime(),
                )
            ),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator([tuple(entry.as_raw().values()) for entry in entries]),
            ):
                await super().test_dispatch()

    async def test_dispatch_first_transaction(self):
        entries = [
            SnapshotEntry(self.uuid_1, classname(Car), 4, created_at=current_datetime(), updated_at=current_datetime()),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_2,
                    version=4,
                    created_at=current_datetime(),
                    updated_at=current_datetime(),
                )
            ),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_3,
                    version=1,
                    created_at=current_datetime(),
                    updated_at=current_datetime(),
                )
            ),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator([tuple(entry.as_raw().values()) for entry in entries]),
            ):
                await super().test_dispatch_first_transaction()

    async def test_dispatch_second_transaction(self):
        entries = [
            SnapshotEntry(self.uuid_1, classname(Car), 4, created_at=current_datetime(), updated_at=current_datetime()),
            SnapshotEntry(self.uuid_2, classname(Car), 4, created_at=current_datetime(), updated_at=current_datetime()),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_3,
                    version=1,
                    created_at=current_datetime(),
                    updated_at=current_datetime(),
                )
            ),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator([tuple(entry.as_raw().values()) for entry in entries]),
            ):
                await super().test_dispatch_second_transaction()

    async def test_dispatch_third_transaction(self):
        entries = [
            SnapshotEntry(self.uuid_1, classname(Car), 4, created_at=current_datetime(), updated_at=current_datetime()),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_2,
                    version=2,
                    created_at=current_datetime(),
                    updated_at=current_datetime(),
                )
            ),
            SnapshotEntry.from_root_entity(
                Car(
                    3,
                    "blue",
                    uuid=self.uuid_3,
                    version=1,
                    created_at=current_datetime(),
                    updated_at=current_datetime(),
                )
            ),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator([tuple(entry.as_raw().values()) for entry in entries]),
            ):
                await super().test_dispatch_third_transaction()

    async def test_dispatch_ignore_previous_version(self):
        entries = [
            SnapshotEntry(
                uuid=self.uuid_1,
                name=classname(Car),
                version=3,
                schema=Car.avro_schema,
                data=Car(3, "blue", uuid=self.uuid_1, version=1).avro_data,
                created_at=current_datetime(),
                updated_at=current_datetime(),
            )
        ]
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                StopAsyncIteration,
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (9999,),
            ],
        ):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                side_effect=[
                    FakeAsyncIterator([]),
                    FakeAsyncIterator([]),
                    FakeAsyncIterator([]),
                    FakeAsyncIterator([tuple(entry.as_raw().values()) for entry in entries]),
                ],
            ):
                await super().test_dispatch_ignore_previous_version()

    async def test_dispatch_with_offset(self):
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                (0,),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (current_datetime(), current_datetime()),
                (11,),
                (current_datetime(), current_datetime()),
                (12,),
                (12,),
            ],
        ):
            await super().test_dispatch_with_offset()

    async def test_find_by_uuid(self):
        entities = [
            Car(3, "blue", uuid=self.uuid_2, version=2),
            Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_root_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_by_uuid()

    async def test_find_with_transaction(self):
        entities = [
            Car(3, "blue", uuid=self.uuid_2, version=4),
            Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_root_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_with_transaction()

    async def test_find_with_transaction_delete(self):
        entities = [Car(3, "blue", uuid=self.uuid_3, version=1)]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_root_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_with_transaction_delete()

    async def test_find_with_transaction_reverted(self):
        entities = [
            Car(3, "blue", uuid=self.uuid_2, version=2),
            Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_root_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_with_transaction_reverted()

    async def test_find_streaming_true(self):
        entities = [
            Car(3, "blue", uuid=self.uuid_2, version=2),
            Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_root_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_streaming_true()

    async def test_find_with_duplicates(self):
        entities = [
            Car(3, "blue", uuid=self.uuid_2, version=2),
            Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_root_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_with_duplicates()

    async def test_find_empty(self):
        entities = []
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_root_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_empty()

    async def test_get(self):
        entities = [
            Car(3, "blue", uuid=self.uuid_2, version=2),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_root_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_get()

    async def test_get_with_transaction(self):
        entities = [Car(3, "blue", uuid=self.uuid_2, version=4)]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_root_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_get_with_transaction()

    async def test_get_raises(self):
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                side_effect=[
                    FakeAsyncIterator([tuple(SnapshotEntry(self.uuid_1, classname(Car), 1).as_raw().values())]),
                    FakeAsyncIterator([]),
                ],
            ):
                await super().test_get_raises()

    async def test_get_with_transaction_raises(self):
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry(self.uuid_1, classname(Car), 1).as_raw().values())]
                ),
            ):
                await super().test_get_with_transaction_raises()

    async def test_find(self):
        entities = [
            Car(3, "blue", uuid=self.uuid_2, version=2),
            Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_root_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find()

    async def test_find_all(self):
        entities = [
            Car(3, "blue", uuid=self.uuid_2, version=2),
            Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_root_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_all()


if __name__ == "__main__":
    unittest.main()

import unittest
from itertools import (
    chain,
    cycle,
)
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
from minos.common import (
    DatabaseClient,
    NotProvidedException,
    ProgrammingException,
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
            DatabaseSnapshotRepository(delta_repository=None)

        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            DatabaseSnapshotRepository(transaction_repository=None)

    async def test_is_synced(self):
        self.delta_repository.select = MagicMock(side_effect=[FakeAsyncIterator([1]), FakeAsyncIterator([])])

        with patch.object(DatabaseClient, "fetch_one", return_value=(0,)):
            self.assertFalse(await self.snapshot_repository.is_synced(SnapshotRepositoryTestCase.Car))
            self.assertTrue(await self.snapshot_repository.is_synced(SnapshotRepositoryTestCase.Car))

    def build_snapshot_repository(self) -> SnapshotRepository:
        return DatabaseSnapshotRepository.from_config(self.config)

    async def synchronize(self):
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                ProgrammingException(""),
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
            with patch.object(
                DatabaseClient,
                "fetch_all",
                side_effect=chain(
                    [
                        FakeAsyncIterator([]),
                        FakeAsyncIterator(
                            [
                                tuple(
                                    SnapshotEntry.from_entity(
                                        SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_1, version=1)
                                    )
                                    .as_raw()
                                    .values()
                                )
                            ]
                        ),
                    ],
                    cycle(
                        [
                            FakeAsyncIterator([]),
                        ]
                    ),
                ),
            ):
                await super().synchronize()

    async def test_dispatch(self):
        entries = [
            SnapshotEntry(
                self.uuid_1,
                classname(SnapshotRepositoryTestCase.Car),
                4,
                created_at=current_datetime(),
                updated_at=current_datetime(),
            ),
            SnapshotEntry.from_entity(
                SnapshotRepositoryTestCase.Car(
                    3,
                    "blue",
                    uuid=self.uuid_2,
                    version=2,
                    created_at=current_datetime(),
                    updated_at=current_datetime(),
                )
            ),
            SnapshotEntry.from_entity(
                SnapshotRepositoryTestCase.Car(
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
            SnapshotEntry(
                self.uuid_1,
                classname(SnapshotRepositoryTestCase.Car),
                4,
                created_at=current_datetime(),
                updated_at=current_datetime(),
            ),
            SnapshotEntry.from_entity(
                SnapshotRepositoryTestCase.Car(
                    3,
                    "blue",
                    uuid=self.uuid_2,
                    version=4,
                    created_at=current_datetime(),
                    updated_at=current_datetime(),
                )
            ),
            SnapshotEntry.from_entity(
                SnapshotRepositoryTestCase.Car(
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
            SnapshotEntry(
                self.uuid_1,
                classname(SnapshotRepositoryTestCase.Car),
                4,
                created_at=current_datetime(),
                updated_at=current_datetime(),
            ),
            SnapshotEntry(
                self.uuid_2,
                classname(SnapshotRepositoryTestCase.Car),
                4,
                created_at=current_datetime(),
                updated_at=current_datetime(),
            ),
            SnapshotEntry.from_entity(
                SnapshotRepositoryTestCase.Car(
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
            SnapshotEntry(
                self.uuid_1,
                classname(SnapshotRepositoryTestCase.Car),
                4,
                created_at=current_datetime(),
                updated_at=current_datetime(),
            ),
            SnapshotEntry.from_entity(
                SnapshotRepositoryTestCase.Car(
                    3,
                    "blue",
                    uuid=self.uuid_2,
                    version=2,
                    created_at=current_datetime(),
                    updated_at=current_datetime(),
                )
            ),
            SnapshotEntry.from_entity(
                SnapshotRepositoryTestCase.Car(
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
                name=classname(SnapshotRepositoryTestCase.Car),
                version=3,
                schema=SnapshotRepositoryTestCase.Car.avro_schema,
                data=SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_1, version=1).avro_data,
                created_at=current_datetime(),
                updated_at=current_datetime(),
            )
        ]
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                ProgrammingException(""),
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
                    FakeAsyncIterator(
                        [
                            tuple(
                                SnapshotEntry.from_entity(
                                    SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_1, version=5)
                                )
                                .as_raw()
                                .values()
                            )
                        ]
                    ),
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
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_2, version=2),
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_by_uuid()

    async def test_find_contains(self):
        entities = [
            SnapshotRepositoryTestCase.NumbersList([1, 2, 3], uuid=self.uuid_2),
            SnapshotRepositoryTestCase.NumbersList([3, 8, 9], uuid=self.uuid_3),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                side_effect=[
                    FakeAsyncIterator(
                        [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                    ),
                    FakeAsyncIterator([tuple(SnapshotEntry.from_entity(entities[0]).as_raw().values())]),
                    FakeAsyncIterator([tuple(SnapshotEntry.from_entity(entities[1]).as_raw().values())]),
                ],
            ):
                await super().test_find_contains()

    async def test_find_equal(self):
        entities = [
            SnapshotRepositoryTestCase.Number(1, uuid=self.uuid_2),
            SnapshotRepositoryTestCase.Number(1, uuid=self.uuid_3),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                side_effect=[
                    FakeAsyncIterator(
                        [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                    ),
                    FakeAsyncIterator([tuple(SnapshotEntry.from_entity(entities[0]).as_raw().values())]),
                    FakeAsyncIterator([tuple(SnapshotEntry.from_entity(entities[1]).as_raw().values())]),
                ],
            ):
                await super().test_find_equal()

    async def test_find_with_transaction(self):
        entities = [
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_2, version=4),
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_with_transaction()

    async def test_find_with_transaction_delete(self):
        entities = [SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_3, version=1)]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_with_transaction_delete()

    async def test_find_with_transaction_reverted(self):
        entities = [
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_2, version=2),
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_with_transaction_reverted()

    async def test_find_streaming_true(self):
        entities = [
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_2, version=2),
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_streaming_true()

    async def test_find_with_duplicates(self):
        entities = [
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_2, version=2),
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
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
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_empty()

    async def test_find_one(self):
        entities = [
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_2, version=2),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_one()

    async def test_find_one_raises(self):
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator([]),
            ):
                await super().test_find_one_raises()

    async def test_get_all(self):
        entities = [
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_1, version=1),
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_2, version=2),
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                side_effect=lambda: FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_get_all()

    async def test_get(self):
        entities = [
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_2, version=2),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_get()

    async def test_get_with_transaction(self):
        entities = [SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_2, version=4)]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_get_with_transaction()

    async def test_get_raises(self):
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                side_effect=[
                    FakeAsyncIterator(
                        [
                            tuple(
                                SnapshotEntry(self.uuid_1, classname(SnapshotRepositoryTestCase.Car), 1)
                                .as_raw()
                                .values()
                            )
                        ]
                    ),
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
                    [tuple(SnapshotEntry(self.uuid_1, classname(SnapshotRepositoryTestCase.Car), 1).as_raw().values())]
                ),
            ):
                await super().test_get_with_transaction_raises()

    async def test_find(self):
        entities = [
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_2, version=2),
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find()

    async def test_find_all(self):
        entities = [
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_2, version=2),
            SnapshotRepositoryTestCase.Car(3, "blue", uuid=self.uuid_3, version=1),
        ]
        with patch.object(DatabaseClient, "fetch_one", return_value=(9999,)):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator(
                    [tuple(SnapshotEntry.from_entity(entity).as_raw().values()) for entity in entities]
                ),
            ):
                await super().test_find_all()


if __name__ == "__main__":
    unittest.main()

import unittest
from datetime import (
    datetime,
    timezone,
)
from unittest.mock import (
    patch,
)
from uuid import (
    uuid4,
)

from minos.common import (
    NULL_UUID,
    Action,
    FieldDiffContainer,
    InMemoryRepository,
    MinosBrokerNotProvidedException,
    MinosRepository,
    MinosRepositoryException,
    RepositoryEntry,
)
from tests.utils import (
    FakeBroker,
    TestRepositorySelect,
)


class TestInMemoryRepository(TestRepositorySelect):
    def setUp(self) -> None:
        self.uuid = uuid4()
        self.broker = FakeBroker()
        self.field_diff_container_patcher = patch(
            "minos.common.FieldDiffContainer.from_avro_bytes", return_value=FieldDiffContainer.empty()
        )
        self.field_diff_container_patcher.start()

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.repository = InMemoryRepository(self.broker)
        await self.repository.setup()

    async def asyncTearDown(self) -> None:
        await self.repository.destroy()
        await super().asyncTearDown()

    def tearDown(self):
        self.field_diff_container_patcher.stop()

    def test_constructor(self):
        repository = InMemoryRepository(self.broker)
        self.assertIsInstance(repository, MinosRepository)

    async def test_constructor_raises(self):
        with self.assertRaises(MinosBrokerNotProvidedException):
            InMemoryRepository()

    async def test_create(self):
        await self.repository.create(RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8")))
        expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE)]
        observed = [v async for v in self.repository.select()]
        self.assert_equal_repository_entries(expected, observed)

    async def test_update(self):
        await self.repository.update(RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8")))
        expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.UPDATE)]
        observed = [v async for v in self.repository.select()]
        self.assert_equal_repository_entries(expected, observed)

    async def test_delete(self):
        await self.repository.delete(RepositoryEntry(self.uuid, "example.Car", 1, bytes()))
        expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes(), 1, Action.DELETE)]
        observed = [v async for v in self.repository.select()]
        self.assert_equal_repository_entries(expected, observed)

    async def test_submit(self):
        await self.repository.submit(RepositoryEntry(self.uuid, "example.Car", action=Action.CREATE))
        expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes(), 1, Action.CREATE)]
        observed = [v async for v in self.repository.select()]
        self.assert_equal_repository_entries(expected, observed)

    async def test_submit_with_version(self):
        await self.repository.submit(RepositoryEntry(self.uuid, "example.Car", version=3, action=Action.CREATE))
        expected = [RepositoryEntry(self.uuid, "example.Car", 3, bytes(), 1, Action.CREATE)]
        observed = [v async for v in self.repository.select()]
        self.assert_equal_repository_entries(expected, observed)

    async def test_submit_with_created_at(self):
        created_at = datetime(2021, 10, 25, 8, 30, tzinfo=timezone.utc)
        await self.repository.submit(
            RepositoryEntry(self.uuid, "example.Car", created_at=created_at, action=Action.CREATE)
        )
        expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes(), 1, Action.CREATE, created_at=created_at)]
        observed = [v async for v in self.repository.select()]
        self.assert_equal_repository_entries(expected, observed)

    async def test_submit_raises(self):
        with self.assertRaises(MinosRepositoryException):
            await self.repository.submit(RepositoryEntry(self.uuid, "example.Car", 1, "foo".encode()))

    async def test_select_empty(self):
        self.assertEqual([], [v async for v in self.repository.select()])


class TestInMemoryRepositorySelect(TestRepositorySelect):
    def setUp(self) -> None:
        super().setUp()
        self.uuid = uuid4()
        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_4 = uuid4()

        self.first_transaction = uuid4()
        self.second_transaction = uuid4()

        self.broker = FakeBroker()
        self.field_diff_container_patcher = patch(
            "minos.common.FieldDiffContainer.from_avro_bytes", return_value=FieldDiffContainer.empty()
        )
        self.field_diff_container_patcher.start()

        self.entries = [
            RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE),
            RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
            RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
            RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
            RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
            RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
            RepositoryEntry(self.uuid_4, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
            RepositoryEntry(
                self.uuid_2,
                "example.Car",
                3,
                bytes("hola", "utf-8"),
                8,
                Action.UPDATE,
                transaction_uuid=self.first_transaction,
            ),
            RepositoryEntry(
                self.uuid_2,
                "example.Car",
                3,
                bytes("salut", "utf-8"),
                9,
                Action.UPDATE,
                transaction_uuid=self.second_transaction,
            ),
            RepositoryEntry(
                self.uuid_2,
                "example.Car",
                4,
                bytes("adios", "utf-8"),
                10,
                Action.UPDATE,
                transaction_uuid=self.first_transaction,
            ),
        ]

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.repository = await self._build_repository()

    async def _build_repository(self):
        repository = InMemoryRepository(self.broker)
        await repository.setup()
        await repository.create(RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8")))
        await repository.update(RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8")))
        await repository.create(RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8")))
        await repository.update(RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8")))
        await repository.delete(RepositoryEntry(self.uuid_1, "example.Car", 4))
        await repository.update(RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8")))
        await repository.create(RepositoryEntry(self.uuid_4, "example.MotorCycle", 1, bytes("one", "utf-8")))
        await repository.update(
            RepositoryEntry(
                self.uuid_2, "example.Car", 3, bytes("hola", "utf-8"), transaction_uuid=self.first_transaction
            )
        )
        await repository.update(
            RepositoryEntry(
                self.uuid_2, "example.Car", 3, bytes("salut", "utf-8"), transaction_uuid=self.second_transaction
            )
        )
        await repository.update(
            RepositoryEntry(
                self.uuid_2, "example.Car", 4, bytes("adios", "utf-8"), transaction_uuid=self.first_transaction
            )
        )
        return repository

    async def asyncTearDown(self):
        await self.repository.destroy()
        await super().asyncTearDown()

    def tearDown(self):
        self.field_diff_container_patcher.stop()
        super().tearDown()

    async def test_select(self):
        expected = self.entries
        observed = [v async for v in self.repository.select()]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_id(self):
        expected = [self.entries[1]]
        observed = [v async for v in self.repository.select(id=2)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_id_lt(self):
        expected = self.entries[:4]
        observed = [v async for v in self.repository.select(id_lt=5)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_id_gt(self):
        expected = self.entries[4:]
        observed = [v async for v in self.repository.select(id_gt=4)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_id_le(self):
        expected = self.entries[:4]
        observed = [v async for v in self.repository.select(id_le=4)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_id_ge(self):
        expected = self.entries[4:]
        observed = [v async for v in self.repository.select(id_ge=5)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_aggregate_uuid(self):
        expected = [self.entries[2], self.entries[5], self.entries[7], self.entries[8], self.entries[9]]
        observed = [v async for v in self.repository.select(aggregate_uuid=self.uuid_2)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_aggregate_name(self):
        expected = [self.entries[6]]
        observed = [v async for v in self.repository.select(aggregate_name="example.MotorCycle")]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_version(self):
        expected = [self.entries[4], self.entries[9]]
        observed = [v async for v in self.repository.select(version=4)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_version_lt(self):
        expected = [self.entries[0], self.entries[2], self.entries[6]]
        observed = [v async for v in self.repository.select(version_lt=2)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_version_gt(self):
        expected = [
            self.entries[1],
            self.entries[3],
            self.entries[4],
            self.entries[5],
            self.entries[7],
            self.entries[8],
            self.entries[9],
        ]
        observed = [v async for v in self.repository.select(version_gt=1)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_version_le(self):
        expected = [self.entries[0], self.entries[2], self.entries[6]]
        observed = [v async for v in self.repository.select(version_le=1)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_version_ge(self):
        expected = [
            self.entries[1],
            self.entries[3],
            self.entries[4],
            self.entries[5],
            self.entries[7],
            self.entries[8],
            self.entries[9],
        ]
        observed = [v async for v in self.repository.select(version_ge=2)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_transaction_uuid_null(self):
        expected = self.entries[:7]
        observed = [v async for v in self.repository.select(transaction_uuid=NULL_UUID)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_transaction_uuid(self):
        expected = [self.entries[7], self.entries[9]]
        observed = [v async for v in self.repository.select(transaction_uuid=self.first_transaction)]
        self.assert_equal_repository_entries(expected, observed)

    async def test_select_combined(self):
        expected = [self.entries[2], self.entries[5], self.entries[7], self.entries[8], self.entries[9]]
        observed = [v async for v in self.repository.select(aggregate_name="example.Car", aggregate_uuid=self.uuid_2)]
        self.assert_equal_repository_entries(expected, observed)


if __name__ == "__main__":
    unittest.main()

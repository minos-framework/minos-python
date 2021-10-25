import unittest
from unittest.mock import (
    patch,
)
from uuid import (
    uuid4,
)

from minos.common import (
    Action,
    FieldDiffContainer,
    InMemoryRepository,
    MinosBrokerNotProvidedException,
    MinosRepository,
    RepositoryEntry,
)
from tests.utils import (
    FakeBroker,
    assert_equal_repository_entries,
)


class TestInMemoryRepository(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.uuid = uuid4()
        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_4 = uuid4()
        self.broker = FakeBroker()
        self.field_diff_container_patcher = patch(
            "minos.common.FieldDiffContainer.from_avro_bytes", return_value=FieldDiffContainer.empty()
        )
        self.field_diff_container_patcher.start()

    def tearDown(self):
        self.field_diff_container_patcher.stop()

    def test_constructor(self):
        repository = InMemoryRepository(self.broker)
        self.assertIsInstance(repository, MinosRepository)

    async def test_constructor_raises(self):
        with self.assertRaises(MinosBrokerNotProvidedException):
            InMemoryRepository()

    async def test_create(self):
        async with InMemoryRepository(self.broker) as repository:
            await repository.create(RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8")))
            expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE)]
            observed = [v async for v in repository.select()]
            assert_equal_repository_entries(self, expected, observed)

    async def test_update(self):
        async with InMemoryRepository(self.broker) as repository:
            await repository.update(RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8")))
            expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.UPDATE)]
            observed = [v async for v in repository.select()]
            assert_equal_repository_entries(self, expected, observed)

    async def test_delete(self):
        async with InMemoryRepository(self.broker) as repository:
            await repository.delete(RepositoryEntry(self.uuid, "example.Car", 1, bytes()))
            expected = [RepositoryEntry(self.uuid, "example.Car", 1, bytes(), 1, Action.DELETE)]
            observed = [v async for v in repository.select()]
            assert_equal_repository_entries(self, expected, observed)

    async def test_select(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE),
            RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
            RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
            RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
            RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
            RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
            RepositoryEntry(self.uuid_4, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
        ]
        observed = [v async for v in repository.select()]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_empty(self):
        repository = InMemoryRepository(self.broker)
        self.assertEqual([], [v async for v in repository.select()])

    async def test_select_id(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
        ]
        observed = [v async for v in repository.select(id=2)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_id_lt(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE),
            RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
            RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
            RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
        ]
        observed = [v async for v in repository.select(id_lt=5)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_id_gt(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
            RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
            RepositoryEntry(self.uuid_4, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
        ]
        observed = [v async for v in repository.select(id_gt=4)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_id_le(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE),
            RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
            RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
            RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
        ]
        observed = [v async for v in repository.select(id_le=4)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_id_ge(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
            RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
            RepositoryEntry(self.uuid_4, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
        ]
        observed = [v async for v in repository.select(id_ge=5)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_aggregate_uuid(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
            RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
        ]
        observed = [v async for v in repository.select(aggregate_uuid=self.uuid_2)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_aggregate_name(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_4, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
        ]
        observed = [v async for v in repository.select(aggregate_name="example.MotorCycle")]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_version(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
        ]
        observed = [v async for v in repository.select(version=4)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_version_lt(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE),
            RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
            RepositoryEntry(self.uuid_4, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
        ]
        observed = [v async for v in repository.select(version_lt=2)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_version_gt(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
            RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
            RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
            RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
        ]
        observed = [v async for v in repository.select(version_gt=1)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_version_le(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8"), 1, Action.CREATE),
            RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8"), 3, Action.CREATE),
            RepositoryEntry(self.uuid_4, "example.MotorCycle", 1, bytes("one", "utf-8"), 7, Action.CREATE),
        ]
        observed = [v async for v in repository.select(version_le=1)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_version_ge(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8"), 2, Action.UPDATE),
            RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
            RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
            RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
        ]
        observed = [v async for v in repository.select(version_ge=2)]
        assert_equal_repository_entries(self, expected, observed)

    async def test_select_combine(self):
        repository = await self._build_repository()
        expected = [
            RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8"), 4, Action.UPDATE),
            RepositoryEntry(self.uuid_1, "example.Car", 4, bytes(), 5, Action.DELETE),
            RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8"), 6, Action.UPDATE),
        ]
        observed = [v async for v in repository.select(aggregate_name="example.Car", id_ge=4)]
        assert_equal_repository_entries(self, expected, observed)

    async def _build_repository(self):
        repository = InMemoryRepository(self.broker)
        await repository.create(RepositoryEntry(self.uuid_1, "example.Car", 1, bytes("foo", "utf-8")))
        await repository.update(RepositoryEntry(self.uuid_1, "example.Car", 2, bytes("bar", "utf-8")))
        await repository.create(RepositoryEntry(self.uuid_2, "example.Car", 1, bytes("hello", "utf-8")))
        await repository.update(RepositoryEntry(self.uuid_1, "example.Car", 3, bytes("foobar", "utf-8")))
        await repository.delete(RepositoryEntry(self.uuid_1, "example.Car", 4))
        await repository.update(RepositoryEntry(self.uuid_2, "example.Car", 2, bytes("bye", "utf-8")))
        await repository.create(RepositoryEntry(self.uuid_4, "example.MotorCycle", 1, bytes("one", "utf-8")))

        return repository


if __name__ == "__main__":
    unittest.main()

import unittest
from abc import (
    ABC,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.common import (
    Action,
    MinosRepository,
    MinosSetup,
    RepositoryEntry,
)
from tests.utils import (
    FakeLock,
    FakeRepository,
    MinosTestCase,
)


class TestMinosRepository(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.repository = FakeRepository()

    def test_subclass(self):
        self.assertTrue(issubclass(MinosRepository, (ABC, MinosSetup)))

    def test_abstract(self):
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_submit", "_select", "_offset"}, MinosRepository.__abstractmethods__)

    def test_constructor(self):
        pass

    def test_transaction(self):
        pass

    def test_check_transaction(self):
        pass

    def test_commit_transaction(self):
        pass

    async def test_create(self):
        mock = AsyncMock(side_effect=lambda x: x)
        self.repository.submit = mock

        entry = RepositoryEntry(uuid4(), "example.Car", 0, bytes())

        self.assertEqual(entry, await self.repository.create(entry))

        self.assertEqual(Action.CREATE, entry.action)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(entry), mock.call_args)

    async def test_update(self):
        mock = AsyncMock(side_effect=lambda x: x)
        self.repository.submit = mock

        entry = RepositoryEntry(uuid4(), "example.Car", 0, bytes())

        self.assertEqual(entry, await self.repository.update(entry))

        self.assertEqual(Action.UPDATE, entry.action)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(entry), mock.call_args)

    async def test_delete(self):
        mock = AsyncMock(side_effect=lambda x: x)
        self.repository.submit = mock

        entry = RepositoryEntry(uuid4(), "example.Car", 0, bytes())

        self.assertEqual(entry, await self.repository.delete(entry))

        self.assertEqual(Action.DELETE, entry.action)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(entry), mock.call_args)

    def test_submit(self):
        pass

    def test_write_lock(self):
        expected = FakeLock()
        mock = MagicMock(return_value=expected)

        self.lock_pool.acquire = mock

        self.assertEqual(expected, self.repository.write_lock())
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call("aggregate_event_write_lock"), mock.call_args)

    def test_validate(self):
        pass

    def test_select(self):
        pass

    def test_offset(self):
        pass


if __name__ == "__main__":
    unittest.main()

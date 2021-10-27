import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    InMemoryTransactionRepository,
    Transaction,
    TransactionRepository,
    TransactionStatus,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
)


class TestInMemoryTransactionRepository(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.uuid = uuid4()

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.repository = InMemoryTransactionRepository(**self.repository_db)
        await self.repository.setup()

    async def asyncTearDown(self) -> None:
        await self.repository.destroy()
        await super().asyncTearDown()

    async def test_subclass(self) -> None:
        self.assertTrue(issubclass(InMemoryTransactionRepository, TransactionRepository))

    async def test_submit(self):
        await self.repository.submit(Transaction(self.uuid, TransactionStatus.CREATED, 34))
        expected = [Transaction(self.uuid, TransactionStatus.CREATED, 34)]
        observed = [v async for v in self.repository.select()]
        self.assertEqual(expected, observed)

    async def test_select_empty(self):
        expected = []
        observed = [v async for v in self.repository.select()]
        self.assertEqual(expected, observed)


class TestInMemoryTransactionRepositorySelect(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_3 = uuid4()
        self.uuid_4 = uuid4()

        self.entries = [
            Transaction(self.uuid_1, TransactionStatus.CREATED, 12),
            Transaction(self.uuid_2, TransactionStatus.PENDING, 15),
            Transaction(self.uuid_3, TransactionStatus.REJECTED, 16),
            Transaction(self.uuid_4, TransactionStatus.COMMITTED, 20),
        ]

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.repository = await self._build_repository()

    async def _build_repository(self):
        repository = InMemoryTransactionRepository(**self.repository_db)
        await repository.setup()
        await repository.submit(Transaction(self.uuid_1, TransactionStatus.CREATED, 12))
        await repository.submit(Transaction(self.uuid_2, TransactionStatus.PENDING, 15))
        await repository.submit(Transaction(self.uuid_3, TransactionStatus.REJECTED, 16))
        await repository.submit(Transaction(self.uuid_4, TransactionStatus.COMMITTED, 20))
        return repository

    async def asyncTearDown(self):
        await self.repository.destroy()
        await super().asyncTearDown()

    async def test_select(self):
        expected = self.entries
        observed = [v async for v in self.repository.select()]
        self.assertEqual(expected, observed)

    async def test_select_uuid(self):
        expected = [self.entries[1]]
        observed = [v async for v in self.repository.select(uuid=self.uuid_2)]
        self.assertEqual(expected, observed)

    async def test_select_uuid_in(self):
        expected = [self.entries[1], self.entries[2]]
        observed = [v async for v in self.repository.select(uuid_in=(self.uuid_2, self.uuid_3))]
        self.assertEqual(expected, observed)

    async def test_select_status(self):
        expected = [self.entries[0]]
        observed = [v async for v in self.repository.select(status=TransactionStatus.CREATED)]
        self.assertEqual(expected, observed)

    async def test_select_status_in(self):
        expected = [self.entries[2], self.entries[3]]
        observed = [
            v async for v in self.repository.select(status_in=(TransactionStatus.COMMITTED, TransactionStatus.REJECTED))
        ]
        self.assertEqual(expected, observed)

    async def test_select_event_offset(self):
        expected = [self.entries[1]]
        observed = [v async for v in self.repository.select(event_offset=15)]
        self.assertEqual(expected, observed)

    async def test_select_event_offset_lt(self):
        expected = [self.entries[0]]
        observed = [v async for v in self.repository.select(event_offset_lt=15)]
        self.assertEqual(expected, observed)

    async def test_select_event_offset_gt(self):
        expected = [self.entries[2], self.entries[3]]
        observed = [v async for v in self.repository.select(event_offset_gt=15)]
        self.assertEqual(expected, observed)

    async def test_select_event_offset_le(self):
        expected = [self.entries[0], self.entries[1]]
        observed = [v async for v in self.repository.select(event_offset_le=15)]
        self.assertEqual(expected, observed)

    async def test_select_event_offset_ge(self):
        expected = [self.entries[1], self.entries[2], self.entries[3]]
        observed = [v async for v in self.repository.select(event_offset_ge=15)]
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

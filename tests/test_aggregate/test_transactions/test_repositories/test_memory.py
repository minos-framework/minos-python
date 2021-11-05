import unittest
from uuid import (
    uuid4,
)

from minos.aggregate import (
    InMemoryTransactionRepository,
    MinosInvalidTransactionStatusException,
    TransactionEntry,
    TransactionRepository,
    TransactionStatus,
)
from tests.utils import (
    MinosTestCase,
)


class TestInMemoryTransactionRepository(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.uuid = uuid4()

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.event_repository = InMemoryTransactionRepository()
        await self.event_repository.setup()

    async def asyncTearDown(self) -> None:
        await self.event_repository.destroy()
        await super().asyncTearDown()

    async def test_subclass(self) -> None:
        self.assertTrue(issubclass(InMemoryTransactionRepository, TransactionRepository))

    async def test_submit(self):
        await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        expected = [TransactionEntry(self.uuid, TransactionStatus.PENDING, 34)]
        observed = [v async for v in self.event_repository.select()]
        self.assertEqual(expected, observed)

    async def test_submit_pending_raises(self):
        await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTED, 34))

    async def test_submit_reserved_raises(self):
        await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVED, 34))

    async def test_submit_committed_raises(self):
        await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.REJECTED, 34))

    async def test_submit_rejected_raises(self):
        await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.REJECTED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.event_repository.submit(TransactionEntry(self.uuid, TransactionStatus.REJECTED, 34))

    async def test_select_empty(self):
        expected = []
        observed = [v async for v in self.event_repository.select()]
        self.assertEqual(expected, observed)


class TestInMemoryTransactionRepositorySelect(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_3 = uuid4()
        self.uuid_4 = uuid4()
        self.uuid_5 = uuid4()

        self.transaction_repository = InMemoryTransactionRepository()

        self.entries = [
            TransactionEntry(self.uuid_1, TransactionStatus.PENDING, 12),
            TransactionEntry(self.uuid_2, TransactionStatus.PENDING, 15),
            TransactionEntry(self.uuid_3, TransactionStatus.REJECTED, 16),
            TransactionEntry(self.uuid_4, TransactionStatus.COMMITTED, 20),
            TransactionEntry(self.uuid_5, TransactionStatus.PENDING, 20, self.uuid_1),
        ]

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self._populate()

    async def _populate(self):
        await self.transaction_repository.setup()
        await self.transaction_repository.submit(TransactionEntry(self.uuid_1, TransactionStatus.PENDING, 12))
        await self.transaction_repository.submit(TransactionEntry(self.uuid_2, TransactionStatus.PENDING, 15))
        await self.transaction_repository.submit(TransactionEntry(self.uuid_3, TransactionStatus.REJECTED, 16))
        await self.transaction_repository.submit(TransactionEntry(self.uuid_4, TransactionStatus.COMMITTED, 20))
        await self.transaction_repository.submit(
            TransactionEntry(self.uuid_5, TransactionStatus.PENDING, 20, self.uuid_1)
        )

    async def asyncTearDown(self):
        await self.transaction_repository.destroy()
        await super().asyncTearDown()

    async def test_select(self):
        expected = self.entries
        observed = [v async for v in self.transaction_repository.select()]
        self.assertEqual(expected, observed)

    async def test_select_uuid(self):
        expected = [self.entries[1]]
        observed = [v async for v in self.transaction_repository.select(uuid=self.uuid_2)]
        self.assertEqual(expected, observed)

    async def test_select_uuid_ne(self):
        expected = [self.entries[0], self.entries[2], self.entries[3], self.entries[4]]
        observed = [v async for v in self.transaction_repository.select(uuid_ne=self.uuid_2)]
        self.assertEqual(expected, observed)

    async def test_select_uuid_in(self):
        expected = [self.entries[1], self.entries[2]]
        observed = [v async for v in self.transaction_repository.select(uuid_in=(self.uuid_2, self.uuid_3))]
        self.assertEqual(expected, observed)

    async def test_select_destination_uuid(self):
        expected = [self.entries[4]]
        observed = [v async for v in self.transaction_repository.select(destination_uuid=self.uuid_1)]
        self.assertEqual(expected, observed)

    async def test_select_status(self):
        expected = [self.entries[0], self.entries[1], self.entries[4]]
        observed = [v async for v in self.transaction_repository.select(status=TransactionStatus.PENDING)]
        self.assertEqual(expected, observed)

    async def test_select_status_in(self):
        expected = [self.entries[2], self.entries[3]]
        observed = [
            v
            async for v in self.transaction_repository.select(
                status_in=(TransactionStatus.COMMITTED, TransactionStatus.REJECTED)
            )
        ]
        self.assertEqual(expected, observed)

    async def test_select_event_offset(self):
        expected = [self.entries[1]]
        observed = [v async for v in self.transaction_repository.select(event_offset=15)]
        self.assertEqual(expected, observed)

    async def test_select_event_offset_lt(self):
        expected = [self.entries[0]]
        observed = [v async for v in self.transaction_repository.select(event_offset_lt=15)]
        self.assertEqual(expected, observed)

    async def test_select_event_offset_gt(self):
        expected = [self.entries[2], self.entries[3], self.entries[4]]
        observed = [v async for v in self.transaction_repository.select(event_offset_gt=15)]
        self.assertEqual(expected, observed)

    async def test_select_event_offset_le(self):
        expected = [self.entries[0], self.entries[1]]
        observed = [v async for v in self.transaction_repository.select(event_offset_le=15)]
        self.assertEqual(expected, observed)

    async def test_select_event_offset_ge(self):
        expected = [self.entries[1], self.entries[2], self.entries[3], self.entries[4]]
        observed = [v async for v in self.transaction_repository.select(event_offset_ge=15)]
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

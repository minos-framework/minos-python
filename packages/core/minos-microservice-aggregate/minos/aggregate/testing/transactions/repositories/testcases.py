from abc import (
    ABC,
    abstractmethod,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    TransactionEntry,
    TransactionRepository,
    TransactionRepositoryConflictException,
    TransactionStatus,
)
from minos.common.testing import (
    MinosTestCase,
)


class TransactionRepositoryTestCase(MinosTestCase, ABC):
    __test__ = False

    def setUp(self) -> None:
        super().setUp()

        self.transaction_repository = self.build_transaction_repository()

        self.uuid = uuid4()
        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_3 = uuid4()
        self.uuid_4 = uuid4()
        self.uuid_5 = uuid4()

        self.entries = [
            TransactionEntry(self.uuid_1, TransactionStatus.PENDING, 12),
            TransactionEntry(self.uuid_2, TransactionStatus.PENDING, 15),
            TransactionEntry(self.uuid_3, TransactionStatus.REJECTED, 16),
            TransactionEntry(self.uuid_4, TransactionStatus.COMMITTED, 20),
            TransactionEntry(self.uuid_5, TransactionStatus.PENDING, 20, self.uuid_1),
        ]

    async def populate(self) -> None:
        await self.transaction_repository.submit(TransactionEntry(self.uuid_1, TransactionStatus.PENDING, 12))
        await self.transaction_repository.submit(TransactionEntry(self.uuid_2, TransactionStatus.PENDING, 15))
        await self.transaction_repository.submit(TransactionEntry(self.uuid_3, TransactionStatus.REJECTED, 16))
        await self.transaction_repository.submit(TransactionEntry(self.uuid_4, TransactionStatus.COMMITTED, 20))
        await self.transaction_repository.submit(
            TransactionEntry(self.uuid_5, TransactionStatus.PENDING, 20, self.uuid_1)
        )

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await self.transaction_repository.setup()

    async def asyncTearDown(self):
        await self.transaction_repository.destroy()
        await super().asyncTearDown()

    def tearDown(self):
        super().tearDown()

    @abstractmethod
    def build_transaction_repository(self) -> TransactionRepository:
        """For testing purposes."""

    async def test_subclass(self) -> None:
        self.assertTrue(isinstance(self.transaction_repository, TransactionRepository))

    async def test_submit(self):
        await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        expected = [TransactionEntry(self.uuid, TransactionStatus.PENDING, 34)]
        observed = [v async for v in self.transaction_repository.select()]
        self.assertEqual(expected, observed)

    async def test_submit_pending_raises(self):
        await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVED, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTED, 34))

    async def test_submit_reserving_raises(self):
        await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTED, 34))

    async def test_submit_reserved_raises(self):
        await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVED, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVED, 34))

    async def test_submit_committing_raises(self):
        await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTED, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVED, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.REJECTED, 34))

    async def test_submit_committed_raises(self):
        await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTED, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVED, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTED, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.REJECTED, 34))

    async def test_submit_rejected_raises(self):
        await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.REJECTED, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.RESERVED, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTING, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.COMMITTED, 34))
        with self.assertRaises(TransactionRepositoryConflictException):
            await self.transaction_repository.submit(TransactionEntry(self.uuid, TransactionStatus.REJECTED, 34))

    async def test_select_empty(self):
        expected = []
        observed = [v async for v in self.transaction_repository.select()]
        self.assertEqual(expected, observed)

    async def test_select(self):
        await self.populate()
        expected = self.entries
        observed = [v async for v in self.transaction_repository.select()]
        self.assertEqual(expected, observed)

    async def test_select_uuid(self):
        await self.populate()
        expected = [self.entries[1]]
        observed = [v async for v in self.transaction_repository.select(uuid=self.uuid_2)]
        self.assertEqual(expected, observed)

    async def test_select_uuid_ne(self):
        await self.populate()
        expected = [self.entries[0], self.entries[2], self.entries[3], self.entries[4]]
        observed = [v async for v in self.transaction_repository.select(uuid_ne=self.uuid_2)]
        self.assertEqual(expected, observed)

    async def test_select_uuid_in(self):
        await self.populate()
        expected = [self.entries[1], self.entries[2]]
        observed = [v async for v in self.transaction_repository.select(uuid_in=(self.uuid_2, self.uuid_3))]
        self.assertEqual(expected, observed)

    async def test_select_destination_uuid(self):
        await self.populate()
        expected = [self.entries[4]]
        observed = [v async for v in self.transaction_repository.select(destination_uuid=self.uuid_1)]
        self.assertEqual(expected, observed)

    async def test_select_status(self):
        await self.populate()
        expected = [self.entries[0], self.entries[1], self.entries[4]]
        observed = [v async for v in self.transaction_repository.select(status=TransactionStatus.PENDING)]
        self.assertEqual(expected, observed)

    async def test_select_status_in(self):
        await self.populate()
        expected = [self.entries[2], self.entries[3]]
        observed = [
            v
            async for v in self.transaction_repository.select(
                status_in=(TransactionStatus.COMMITTED, TransactionStatus.REJECTED)
            )
        ]
        self.assertEqual(expected, observed)

    async def test_select_event_offset(self):
        await self.populate()
        expected = [self.entries[1]]
        observed = [v async for v in self.transaction_repository.select(event_offset=15)]
        self.assertEqual(expected, observed)

    async def test_select_event_offset_lt(self):
        await self.populate()
        expected = [self.entries[0]]
        observed = [v async for v in self.transaction_repository.select(event_offset_lt=15)]
        self.assertEqual(expected, observed)

    async def test_select_event_offset_gt(self):
        await self.populate()
        expected = [self.entries[2], self.entries[3], self.entries[4]]
        observed = [v async for v in self.transaction_repository.select(event_offset_gt=15)]
        self.assertEqual(expected, observed)

    async def test_select_event_offset_le(self):
        await self.populate()
        expected = [self.entries[0], self.entries[1]]
        observed = [v async for v in self.transaction_repository.select(event_offset_le=15)]
        self.assertEqual(expected, observed)

    async def test_select_event_offset_ge(self):
        await self.populate()
        expected = [self.entries[1], self.entries[2], self.entries[3], self.entries[4]]
        observed = [v async for v in self.transaction_repository.select(event_offset_ge=15)]
        self.assertEqual(expected, observed)

    async def test_select_updated_at(self):
        await self.populate()
        updated_at = (await self.transaction_repository.get(self.uuid_3)).updated_at

        expected = [self.entries[2]]
        observed = [v async for v in self.transaction_repository.select(updated_at=updated_at)]
        self.assertEqual(expected, observed)

    async def test_select_updated_at_lt(self):
        await self.populate()
        updated_at = (await self.transaction_repository.get(self.uuid_3)).updated_at

        expected = [self.entries[0], self.entries[1]]
        observed = [v async for v in self.transaction_repository.select(updated_at_lt=updated_at)]
        self.assertEqual(expected, observed)

    async def test_select_updated_at_gt(self):
        await self.populate()
        updated_at = (await self.transaction_repository.get(self.uuid_3)).updated_at

        expected = [self.entries[3], self.entries[4]]
        observed = [v async for v in self.transaction_repository.select(updated_at_gt=updated_at)]
        self.assertEqual(expected, observed)

    async def test_select_updated_at_le(self):
        await self.populate()
        updated_at = (await self.transaction_repository.get(self.uuid_3)).updated_at

        expected = [self.entries[0], self.entries[1], self.entries[2]]
        observed = [v async for v in self.transaction_repository.select(updated_at_le=updated_at)]
        self.assertEqual(expected, observed)

    async def test_select_updated_at_ge(self):
        await self.populate()
        updated_at = (await self.transaction_repository.get(self.uuid_3)).updated_at

        expected = [self.entries[2], self.entries[3], self.entries[4]]
        observed = [v async for v in self.transaction_repository.select(updated_at_ge=updated_at)]
        self.assertEqual(expected, observed)

import unittest
from uuid import (
    uuid4,
)

import aiopg

from minos.common import (
    MinosInvalidTransactionStatusException,
    PostgreSqlTransactionRepository,
    Transaction,
    TransactionRepository,
    TransactionStatus,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
    MinosTestCase,
)


class TestPostgreSqlTransactionRepository(MinosTestCase, PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.uuid = uuid4()

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.repository = PostgreSqlTransactionRepository(**self.repository_db)
        await self.repository.setup()

    async def asyncTearDown(self) -> None:
        await self.repository.destroy()
        await super().asyncTearDown()

    async def test_subclass(self) -> None:
        self.assertTrue(issubclass(PostgreSqlTransactionRepository, TransactionRepository))

    def test_constructor(self):
        repository = PostgreSqlTransactionRepository("host", 1234, "database", "user", "password")
        self.assertIsInstance(repository, PostgreSqlTransactionRepository)
        self.assertEqual("host", repository.host)
        self.assertEqual(1234, repository.port)
        self.assertEqual("database", repository.database)
        self.assertEqual("user", repository.user)
        self.assertEqual("password", repository.password)

    def test_from_config(self):
        repository = PostgreSqlTransactionRepository.from_config(self.config)
        self.assertEqual(self.config.repository.host, repository.host)
        self.assertEqual(self.config.repository.port, repository.port)
        self.assertEqual(self.config.repository.database, repository.database)
        self.assertEqual(self.config.repository.user, repository.user)
        self.assertEqual(self.config.repository.password, repository.password)

    async def test_setup(self):
        async with aiopg.connect(**self.repository_db) as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'aggregate_transaction');"
                )
                response = (await cursor.fetchone())[0]
        self.assertTrue(response)

    async def test_submit(self):
        await self.repository.submit(Transaction(self.uuid, TransactionStatus.PENDING, 34))
        expected = [Transaction(self.uuid, TransactionStatus.PENDING, 34)]
        observed = [v async for v in self.repository.select()]
        self.assertEqual(expected, observed)

    async def test_select_empty(self):
        expected = []
        observed = [v async for v in self.repository.select()]
        self.assertEqual(expected, observed)

    async def test_submit_pending_raises(self):
        await self.repository.submit(Transaction(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.repository.submit(Transaction(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.repository.submit(Transaction(self.uuid, TransactionStatus.COMMITTED, 34))

    async def test_submit_reserved_raises(self):
        await self.repository.submit(Transaction(self.uuid, TransactionStatus.RESERVED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.repository.submit(Transaction(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.repository.submit(Transaction(self.uuid, TransactionStatus.RESERVED, 34))

    async def test_submit_committed_raises(self):
        await self.repository.submit(Transaction(self.uuid, TransactionStatus.COMMITTED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.repository.submit(Transaction(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.repository.submit(Transaction(self.uuid, TransactionStatus.RESERVED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.repository.submit(Transaction(self.uuid, TransactionStatus.COMMITTED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.repository.submit(Transaction(self.uuid, TransactionStatus.REJECTED, 34))

    async def test_submit_rejected_raises(self):
        await self.repository.submit(Transaction(self.uuid, TransactionStatus.REJECTED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.repository.submit(Transaction(self.uuid, TransactionStatus.PENDING, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.repository.submit(Transaction(self.uuid, TransactionStatus.RESERVED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.repository.submit(Transaction(self.uuid, TransactionStatus.COMMITTED, 34))
        with self.assertRaises(MinosInvalidTransactionStatusException):
            await self.repository.submit(Transaction(self.uuid, TransactionStatus.REJECTED, 34))


class TestPostgreSqlTransactionRepositorySelect(MinosTestCase, PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.uuid_1 = uuid4()
        self.uuid_2 = uuid4()
        self.uuid_3 = uuid4()
        self.uuid_4 = uuid4()

        self.entries = [
            Transaction(self.uuid_1, TransactionStatus.PENDING, 12),
            Transaction(self.uuid_2, TransactionStatus.PENDING, 15),
            Transaction(self.uuid_3, TransactionStatus.REJECTED, 16),
            Transaction(self.uuid_4, TransactionStatus.COMMITTED, 20),
        ]

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.repository = await self._build_repository()

    async def _build_repository(self):
        repository = PostgreSqlTransactionRepository(**self.repository_db)
        await repository.setup()
        await repository.submit(Transaction(self.uuid_1, TransactionStatus.PENDING, 12))
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

    async def test_select_uuid_ne(self):
        expected = [self.entries[0], self.entries[2], self.entries[3]]
        observed = [v async for v in self.repository.select(uuid_ne=self.uuid_2)]
        self.assertEqual(expected, observed)

    async def test_select_uuid_in(self):
        expected = [self.entries[1], self.entries[2]]
        observed = [v async for v in self.repository.select(uuid_in=(self.uuid_2, self.uuid_3))]
        self.assertEqual(expected, observed)

    async def test_select_status(self):
        expected = [self.entries[0], self.entries[1]]
        observed = [v async for v in self.repository.select(status=TransactionStatus.PENDING)]
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

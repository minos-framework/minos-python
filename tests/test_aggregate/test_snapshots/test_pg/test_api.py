import unittest
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    Condition,
    Ordering,
    PostgreSqlSnapshotReader,
    PostgreSqlSnapshotRepository,
    PostgreSqlSnapshotWriter,
    TransactionEntry,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
    FakeAsyncIterator,
    MinosTestCase,
)


class TestPostgreSqlSnapshotRepository(MinosTestCase, PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.snapshot_repository = PostgreSqlSnapshotRepository.from_config(
            self.config, event_repository=self.event_repository, transaction_repository=self.transaction_repository
        )

        self.dispatch_mock = AsyncMock()
        self.get_mock = AsyncMock(return_value=1)
        self.find_mock = MagicMock(return_value=FakeAsyncIterator(range(5)))
        self.snapshot_repository.reader.get = self.get_mock
        self.snapshot_repository.reader.find = self.find_mock
        self.snapshot_repository.writer.dispatch = self.dispatch_mock

    def test_from_config(self):
        self.assertIsInstance(self.snapshot_repository.reader, PostgreSqlSnapshotReader)
        self.assertIsInstance(self.snapshot_repository.writer, PostgreSqlSnapshotWriter)

    async def test_get(self):
        transaction = TransactionEntry()
        uuid = uuid4()
        observed = await self.snapshot_repository.get("path.to.Aggregate", uuid, transaction)
        self.assertEqual(1, observed)

        self.assertEqual(1, self.dispatch_mock.call_count)
        self.assertEqual(call(), self.dispatch_mock.call_args)

        self.assertEqual(1, self.get_mock.call_count)
        args = call(aggregate_name="path.to.Aggregate", uuid=uuid, transaction=transaction)
        self.assertEqual(args, self.get_mock.call_args)

    async def test_find(self):
        transaction = TransactionEntry()
        iterable = self.snapshot_repository.find(
            "path.to.Aggregate", Condition.TRUE, Ordering.ASC("name"), 10, True, transaction
        )
        observed = [a async for a in iterable]
        self.assertEqual(list(range(5)), observed)

        self.assertEqual(1, self.dispatch_mock.call_count)
        self.assertEqual(call(), self.dispatch_mock.call_args)

        self.assertEqual(1, self.find_mock.call_count)
        args = call(
            aggregate_name="path.to.Aggregate",
            condition=Condition.TRUE,
            ordering=Ordering.ASC("name"),
            limit=10,
            streaming_mode=True,
            transaction=transaction,
        )
        self.assertEqual(args, self.find_mock.call_args)

    async def test_synchronize(self):
        await self.snapshot_repository.synchronize()

        self.assertEqual(1, self.dispatch_mock.call_count)
        self.assertEqual(call(), self.dispatch_mock.call_args)


if __name__ == "__main__":
    unittest.main()

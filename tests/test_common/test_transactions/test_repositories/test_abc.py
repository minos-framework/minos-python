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
    MinosSetup,
    Transaction,
    TransactionRepository,
    TransactionStatus,
)
from tests.utils import (
    FakeAsyncIterator,
    FakeTransactionRepository,
)


class TestTransactionRepository(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.repository = FakeTransactionRepository()

    def test_abstract(self):
        self.assertTrue(issubclass(TransactionRepository, (ABC, MinosSetup)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_submit", "_select"}, TransactionRepository.__abstractmethods__)

    async def test_submit(self):
        transaction = Transaction()
        mock = AsyncMock()
        self.repository._submit = mock

        await self.repository.submit(transaction)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(transaction), mock.call_args)

    async def test_select(self):
        uuid = uuid4()

        mock = MagicMock(return_value=FakeAsyncIterator(range(5)))
        self.repository._select = mock

        iterable = self.repository.select(
            uuid=uuid, status_in=(TransactionStatus.REJECTED, TransactionStatus.COMMITTED), event_offset_gt=56
        )
        observed = [v async for v in iterable]

        self.assertEqual(list(range(5)), observed)
        self.assertEqual(1, mock.call_count)
        args = call(
            uuid=uuid,
            uuid_in=None,
            status=None,
            status_in=(TransactionStatus.REJECTED, TransactionStatus.COMMITTED),
            event_offset=None,
            event_offset_lt=None,
            event_offset_gt=56,
            event_offset_le=None,
            event_offset_ge=None,
        )
        self.assertEqual(args, mock.call_args)


if __name__ == "__main__":
    unittest.main()

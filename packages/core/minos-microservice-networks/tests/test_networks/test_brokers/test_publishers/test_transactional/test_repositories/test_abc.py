import unittest
from abc import (
    ABC,
)
from typing import (
    AsyncIterator,
    Optional,
)
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    SetupMixin,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisherTransactionEntry,
    BrokerPublisherTransactionRepository,
)
from tests.utils import (
    FakeAsyncIterator,
    NetworksTestCase,
)


class _BrokerPublisherTransactionRepository(BrokerPublisherTransactionRepository):
    """For testing purposes."""

    def _select(self, transaction_uuid: Optional[UUID], **kwargs) -> AsyncIterator[BrokerPublisherTransactionEntry]:
        """For testing purposes."""

    async def _submit(self, entry: BrokerPublisherTransactionEntry) -> None:
        """For testing purposes."""

    async def _delete_batch(self, transaction_uuid: UUID) -> None:
        """For testing purposes."""


class TestBrokerPublisherTransactionRepository(NetworksTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerPublisherTransactionRepository, (ABC, SetupMixin)))

    async def test_select(self):
        repository = _BrokerPublisherTransactionRepository()

        m1 = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))
        m2 = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))

        transaction_uuid = uuid4()
        entries = [
            BrokerPublisherTransactionEntry(m1, transaction_uuid),
            BrokerPublisherTransactionEntry(m2, transaction_uuid),
        ]

        mock = MagicMock(return_value=FakeAsyncIterator(entries))
        repository._select = mock

        observed = [entry async for entry in repository.select()]
        self.assertEqual(entries, observed)

        self.assertEqual([call(transaction_uuid=None)], mock.call_args_list)

    async def test_select_transaction(self):
        repository = _BrokerPublisherTransactionRepository()
        transaction_uuid = uuid4()
        mock = MagicMock(return_value=FakeAsyncIterator([]))
        repository._select = mock

        repository.select(transaction_uuid=transaction_uuid)

        self.assertEqual([call(transaction_uuid=transaction_uuid)], mock.call_args_list)

    async def test_submit(self):
        repository = _BrokerPublisherTransactionRepository()
        transaction_uuid = uuid4()

        mock = AsyncMock()
        repository._submit = mock

        m1 = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))
        entry = BrokerPublisherTransactionEntry(m1, transaction_uuid)
        await repository.submit(entry)

        self.assertEqual([call(entry=entry)], mock.call_args_list)

    async def test_delete_batch(self):
        repository = _BrokerPublisherTransactionRepository()
        transaction_uuid = uuid4()

        mock = AsyncMock()
        repository._delete_batch = mock

        await repository.delete_batch(transaction_uuid)

        self.assertEqual([call(transaction_uuid=transaction_uuid)], mock.call_args_list)


if __name__ == "__main__":
    unittest.main()

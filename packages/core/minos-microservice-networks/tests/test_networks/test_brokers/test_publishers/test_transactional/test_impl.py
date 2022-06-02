import unittest
from unittest.mock import (
    AsyncMock,
    MagicMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.common import (
    NULL_UUID,
    classname,
)
from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisher,
    BrokerPublisherTransactionEntry,
    InMemoryBrokerPublisher,
    InMemoryBrokerPublisherTransactionRepository,
    TransactionalBrokerPublisher,
)
from minos.transactions import (
    TRANSACTION_CONTEXT_VAR,
    TransactionalMixin,
    TransactionEntry,
)
from tests.utils import (
    FakeAsyncIterator,
    NetworksTestCase,
)


class TestTransactionalBrokerPublisher(NetworksTestCase):
    def setUp(self):
        super().setUp()
        self.impl = InMemoryBrokerPublisher()
        self.repository = InMemoryBrokerPublisherTransactionRepository()

    def test_is_subclass(self):
        self.assertTrue(issubclass(TransactionalBrokerPublisher, (BrokerPublisher, TransactionalMixin)))

    def test_constructor(self):
        publisher = TransactionalBrokerPublisher(self.impl, self.repository)
        self.assertEqual(self.impl, publisher.impl)
        self.assertEqual(self.repository, publisher.repository)

    def test_from_config(self):
        publisher = TransactionalBrokerPublisher.from_config(
            self.config,
            repository=classname(InMemoryBrokerPublisherTransactionRepository),
            broker_publisher=self.impl,
        )
        self.assertEqual(self.impl, publisher.impl)
        self.assertIsInstance(publisher.repository, InMemoryBrokerPublisherTransactionRepository)

    async def test_send_outside_transaction(self):
        send_mock = AsyncMock()
        submit_mock = AsyncMock()
        self.impl.send = send_mock
        self.repository.submit = submit_mock

        message = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))

        async with TransactionalBrokerPublisher(self.impl, self.repository) as publisher:
            await publisher.send(message)

        self.assertEqual([call(message)], send_mock.call_args_list)
        self.assertEqual([], submit_mock.call_args_list)

    async def test_send_inside_transaction(self):
        send_mock = AsyncMock()
        submit_mock = AsyncMock()
        self.impl.send = send_mock
        self.repository.submit = submit_mock

        message = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))
        transaction = TransactionEntry()

        async with TransactionalBrokerPublisher(self.impl, self.repository) as publisher:
            token = TRANSACTION_CONTEXT_VAR.set(transaction)
            try:
                await publisher.send(message)
            finally:
                TRANSACTION_CONTEXT_VAR.reset(token)

        self.assertEqual([], send_mock.call_args_list)
        self.assertEqual([call(BrokerPublisherTransactionEntry(message, transaction.uuid))], submit_mock.call_args_list)

    async def test_commit_transaction_without_destination(self):
        m1 = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))
        m2 = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))

        transaction_uuid = uuid4()

        entries = [
            BrokerPublisherTransactionEntry(m1, transaction_uuid),
            BrokerPublisherTransactionEntry(m2, transaction_uuid),
        ]

        send_mock = AsyncMock()
        select_mock = MagicMock(return_value=FakeAsyncIterator(entries))
        delete_mock = AsyncMock()
        self.impl.send = send_mock
        self.repository.select = select_mock
        self.repository.delete_batch = delete_mock

        async with TransactionalBrokerPublisher(self.impl, self.repository) as publisher:
            await publisher.commit_transaction(transaction_uuid, NULL_UUID)

        self.assertEqual([call(m1), call(m2)], send_mock.call_args_list)
        self.assertEqual([call(transaction_uuid=transaction_uuid)], select_mock.call_args_list)
        self.assertEqual([call(transaction_uuid=transaction_uuid)], delete_mock.call_args_list)

    async def test_commit_transaction_with_destination(self):
        m1 = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))
        m2 = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))

        transaction_uuid = uuid4()
        destination_transaction_uuid = uuid4()
        entries = [
            BrokerPublisherTransactionEntry(m1, transaction_uuid),
            BrokerPublisherTransactionEntry(m2, transaction_uuid),
        ]

        send_mock = AsyncMock()
        submit_mock = AsyncMock()
        select_mock = MagicMock(return_value=FakeAsyncIterator(entries))
        delete_mock = AsyncMock()
        self.impl.send = send_mock
        self.repository.submit = submit_mock
        self.repository.select = select_mock
        self.repository.delete_batch = delete_mock

        async with TransactionalBrokerPublisher(self.impl, self.repository) as publisher:
            await publisher.commit_transaction(transaction_uuid, destination_transaction_uuid)

        self.assertEqual([], send_mock.call_args_list)
        self.assertEqual(
            [
                call(BrokerPublisherTransactionEntry(m1, destination_transaction_uuid)),
                call(BrokerPublisherTransactionEntry(m2, destination_transaction_uuid)),
            ],
            submit_mock.call_args_list,
        )
        self.assertEqual([call(transaction_uuid=transaction_uuid)], select_mock.call_args_list)
        self.assertEqual([call(transaction_uuid=transaction_uuid)], delete_mock.call_args_list)

    async def test_reject_transaction(self):
        transaction_uuid = uuid4()

        delete_mock = AsyncMock()
        self.repository.delete_batch = delete_mock

        async with TransactionalBrokerPublisher(self.impl, self.repository) as publisher:
            await publisher.reject_transaction(transaction_uuid)

        self.assertEqual([call(transaction_uuid=transaction_uuid)], delete_mock.call_args_list)


if __name__ == "__main__":
    unittest.main()

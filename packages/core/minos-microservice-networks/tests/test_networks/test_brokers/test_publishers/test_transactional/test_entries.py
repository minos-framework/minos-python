import unittest
from uuid import (
    uuid4,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerPublisherTransactionEntry,
)
from tests.utils import (
    NetworksTestCase,
)


class TestBrokerPublisherTransactionEntry(NetworksTestCase):
    def setUp(self):
        super().setUp()
        self.transaction_uuid = uuid4()
        self.message = BrokerMessageV1("AddFoo", BrokerMessageV1Payload(56))

    def test_is_subclass(self):
        self.assertTrue(issubclass(BrokerPublisherTransactionEntry, object))

    def test_constructor(self):
        entry = BrokerPublisherTransactionEntry(self.message, self.transaction_uuid)
        self.assertEqual(self.message, entry.message)
        self.assertEqual(self.transaction_uuid, entry.transaction_uuid)

    def test_constructor_message_bytes(self):
        entry = BrokerPublisherTransactionEntry(self.message.avro_bytes, self.transaction_uuid)
        self.assertEqual(self.message, entry.message)
        self.assertEqual(self.transaction_uuid, entry.transaction_uuid)

    # noinspection SpellCheckingInspection
    def test_constructor_message_memoryview(self):
        entry = BrokerPublisherTransactionEntry(memoryview(self.message.avro_bytes), self.transaction_uuid)
        self.assertEqual(self.message, entry.message)
        self.assertEqual(self.transaction_uuid, entry.transaction_uuid)

    def test_as_raw(self):
        entry = BrokerPublisherTransactionEntry(self.message, self.transaction_uuid)

        observed = entry.as_raw()
        self.assertEqual({"message", "transaction_uuid"}, observed.keys())
        self.assertEqual(self.message, BrokerMessageV1.from_avro_bytes(observed["message"]))
        self.assertEqual(self.transaction_uuid, observed["transaction_uuid"])

    def test_iter(self):
        entry = BrokerPublisherTransactionEntry(self.message, self.transaction_uuid)
        self.assertEqual((self.message, self.transaction_uuid), tuple(entry))

    def test_equal(self):
        base = BrokerPublisherTransactionEntry(self.message, self.transaction_uuid)

        one = BrokerPublisherTransactionEntry(self.message, self.transaction_uuid)
        self.assertEqual(one, base)

        two = BrokerPublisherTransactionEntry(
            BrokerMessageV1("AddFoo", BrokerMessageV1Payload(42)), self.transaction_uuid
        )
        self.assertNotEqual(two, base)

        three = BrokerPublisherTransactionEntry(self.message, uuid4())
        self.assertNotEqual(three, base)


if __name__ == "__main__":
    unittest.main()

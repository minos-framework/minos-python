import unittest
from uuid import (
    UUID,
    uuid4,
)

from minos.networks import (
    BrokerMessage,
    BrokerMessageStatus,
    BrokerMessageStrategy,
)
from tests.utils import (
    FakeModel,
)


class TestBrokerMessage(unittest.TestCase):
    def setUp(self) -> None:
        self.topic = "FooCreated"
        self.data = [FakeModel("blue"), FakeModel("red")]
        self.identifier = uuid4()
        self.reply_topic = "AddOrderReply"
        self.status = BrokerMessageStatus.SUCCESS
        self.user = uuid4()
        self.strategy = BrokerMessageStrategy.MULTICAST

    def test_constructor_simple(self):
        message = BrokerMessage(self.topic, self.data)
        self.assertEqual(self.topic, message.topic)
        self.assertEqual(self.data, message.data)
        self.assertIsInstance(message.identifier, UUID)
        self.assertEqual(None, message.reply_topic)
        self.assertEqual(None, message.user)
        self.assertEqual(BrokerMessageStatus.SUCCESS, message.status)
        self.assertEqual(BrokerMessageStrategy.UNICAST, message.strategy)

    def test_constructor(self):
        message = BrokerMessage(
            self.topic,
            self.data,
            identifier=self.identifier,
            reply_topic=self.reply_topic,
            user=self.user,
            status=self.status,
            strategy=self.strategy,
        )
        self.assertEqual(self.topic, message.topic)
        self.assertEqual(self.data, message.data)
        self.assertEqual(self.identifier, message.identifier)
        self.assertEqual(self.reply_topic, message.reply_topic)
        self.assertEqual(self.user, message.user)
        self.assertEqual(self.status, message.status)
        self.assertEqual(self.strategy, message.strategy)

    def test_ok(self):
        self.assertTrue(BrokerMessage(self.topic, self.data, status=BrokerMessageStatus.SUCCESS).ok)
        self.assertFalse(BrokerMessage(self.topic, self.data, status=BrokerMessageStatus.ERROR).ok)
        self.assertFalse(BrokerMessage(self.topic, self.data, status=BrokerMessageStatus.SYSTEM_ERROR).ok)

    def test_avro_serialization(self):
        message = BrokerMessage(
            self.topic,
            self.data,
            identifier=self.identifier,
            reply_topic=self.reply_topic,
            user=self.user,
            status=self.status,
            strategy=self.strategy,
        )
        observed = BrokerMessage.from_avro_bytes(message.avro_bytes)
        self.assertEqual(message, observed)


if __name__ == "__main__":
    unittest.main()

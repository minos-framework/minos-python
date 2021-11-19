import unittest
from uuid import (
    uuid4,
)

from minos.networks import (
    BrokerMessage,
    BrokerMessageStatus,
)
from tests.utils import (
    FakeModel,
)


class TestBrokerMessage(unittest.TestCase):
    def setUp(self) -> None:
        self.topic = "FooCreated"
        self.data = [FakeModel("blue"), FakeModel("red")]
        self.saga = uuid4()
        self.reply_topic = "AddOrderReply"
        self.status = BrokerMessageStatus.SUCCESS
        self.user = uuid4()
        self.service_name = "foo"

    def test_constructor_simple(self):
        message = BrokerMessage(self.topic, self.data)
        self.assertEqual(self.topic, message.topic)
        self.assertEqual(self.data, message.data)
        self.assertEqual(None, message.saga)
        self.assertEqual(None, message.reply_topic)
        self.assertEqual(None, message.user)
        self.assertEqual(None, message.status)
        self.assertEqual(None, message.service_name)

    def test_constructor(self):
        message = BrokerMessage(
            self.topic,
            self.data,
            saga=self.saga,
            reply_topic=self.reply_topic,
            user=self.user,
            status=self.status,
            service_name=self.service_name,
        )
        self.assertEqual(self.topic, message.topic)
        self.assertEqual(self.data, message.data)
        self.assertEqual(self.saga, message.saga)
        self.assertEqual(self.reply_topic, message.reply_topic)
        self.assertEqual(self.user, message.user)
        self.assertEqual(self.status, message.status)
        self.assertEqual(self.service_name, message.service_name)

    def test_ok(self):
        self.assertTrue(BrokerMessage(self.topic, self.data, status=BrokerMessageStatus.SUCCESS).ok)
        self.assertFalse(BrokerMessage(self.topic, self.data, status=BrokerMessageStatus.ERROR).ok)
        self.assertFalse(BrokerMessage(self.topic, self.data, status=BrokerMessageStatus.SYSTEM_ERROR).ok)

    def test_avro_serialization(self):
        message = BrokerMessage(
            self.topic,
            self.data,
            saga=self.saga,
            reply_topic=self.reply_topic,
            user=self.user,
            status=self.status,
            service_name=self.service_name,
        )
        observed = BrokerMessage.from_avro_bytes(message.avro_bytes)
        self.assertEqual(message, observed)


if __name__ == "__main__":
    unittest.main()

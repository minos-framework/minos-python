import unittest
from abc import (
    ABC,
)

from minos.common import (
    Model,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageV1,
    BrokerMessageV1Payload,
)


class TestBrokerMessage(unittest.TestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(BrokerMessage, (ABC, Model)))
        expected = {"version", "topic", "identifier", "reply_topic", "content", "headers", "status", "ok"}
        self.assertTrue(expected.issubset(BrokerMessage.__abstractmethods__))

    def test_v1(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        observed = BrokerMessage.from_avro(message.avro_schema, message.avro_data)
        self.assertEqual(message, observed)

    def test_greater_version(self):
        message = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        observed = BrokerMessage.from_avro(message.avro_schema, message.avro_data | {"version": 999999})
        self.assertEqual(message, observed)


if __name__ == "__main__":
    unittest.main()

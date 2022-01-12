import unittest
import warnings
from uuid import (
    UUID,
    uuid4,
)

from minos.networks import (
    BrokerMessage,
    BrokerMessagePayload,
    BrokerMessageStatus,
    BrokerMessageStrategy,
)
from tests.utils import FakeModel


class TestBrokerMessage(unittest.TestCase):
    def setUp(self) -> None:
        self.topic = "FooCreated"
        self.identifier = uuid4()
        self.reply_topic = "AddOrderReply"
        self.strategy = BrokerMessageStrategy.MULTICAST

        self.payload = BrokerMessagePayload([FakeModel("blue"), FakeModel("red")], status=BrokerMessageStatus.SUCCESS)

    def test_constructor_simple(self):
        message = BrokerMessage(self.topic, payload=self.payload)
        self.assertEqual(self.topic, message.topic)
        self.assertIsInstance(message.identifier, UUID)
        self.assertEqual(None, message.reply_topic)
        self.assertEqual(BrokerMessageStrategy.UNICAST, message.strategy)
        self.assertEqual(self.payload, message.payload)

    def test_constructor(self):
        message = BrokerMessage(
            self.topic,
            identifier=self.identifier,
            reply_topic=self.reply_topic,
            strategy=self.strategy,
            payload=self.payload,
        )
        self.assertEqual(self.topic, message.topic)
        self.assertEqual(self.identifier, message.identifier)
        self.assertEqual(self.reply_topic, message.reply_topic)
        self.assertEqual(self.strategy, message.strategy)
        self.assertEqual(self.payload, message.payload)

    def test_ok(self):
        message = BrokerMessage(self.topic, payload=self.payload)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(self.payload.ok, message.ok)

    def test_status(self):
        message = BrokerMessage(self.topic, payload=self.payload)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(self.payload.status, message.status)

    def test_data(self):
        message = BrokerMessage(self.topic, payload=self.payload)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(self.payload.content, message.data)

    def test_avro(self):
        message = BrokerMessage(
            self.topic,
            identifier=self.identifier,
            reply_topic=self.reply_topic,
            strategy=self.strategy,
            payload=self.payload,
        )
        observed = BrokerMessage.from_avro_bytes(message.avro_bytes)
        self.assertEqual(message, observed)

    def test_sort(self):
        unsorted = [
            BrokerMessage("", BrokerMessagePayload("foo")),
            BrokerMessage("", BrokerMessagePayload(4)),
            BrokerMessage("", BrokerMessagePayload(2)),
            BrokerMessage("", BrokerMessagePayload(3)),
            BrokerMessage("", BrokerMessagePayload(1)),
            BrokerMessage("", BrokerMessagePayload("bar")),
        ]

        expected = [unsorted[0], unsorted[4], unsorted[2], unsorted[3], unsorted[1], unsorted[5]]

        observed = sorted(unsorted)
        self.assertEqual(expected, observed)


class TestBrokerMessagePayload(unittest.TestCase):
    def setUp(self) -> None:
        self.content = [FakeModel("blue"), FakeModel("red")]

    def test_ok(self):
        self.assertTrue(BrokerMessagePayload(self.content, status=BrokerMessageStatus.SUCCESS).ok)
        self.assertFalse(BrokerMessagePayload(self.content, status=BrokerMessageStatus.ERROR).ok)
        self.assertFalse(BrokerMessagePayload(self.content, status=BrokerMessageStatus.SYSTEM_ERROR).ok)

    def test_data(self):
        payload = BrokerMessagePayload(self.content)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(self.content, payload.data)


if __name__ == "__main__":
    unittest.main()

import unittest
import warnings
from uuid import (
    UUID,
    uuid4,
)

from minos.networks import (
    BrokerMessageStrategy,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerMessageV1Status,
)
from tests.utils import (
    FakeModel,
)


class TestBrokerMessage(unittest.TestCase):
    def setUp(self) -> None:
        self.topic = "FooCreated"
        self.identifier = uuid4()
        self.reply_topic = "AddOrderReply"
        self.strategy = BrokerMessageStrategy.MULTICAST

        self.payload = BrokerMessageV1Payload(
            content=[FakeModel("blue"), FakeModel("red")], headers={"foo": "bar"}, status=BrokerMessageV1Status.ERROR
        )

    def test_constructor_simple(self):
        message = BrokerMessageV1(self.topic, payload=self.payload)
        self.assertEqual(self.topic, message.topic)
        self.assertIsInstance(message.identifier, UUID)
        self.assertEqual(None, message.reply_topic)
        self.assertEqual(BrokerMessageStrategy.UNICAST, message.strategy)
        self.assertEqual(self.payload, message.payload)

    def test_constructor(self):
        message = BrokerMessageV1(
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
        message = BrokerMessageV1(self.topic, payload=self.payload)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(self.payload.ok, message.ok)

    def test_status(self):
        message = BrokerMessageV1(self.topic, payload=self.payload)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(self.payload.status, message.status)

    def test_headers(self):
        message = BrokerMessageV1(self.topic, payload=self.payload)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(self.payload.headers, message.headers)

    def test_data(self):
        message = BrokerMessageV1(self.topic, payload=self.payload)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(self.payload.content, message.data)

    def test_avro(self):
        message = BrokerMessageV1(
            self.topic,
            identifier=self.identifier,
            reply_topic=self.reply_topic,
            strategy=self.strategy,
            payload=self.payload,
        )
        observed = BrokerMessageV1.from_avro_bytes(message.avro_bytes)
        self.assertEqual(message, observed)

    def test_sort(self):
        unsorted = [
            BrokerMessageV1("", BrokerMessageV1Payload("foo")),
            BrokerMessageV1("", BrokerMessageV1Payload(4)),
            BrokerMessageV1("", BrokerMessageV1Payload(2)),
            BrokerMessageV1("", BrokerMessageV1Payload(3)),
            BrokerMessageV1("", BrokerMessageV1Payload(1)),
            BrokerMessageV1("", BrokerMessageV1Payload("bar")),
        ]

        expected = [unsorted[0], unsorted[4], unsorted[2], unsorted[3], unsorted[1], unsorted[5]]

        observed = sorted(unsorted)
        self.assertEqual(expected, observed)

    def test_payload_message(self):
        message = BrokerMessageV1(self.topic, payload=self.payload)
        self.assertEqual(message, self.payload.message)

    def test_payload_message_raises(self):
        BrokerMessageV1(self.topic, payload=self.payload)

        with self.assertRaises(ValueError):
            BrokerMessageV1(self.topic, payload=self.payload)


class TestBrokerMessagePayload(unittest.TestCase):
    def setUp(self) -> None:
        self.content = [FakeModel("blue"), FakeModel("red")]

    def test_message_none(self):
        payload = BrokerMessageV1Payload(self.content)
        self.assertEqual(None, payload.message)

    def test_message_setter(self):
        payload = BrokerMessageV1Payload(self.content)
        payload._message = "foo"
        self.assertEqual("foo", payload.message)

    def test_ok(self):
        self.assertTrue(BrokerMessageV1Payload(self.content, status=BrokerMessageV1Status.SUCCESS).ok)
        self.assertFalse(BrokerMessageV1Payload(self.content, status=BrokerMessageV1Status.ERROR).ok)
        self.assertFalse(BrokerMessageV1Payload(self.content, status=BrokerMessageV1Status.SYSTEM_ERROR).ok)

    def test_data(self):
        payload = BrokerMessageV1Payload(self.content)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            self.assertEqual(self.content, payload.data)


if __name__ == "__main__":
    unittest.main()

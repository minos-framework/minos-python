import unittest
from uuid import (
    uuid4,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    InMemoryBrokerSubscriberDuplicateDetector,
)


class TestInMemoryBrokerSubscriberDuplicateDetector(unittest.IsolatedAsyncioTestCase):
    async def test_constructor(self):
        detector = InMemoryBrokerSubscriberDuplicateDetector()
        self.assertEqual(set(), detector.seen)

    async def test_constructor_extended(self):
        one = ("foo", uuid4())
        detector = InMemoryBrokerSubscriberDuplicateDetector([one])
        self.assertEqual({one}, detector.seen)

    async def test_is_valid(self):
        one = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        two = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        three = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        detector = InMemoryBrokerSubscriberDuplicateDetector()
        self.assertTrue(await detector.is_valid(one))
        self.assertTrue(await detector.is_valid(two))
        self.assertFalse(await detector.is_valid(one))
        self.assertTrue(await detector.is_valid(three))


if __name__ == "__main__":
    unittest.main()

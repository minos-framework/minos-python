import unittest
from uuid import (
    uuid4,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    InMemoryBrokerSubscriberDuplicateValidator,
)


class TestInMemoryBrokerSubscriberDuplicateValidator(unittest.IsolatedAsyncioTestCase):
    async def test_constructor(self):
        validator = InMemoryBrokerSubscriberDuplicateValidator()
        self.assertEqual(set(), validator.seen)

    async def test_constructor_extended(self):
        one = ("foo", uuid4())
        validator = InMemoryBrokerSubscriberDuplicateValidator([one])
        self.assertEqual({one}, validator.seen)

    async def test_is_valid(self):
        one = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        two = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        three = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))

        validator = InMemoryBrokerSubscriberDuplicateValidator()
        self.assertTrue(await validator.is_valid(one))
        self.assertTrue(await validator.is_valid(two))
        self.assertFalse(await validator.is_valid(one))
        self.assertTrue(await validator.is_valid(three))


if __name__ == "__main__":
    unittest.main()

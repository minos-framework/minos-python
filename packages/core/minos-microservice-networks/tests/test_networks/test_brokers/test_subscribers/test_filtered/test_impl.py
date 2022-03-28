import unittest
from unittest.mock import (
    AsyncMock,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriber,
    FilteredBrokerSubscriber,
    InMemoryBrokerSubscriber,
    InMemoryBrokerSubscriberDuplicateValidator,
)


class TestFilteredBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.topics = {"foo", "bar"}
        self.impl = InMemoryBrokerSubscriber(self.topics)
        self.validator = InMemoryBrokerSubscriberDuplicateValidator()

    def test_is_subclass(self):
        self.assertTrue(issubclass(FilteredBrokerSubscriber, BrokerSubscriber))

    def test_impl(self):
        subscriber = FilteredBrokerSubscriber(self.impl, self.validator)
        self.assertEqual(self.impl, subscriber.impl)

    def test_queue(self):
        subscriber = FilteredBrokerSubscriber(self.impl, self.validator)
        self.assertEqual(self.validator, subscriber.validator)

    async def test_setup_destroy(self):
        impl_setup_mock = AsyncMock()
        impl_destroy_mock = AsyncMock()
        validator_setup_mock = AsyncMock()
        validator_destroy_mock = AsyncMock()

        self.impl.setup = impl_setup_mock
        self.impl.destroy = impl_destroy_mock
        self.validator.setup = validator_setup_mock
        self.validator.destroy = validator_destroy_mock

        async with FilteredBrokerSubscriber(self.impl, self.validator):
            self.assertEqual(1, impl_setup_mock.call_count)
            self.assertEqual(0, impl_destroy_mock.call_count)
            self.assertEqual(1, validator_setup_mock.call_count)
            self.assertEqual(0, validator_destroy_mock.call_count)

            impl_setup_mock.reset_mock()
            impl_destroy_mock.reset_mock()
            validator_setup_mock.reset_mock()
            validator_destroy_mock.reset_mock()

        self.assertEqual(0, impl_setup_mock.call_count)
        self.assertEqual(1, impl_destroy_mock.call_count)
        self.assertEqual(0, validator_setup_mock.call_count)
        self.assertEqual(1, validator_destroy_mock.call_count)

    async def test_receive(self):
        one = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        two = BrokerMessageV1("bar", BrokerMessageV1Payload("foo"))

        self.impl.add_message(one)
        self.impl.add_message(one)
        self.impl.add_message(two)

        async with FilteredBrokerSubscriber(self.impl, self.validator) as subscriber:
            self.assertEqual(one, await subscriber.receive())
            self.assertEqual(two, await subscriber.receive())


if __name__ == "__main__":
    unittest.main()

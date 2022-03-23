import unittest
from unittest.mock import (
    AsyncMock,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerSubscriber,
    IdempotentBrokerSubscriber,
    InMemoryBrokerSubscriber,
    InMemoryBrokerSubscriberDuplicateDetector,
)


class TestIdempotentBrokerSubscriber(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.topics = {"foo", "bar"}
        self.impl = InMemoryBrokerSubscriber(self.topics)
        self.duplicate_detector = InMemoryBrokerSubscriberDuplicateDetector()

    def test_is_subclass(self):
        self.assertTrue(issubclass(IdempotentBrokerSubscriber, BrokerSubscriber))

    def test_impl(self):
        subscriber = IdempotentBrokerSubscriber(self.impl, self.duplicate_detector)
        self.assertEqual(self.impl, subscriber.impl)

    def test_queue(self):
        subscriber = IdempotentBrokerSubscriber(self.impl, self.duplicate_detector)
        self.assertEqual(self.duplicate_detector, subscriber.duplicate_detector)

    async def test_setup_destroy(self):
        impl_setup_mock = AsyncMock()
        impl_destroy_mock = AsyncMock()
        duplicate_detector_setup_mock = AsyncMock()
        duplicate_detector_destroy_mock = AsyncMock()

        self.impl.setup = impl_setup_mock
        self.impl.destroy = impl_destroy_mock
        self.duplicate_detector.setup = duplicate_detector_setup_mock
        self.duplicate_detector.destroy = duplicate_detector_destroy_mock

        async with IdempotentBrokerSubscriber(self.impl, self.duplicate_detector):
            self.assertEqual(1, impl_setup_mock.call_count)
            self.assertEqual(0, impl_destroy_mock.call_count)
            self.assertEqual(1, duplicate_detector_setup_mock.call_count)
            self.assertEqual(0, duplicate_detector_destroy_mock.call_count)

            impl_setup_mock.reset_mock()
            impl_destroy_mock.reset_mock()
            duplicate_detector_setup_mock.reset_mock()
            duplicate_detector_destroy_mock.reset_mock()

        self.assertEqual(0, impl_setup_mock.call_count)
        self.assertEqual(1, impl_destroy_mock.call_count)
        self.assertEqual(0, duplicate_detector_setup_mock.call_count)
        self.assertEqual(1, duplicate_detector_destroy_mock.call_count)

    async def test_receive(self):
        one = BrokerMessageV1("foo", BrokerMessageV1Payload("bar"))
        two = BrokerMessageV1("bar", BrokerMessageV1Payload("foo"))

        self.impl.add_message(one)
        self.impl.add_message(one)
        self.impl.add_message(two)

        async with IdempotentBrokerSubscriber(self.impl, self.duplicate_detector) as subscriber:
            self.assertEqual(one, await subscriber.receive())
            self.assertEqual(two, await subscriber.receive())


if __name__ == "__main__":
    unittest.main()

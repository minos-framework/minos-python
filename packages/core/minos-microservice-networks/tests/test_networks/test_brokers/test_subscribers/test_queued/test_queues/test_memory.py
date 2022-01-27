import unittest

from minos.networks import (
    BrokerSubscriberQueue,
    InMemoryBrokerQueue,
    InMemoryBrokerSubscriberQueue,
    InMemoryBrokerSubscriberQueueBuilder,
)


class TestInMemoryBrokerSubscriberQueue(unittest.IsolatedAsyncioTestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(InMemoryBrokerSubscriberQueue, (InMemoryBrokerQueue, BrokerSubscriberQueue)))


class TestInMemoryBrokerSubscriberQueueBuilder(unittest.TestCase):
    def test_build(self):
        builder = InMemoryBrokerSubscriberQueueBuilder().with_topics({"one", "two"})
        subscriber = builder.build()

        self.assertIsInstance(subscriber, InMemoryBrokerSubscriberQueue)
        self.assertEqual({"one", "two"}, subscriber.topics)


if __name__ == "__main__":
    unittest.main()

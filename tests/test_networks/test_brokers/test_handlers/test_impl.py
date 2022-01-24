import unittest
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.networks import (
    BrokerHandler,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    InMemoryBrokerPublisher,
    InMemoryBrokerSubscriberBuilder,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestBrokerHandler(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.messages = [
            BrokerMessageV1("foo", BrokerMessageV1Payload("bar")),
            BrokerMessageV1("bar", BrokerMessageV1Payload("foo")),
        ]

        self.publisher = InMemoryBrokerPublisher()
        self.subscriber_builder = InMemoryBrokerSubscriberBuilder

    async def test_run(self):
        dispatch_mock = AsyncMock(side_effect=[None, ValueError])

        async with BrokerHandler.from_config(
            CONFIG_FILE_PATH, publisher=self.publisher, subscriber_builder=self.subscriber_builder
        ) as handler:
            handler._subscriber.receive = AsyncMock(side_effect=self.messages)
            handler._dispatcher.dispatch = dispatch_mock
            await handler.run()

        self.assertEqual([call(self.messages[0]), call(self.messages[1])], dispatch_mock.call_args_list)


if __name__ == "__main__":
    unittest.main()

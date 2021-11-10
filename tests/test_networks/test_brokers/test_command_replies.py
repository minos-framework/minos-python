import unittest
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandReplyBroker,
    PublishResponse,
    PublishResponseStatus,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


class TestCommandReplyBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config_default(self):
        broker = CommandReplyBroker.from_config(config=self.config)
        self.assertIsInstance(broker, CommandReplyBroker)

    def test_action(self):
        self.assertEqual("commandReply", CommandReplyBroker.ACTION)

    async def test_send(self):
        mock = AsyncMock(return_value=56)
        saga = uuid4()
        reply_topic = "fakeReply"
        async with CommandReplyBroker.from_config(config=self.config) as broker:
            broker.enqueue = mock
            identifier = await broker.send(
                FakeModel("foo"), saga=saga, topic=reply_topic, status=PublishResponseStatus.SUCCESS
            )

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual(reply_topic, args[0])

        expected = PublishResponse(
            reply_topic, FakeModel("foo"), saga, PublishResponseStatus.SUCCESS, self.config.service.name
        )
        observed = PublishResponse.from_avro_bytes(args[1])
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

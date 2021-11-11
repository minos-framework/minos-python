import unittest
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

from minos.common import (
    Model,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerMessage,
    BrokerMessageStatus,
    CommandReplyBroker,
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
                FakeModel("foo"), saga=saga, topic=reply_topic, status=BrokerMessageStatus.SUCCESS
            )

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual(reply_topic, args[0])

        expected = BrokerMessage(
            reply_topic,
            FakeModel("foo"),
            identifier=saga,
            status=BrokerMessageStatus.SUCCESS,
            service_name=self.config.service.name,
        )
        observed = Model.from_avro_bytes(args[1])
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

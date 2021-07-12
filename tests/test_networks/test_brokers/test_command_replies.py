"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

from minos.common import (
    CommandReply,
    CommandStatus,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
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
        async with CommandReplyBroker.from_config(config=self.config) as broker:
            broker.send_bytes = mock
            identifier = await broker.send(FakeModel("foo"), saga=saga, topic="fake", status=CommandStatus.SUCCESS)

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fakeReply", args[0])

        expected = CommandReply("fakeReply", FakeModel("foo"), saga, CommandStatus.SUCCESS)
        observed = CommandReply.from_avro_bytes(args[1])
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

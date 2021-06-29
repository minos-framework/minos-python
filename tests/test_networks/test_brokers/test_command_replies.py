"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from unittest.mock import (
    MagicMock,
)

from minos.common import (
    CommandReply,
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

    def test_from_config_with_args(self):
        broker = CommandReplyBroker.from_config("CommandBroker", saga_uuid="9347839473kfslf", config=self.config)
        self.assertIsInstance(broker, CommandReplyBroker)

    def test_from_config_default(self):
        broker = CommandReplyBroker.from_config(config=self.config)
        self.assertIsInstance(broker, CommandReplyBroker)

    async def test_send_one(self):
        async def _fn(*args, **kwargs):
            return 56

        mock = MagicMock(side_effect=_fn)

        async with CommandReplyBroker.from_config(config=self.config) as broker:
            broker.send_bytes = mock
            identifier = await broker.send_one(FakeModel("foo"), saga_uuid="9347839473kfslf", topic="fake")

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fakeReply", args[0])
        self.assertEqual(
            CommandReply(topic="fakeReply", items=[FakeModel("foo")], saga_uuid="9347839473kfslf"),
            CommandReply.from_avro_bytes(args[1]),
        )


if __name__ == "__main__":
    unittest.main()

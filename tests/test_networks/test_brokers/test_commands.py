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
    Command,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandBroker,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


class TestCommandBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config_with_arguments(self):
        broker = CommandBroker.from_config(
            "CommandBroker",
            saga_id="9347839473kfslf",
            task_id="92839283hjijh232",
            reply_on="test_reply_on",
            config=self.config,
        )
        self.assertIsInstance(broker, CommandBroker)

    def test_from_config_default(self):
        broker = CommandBroker.from_config(config=self.config)
        self.assertIsInstance(broker, CommandBroker)

    async def test_send_one(self):
        async def _fn(*args, **kwargs):
            return 56

        mock = MagicMock(side_effect=_fn)

        async with CommandBroker.from_config(config=self.config) as broker:
            broker.send_bytes = mock
            identifier = await broker.send_one(
                FakeModel("foo"), saga_uuid="9347839473kfslf", topic="fake", reply_topic="ekaf"
            )

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(
            Command(topic="fake", items=[FakeModel("foo")], saga_uuid="9347839473kfslf", reply_topic="ekaf"),
            Command.from_avro_bytes(args[1]),
        )


if __name__ == "__main__":
    unittest.main()

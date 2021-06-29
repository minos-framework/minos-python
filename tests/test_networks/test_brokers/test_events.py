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
    Event,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    EventBroker,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
)


class TestEventBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config_with_args(self):
        self.assertIsInstance(EventBroker.from_config("EventBroker", config=self.config), EventBroker)

    def test_from_config_default(self):
        self.assertIsInstance(EventBroker.from_config(config=self.config), EventBroker)

    def test_action(self):
        self.assertEqual("event", EventBroker.ACTION)

    async def test_send_one(self):
        async def _fn(*args, **kwargs):
            return 56

        mock = MagicMock(side_effect=_fn)

        async with EventBroker.from_config(config=self.config) as broker:
            broker.send_bytes = mock
            identifier = await broker.send_one(FakeModel("foo"), topic="fake")

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(Event(topic="fake", items=[FakeModel("foo")]), Event.from_avro_bytes(args[1]))


if __name__ == "__main__":
    unittest.main()

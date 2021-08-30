"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from unittest.mock import (
    AsyncMock,
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
    FAKE_AGGREGATE_DIFF,
)


class TestEventBroker(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config_default(self):
        self.assertIsInstance(EventBroker.from_config(config=self.config), EventBroker)

    def test_action(self):
        self.assertEqual("event", EventBroker.ACTION)

    async def test_send(self):
        mock = AsyncMock(return_value=56)
        async with EventBroker.from_config(config=self.config) as broker:
            broker.enqueue = mock
            identifier = await broker.send(FAKE_AGGREGATE_DIFF, topic="fake")

        self.assertEqual(56, identifier)
        self.assertEqual(1, mock.call_count)

        args = mock.call_args.args
        self.assertEqual("fake", args[0])
        self.assertEqual(Event("fake", FAKE_AGGREGATE_DIFF), Event.from_avro_bytes(args[1]))


if __name__ == "__main__":
    unittest.main()

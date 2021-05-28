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
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandReplyConsumer,
)
from tests.utils import (
    BASE_PATH,
    FakeConsumer,
    Message,
    NaiveAggregate,
)


class TestCommandReplyConsumer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = CommandReplyConsumer.from_config(config=self.config)
        self.assertIsInstance(dispatcher, CommandReplyConsumer)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            CommandReplyConsumer.from_config()

    async def test_queue_add(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = CommandReply(
            topic="AddOrder", model=model.classname, items=[], saga_uuid="43434jhij", reply_on="mkk2334",
        )
        bin_data = event_instance.avro_bytes
        CommandReply.from_avro_bytes(bin_data)

        async with CommandReplyConsumer.from_config(config=self.config, consumer=FakeConsumer()) as consumer:
            id = await consumer.queue_add(topic=event_instance.topic, partition=0, binary=bin_data)
            assert id > 0

    async def test_dispatch(self):
        model = NaiveAggregate(test_id=1, test=2, id=1, version=1)
        event_instance = CommandReply(
            topic="AddOrder", model=model.classname, items=[], saga_uuid="43434jhij", reply_on="mkk2334",
        )
        bin_data = event_instance.avro_bytes
        consumer = FakeConsumer([Message(topic="AddOrder", partition=0, value=bin_data)])

        async with CommandReplyConsumer.from_config(config=self.config, consumer=consumer) as dispatcher:
            mock = MagicMock(side_effect=dispatcher.handle_single_message)
            dispatcher.handle_single_message = mock
            await dispatcher.dispatch()
            self.assertEqual(1, mock.call_count)

    async def test_dispatch_ko(self):
        bin_data = bytes(b"test")
        consumer = FakeConsumer([Message(topic="AddOrder", partition=0, value=bin_data)])
        async with CommandReplyConsumer.from_config(config=self.config, consumer=consumer) as consumer:
            await consumer.dispatch()


if __name__ == "__main__":
    unittest.main()

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
    MinosConfigException,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    EventConsumer,
)
from tests.utils import (
    BASE_PATH,
    FakeConsumer,
    Foo,
    Message,
)


class TestEventConsumer(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = EventConsumer.from_config(config=self.config)
        self.assertIsInstance(dispatcher, EventConsumer)

    def test_from_config_raises(self):
        with self.assertRaises(MinosConfigException):
            EventConsumer.from_config()

    async def test_queue_add(self):
        model = Foo("test")
        event_instance = Event(topic="TestEventQueueAdd", model=model.classname, items=[])
        bin_data = event_instance.avro_bytes
        Event.from_avro_bytes(bin_data)

        async with EventConsumer.from_config(config=self.config, consumer=FakeConsumer()) as dispatcher:
            id = await dispatcher.queue_add(topic=event_instance.topic, partition=0, binary=bin_data)
            assert id > 0

    async def test_dispatch(self):
        model = Foo("test")
        event_instance = Event(topic="TicketAdded", model=model.classname, items=[model])
        bin_data = event_instance.avro_bytes
        consumer = FakeConsumer([Message(topic="TicketAdded", partition=0, value=bin_data)])

        async with EventConsumer.from_config(config=self.config, consumer=consumer) as dispatcher:
            mock = MagicMock(side_effect=dispatcher.handle_single_message)
            dispatcher.handle_single_message = mock
            await dispatcher.dispatch()
            self.assertEqual(1, mock.call_count)

    async def test_dispatch_ko(self):
        bin_data = bytes(b"test")
        consumer = FakeConsumer([Message(topic="TicketAdded", partition=0, value=bin_data)])

        async with EventConsumer.from_config(config=self.config, consumer=consumer) as dispatcher:
            await dispatcher.dispatch()


if __name__ == "__main__":
    unittest.main()

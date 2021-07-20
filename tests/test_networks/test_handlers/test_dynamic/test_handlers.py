"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from datetime import (
    datetime,
    timedelta,
)
from unittest.mock import (
    patch,
)

from minos.common import (
    MinosHandler,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    DynamicHandler,
    HandlerEntry,
    MinosHandlerNotEnoughEntriesFoundException,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
    Message,
)


class TestDynamicHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()
        self.handler = DynamicHandler.from_config(config=self.config)

    def test_base_classes(self):
        self.assertIsInstance(self.handler, MinosHandler)

    def test_broker_host(self):
        self.assertEqual(self.config.saga.broker.host, self.handler.broker_host)

    def test_broker_port(self):
        self.assertEqual(self.config.saga.broker.port, self.handler.broker_port)

    async def test_get_one(self):
        with patch("aiokafka.AIOKafkaConsumer.getmany") as mock:
            mock.return_value = {"foo": [Message("foo", 0, FakeModel("test").avro_bytes)]}
            expected = (await self.handler.get_many("foo", count=1))[0]
            observed = await self.handler.get_one("foo")
            self._assert_equal_entries(expected, observed)

    async def test_get_many(self):
        expected = [
            HandlerEntry(0, "foo", None, 0, FakeModel("test1"), 0, datetime.now()),
            HandlerEntry(0, "foo", None, 0, FakeModel("test2"), 0, datetime.now()),
            HandlerEntry(0, "bar", None, 0, FakeModel("test3"), 0, datetime.now()),
            HandlerEntry(0, "bar", None, 0, FakeModel("test4"), 0, datetime.now()),
        ]
        with patch("aiokafka.AIOKafkaConsumer.getmany") as mock:
            mock.return_value = {
                "foo": [
                    Message("foo", 0, FakeModel("test1").avro_bytes),
                    Message("foo", 0, FakeModel("test2").avro_bytes),
                ],
                "bar": [
                    Message("bar", 0, FakeModel("test3").avro_bytes),
                    Message("bar", 0, FakeModel("test4").avro_bytes),
                ],
            }
            observed = await self.handler.get_many("foo")

        self.assertEqual(len(expected), len(observed))
        for e, o in zip(expected, observed):
            self._assert_equal_entries(e, o)

    async def test_get_many_raises(self):
        with patch("aiokafka.AIOKafkaConsumer.getmany") as mock:
            mock.return_value = {
                "foo": [Message("foo", 0, FakeModel("test1").avro_bytes)],
                "bar": [Message("bar", 0, FakeModel("test4").avro_bytes)],
            }
            with self.assertRaises(MinosHandlerNotEnoughEntriesFoundException):
                await self.handler.get_many("foo", count=3)

    def _assert_equal_entries(self, expected, observed):
        self.assertEqual(expected.id, observed.id)
        self.assertEqual(expected.topic, observed.topic)
        self.assertEqual(expected.callback, observed.callback)
        self.assertEqual(expected.partition_id, observed.partition_id)
        self.assertEqual(expected.data, observed.data)
        self.assertEqual(expected.retry, observed.retry)
        self.assertAlmostEqual(expected.created_at, observed.created_at, delta=timedelta(seconds=2))


if __name__ == "__main__":
    unittest.main()

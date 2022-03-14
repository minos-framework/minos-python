import unittest
from abc import (
    ABC,
)
from asyncio import (
    Queue,
)
from typing import (
    Any,
)

from minos.common import (
    MinosConfig,
    SetupMixin,
)
from minos.networks import (
    Builder,
    consume_queue,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class _Builder(Builder[dict[str, Any]]):
    def build(self) -> dict[str, Any]:
        """For testing purposes."""
        return self.kwargs


class TestUtils(unittest.IsolatedAsyncioTestCase):
    async def test_consume_queue(self):
        queue = Queue()

        await queue.put(1)

        await consume_queue(queue, 10)

    async def test_consume_queue_full(self):
        queue = Queue()

        await queue.put(1)
        await queue.put(2)
        await queue.put(3)

        await consume_queue(queue, 2)

        self.assertEqual(3, await queue.get())
        self.assertTrue(queue.empty())


class TestBuilder(unittest.TestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(Builder, (ABC, SetupMixin)))
        # noinspection PyUnresolvedReferences
        self.assertEqual({"build"}, Builder.__abstractmethods__)

    def test_new(self):
        builder = _Builder.new()
        self.assertIsInstance(builder, _Builder)
        self.assertEqual(dict(), builder.kwargs)

    def test_copy(self):
        base = _Builder.new().with_kwargs({"one": "two"})
        builder = base.copy()
        self.assertNotEqual(id(base), id(builder))
        self.assertIsInstance(builder, _Builder)
        self.assertEqual({"one": "two"}, builder.kwargs)

    def test_with_kwargs(self):
        builder = _Builder().with_kwargs({"foo": "bar"})
        self.assertIsInstance(builder, _Builder)
        self.assertEqual({"foo": "bar"}, builder.kwargs)

    def test_with_config(self):
        config = MinosConfig(CONFIG_FILE_PATH)
        builder = _Builder().with_config(config)
        self.assertIsInstance(builder, _Builder)
        self.assertEqual(dict(), builder.kwargs)

    def test_build(self):
        builder = _Builder().with_kwargs({"one": "two"})
        self.assertIsInstance(builder, _Builder)
        self.assertEqual({"one": "two"}, builder.build())

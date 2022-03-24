import unittest
import warnings
from asyncio import (
    Queue,
)

from minos.common import Builder as CommonBuilder
from minos.networks import (
    Builder,
    consume_queue,
)


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
    def test_is_subclass(self):
        self.assertTrue(issubclass(Builder, CommonBuilder))

    def test_warnings(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            builder = Builder()
            self.assertIsInstance(builder, CommonBuilder)


if __name__ == "__main__":
    unittest.main()

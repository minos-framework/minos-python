import unittest
from asyncio import (
    Queue,
)

from minos.networks import (
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

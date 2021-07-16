"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    AggregateDiff,
    Event,
    Field,
    FieldsDiff,
)
from minos.networks import (
    Subscribe,
)

FAKE_AGGREGATE_DIFF = AggregateDiff(uuid4(), "Foo", 3, FieldsDiff({"doors": Field("doors", int, 5)}))


class HandleEvent:
    def __init__(self, *args, **kwargs):
        pass

    async def _handle(self, *args, **kwargs):
        print(args)
        print(kwargs)
        print(self)
        return 1


class TestDecorators(unittest.IsolatedAsyncioTestCase):
    async def test_base_call(self):
        def mock_func(*args, **kwargs):
            return 1

        result = Subscribe(mock_func)
        result = await result()
        self.assertEqual(result, 1)

    async def test_async(self):
        async def mock_func(*args, **kwargs):
            return 1

        result = Subscribe(mock_func)
        result = await result()
        self.assertEqual(result, 1)

    async def test_event(self):
        instance = Event("AddOrder", FAKE_AGGREGATE_DIFF)
        result = Subscribe(HandleEvent)
        result = await result(instance)
        self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()

"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from minos.networks import (
    Subscribe,
)


class TestDecorators(unittest.IsolatedAsyncioTestCase):

    async def _handle(self, *args, **kwargs):
        pass

    async def test_base_call(self):
        def mock_func(*args, **kwargs):
            return 1
        result = Subscribe(mock_func)
        result = await result()
        self.assertEqual(result, 1)

    async def test_event(self):
        def mock_func(*args, **kwargs):
            return 1
        result = Subscribe(mock_func)
        result = await result()
        self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()

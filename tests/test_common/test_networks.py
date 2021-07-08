"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from tests.model_classes import (
    Foo,
)
from tests.utils import (
    FakeBroker,
)


class TestMinosBroker(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.broker = FakeBroker()

    async def test_send(self):
        await self.broker.send([Foo("red"), Foo("red")])
        self.assertEqual(1, self.broker.call_count)
        self.assertEqual({"data": [Foo("red"), Foo("red")]}, self.broker.call_kwargs)


if __name__ == "__main__":
    unittest.main()

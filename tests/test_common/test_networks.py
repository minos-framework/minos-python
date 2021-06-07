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


class TestMinosBaseBroker(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.broker = FakeBroker()

    async def test_send(self):
        self.assertEqual(None, await self.broker.send([Foo("red"), Foo("red")]))

    async def test_send_one(self):
        self.assertEqual(None, await self.broker.send_one(Foo("red")))


if __name__ == "__main__":
    unittest.main()

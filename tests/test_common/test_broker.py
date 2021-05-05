"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from typing import (
    NoReturn,
)

from minos.common import (
    Aggregate,
    MinosBaseBroker,
)
from tests.aggregate_classes import (
    Car,
)


class MinosBroker(MinosBaseBroker):
    async def send(self, items: list[Aggregate]) -> NoReturn:
        pass


class TestMinosBaseBroker(unittest.IsolatedAsyncioTestCase):
    def test_topic(self):
        broker = MinosBroker("CarAdded")
        self.assertEqual("CarAdded", broker.topic)

    async def test_send(self):
        broker = MinosBroker("CarAdded")
        self.assertEqual(None, await broker.send([Car(1, 1, 3, "red"), Car(1, 1, 3, "red")]))

    async def test_send_one(self):
        broker = MinosBroker("CarAdded")
        self.assertEqual(None, await broker.send_one(Car(1, 1, 3, "red")))


if __name__ == "__main__":
    unittest.main()

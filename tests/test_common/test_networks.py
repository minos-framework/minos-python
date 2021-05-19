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
    MinosBroker,
)
from tests.aggregate_classes import (
    Car,
)


class _MinosBroker(MinosBroker):
    @classmethod
    async def send(cls, items: list[Aggregate], **kwargs) -> NoReturn:
        pass


class TestMinosBaseBroker(unittest.IsolatedAsyncioTestCase):
    async def test_send(self):
        self.assertEqual(None, await _MinosBroker.send([Car(1, 1, 3, "red"), Car(1, 1, 3, "red")]))

    async def test_send_one(self):
        self.assertEqual(None, await _MinosBroker.send_one(Car(1, 1, 3, "red")))


if __name__ == "__main__":
    unittest.main()

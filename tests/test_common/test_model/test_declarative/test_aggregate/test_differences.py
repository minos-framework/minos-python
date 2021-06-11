"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    AggregateDiff,
    ModelField,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    FakeBroker,
    FakeRepository,
    FakeSnapshot,
)


class TestAggregateDifferences(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        async with FakeBroker() as broker, FakeRepository() as repository, FakeSnapshot() as snapshot:
            self.initial = Car(1, 1, 3, "blue", _broker=broker, _repository=repository, _snapshot=snapshot)
            self.final = Car(1, 3, 5, "yellow", _broker=broker, _repository=repository, _snapshot=snapshot)

    def test_diff(self):
        expected = AggregateDiff({"doors": ModelField("doors", int, 5), "color": ModelField("color", str, "yellow")})
        observed = self.initial.diff(self.final)
        self.assertEqual(expected, observed)

    def test_apply_diff(self):
        diff = AggregateDiff({"doors": ModelField("doors", int, 5), "color": ModelField("color", str, "yellow")})
        self.initial.apply_diff(3, diff)
        self.assertEqual(self.final, self.initial)


if __name__ == "__main__":
    unittest.main()

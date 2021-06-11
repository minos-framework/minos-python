"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from typing import (
    Optional,
)

from minos.common import (
    AggregateDiff,
    ModelField,
    ModelRef,
)
from tests.aggregate_classes import (
    Car,
    Owner,
)
from tests.utils import (
    FakeBroker,
    FakeRepository,
    FakeSnapshot,
)


class TestAggregateDiff(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        async with FakeBroker() as broker, FakeRepository() as repository, FakeSnapshot() as snapshot:
            self.car_one = Car(1, 1, 3, "blue", _broker=broker, _repository=repository, _snapshot=snapshot)
            self.car_two = Car(1, 2, 5, "red", _broker=broker, _repository=repository, _snapshot=snapshot)
            self.car_three = Car(1, 3, 5, "yellow", _broker=broker, _repository=repository, _snapshot=snapshot)

    def test_constructor(self):
        fields = {"doors": ModelField("doors", int, 5), "color": ModelField("color", str, "red")}
        difference = AggregateDiff(fields)
        self.assertEqual(fields, difference.fields)

    def test_from_update(self):
        expected = AggregateDiff({"doors": ModelField("doors", int, 5), "color": ModelField("color", str, "red")})
        observed = AggregateDiff.from_update(self.car_one, self.car_two)
        self.assertEqual(expected, observed)

    def test_from_create(self):
        expected = AggregateDiff(
            {
                "doors": ModelField("doors", int, 5),
                "color": ModelField("color", str, "red"),
                "owner": ModelField("owner", Optional[list[ModelRef[Owner]]], None),
            }
        )
        observed = AggregateDiff.from_create(self.car_two)
        self.assertEqual(expected, observed)

    def test_simplify(self):
        expected = AggregateDiff({"doors": ModelField("doors", int, 5), "color": ModelField("color", str, "yellow")})

        first = AggregateDiff.from_update(self.car_one, self.car_two)
        second = AggregateDiff.from_update(self.car_two, self.car_three)
        observed = AggregateDiff.simplify(first, second)

        self.assertEqual(expected, observed)

    def test_apply_to(self):
        diff = AggregateDiff({"doors": ModelField("doors", int, 5), "color": ModelField("color", str, "yellow")})
        self.car_one.apply_diff(3, diff)
        self.assertEqual(self.car_three, self.car_one)

    def test_avro_schema(self):
        expected = [
            {
                "fields": [{"name": "doors", "type": "int"}, {"name": "color", "type": "string"}],
                "name": "AggregateDiff",
                "namespace": "minos.common.model.dynamic.aggregate_diff",
                "type": "record",
            }
        ]
        diff = AggregateDiff({"doors": ModelField("doors", int, 5), "color": ModelField("color", str, "yellow")})
        self.assertEqual(expected, diff.avro_schema)

    def test_avro_data(self):
        expected = {"color": "yellow", "doors": 5}
        diff = AggregateDiff({"doors": ModelField("doors", int, 5), "color": ModelField("color", str, "yellow")})
        self.assertEqual(expected, diff.avro_data)

    def test_avro_bytes(self):
        diff = AggregateDiff({"doors": ModelField("doors", int, 5), "color": ModelField("color", str, "yellow")})
        self.assertIsInstance(diff.avro_bytes, bytes)

    def test_from_avro_bytes(self):
        initial = AggregateDiff({"doors": ModelField("doors", int, 5), "color": ModelField("color", str, "yellow")})
        observed = AggregateDiff.from_avro_bytes(initial.avro_bytes)
        self.assertEqual(initial, observed)


if __name__ == "__main__":
    unittest.main()

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
    Field,
    FieldsDiff,
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
            self.initial = Car(3, "blue", id=1, version=1, _broker=broker, _repository=repository, _snapshot=snapshot)
            self.final = Car(5, "yellow", id=1, version=3, _broker=broker, _repository=repository, _snapshot=snapshot)
            self.another = Car(3, "blue", id=3, version=1, _broker=broker, _repository=repository, _snapshot=snapshot)

    def test_from_aggregate(self):
        expected = AggregateDiff(
            id=1,
            name=Car.classname,
            version=1,
            fields_diff=FieldsDiff(
                {
                    "doors": Field("doors", int, 3),
                    "color": Field("color", str, "blue"),
                    "owner": Field("owner", Optional[list[ModelRef[Owner]]], None),
                }
            ),
        )
        observed = AggregateDiff.from_aggregate(self.initial)
        self.assertEqual(expected, observed)

    def test_from_deleted_aggregate(self):
        expected = AggregateDiff(id=1, name=Car.classname, version=1, fields_diff=FieldsDiff.empty(),)
        observed = AggregateDiff.from_deleted_aggregate(self.initial)
        self.assertEqual(expected, observed)

    def test_from_difference(self):
        expected = AggregateDiff(
            id=1,
            name=Car.classname,
            version=3,
            fields_diff=FieldsDiff({"doors": Field("doors", int, 5), "color": Field("color", str, "yellow")}),
        )
        observed = AggregateDiff.from_difference(self.final, self.initial)
        self.assertEqual(expected, observed)

    def test_from_difference_raises(self):
        with self.assertRaises(ValueError):
            AggregateDiff.from_difference(self.initial, self.another)

    def test_simplify(self):
        expected = AggregateDiff(
            id=1,
            name=Car.classname,
            version=3,
            fields_diff=FieldsDiff({"doors": Field("doors", int, 5), "color": Field("color", str, "red")}),
        )

        one = AggregateDiff(1, Car.classname, 1, FieldsDiff({"color": Field("color", str, "yellow")}))
        two = AggregateDiff(
            id=1,
            name=Car.classname,
            version=2,
            fields_diff=FieldsDiff({"doors": Field("doors", int, 1), "color": Field("color", str, "red")}),
        )
        three = AggregateDiff(1, Car.classname, 3, FieldsDiff({"doors": Field("doors", int, 5)}))
        observed = AggregateDiff.simplify(one, two, three)
        self.assertEqual(expected, observed)

    def test_avro_serialization(self):
        initial = AggregateDiff(
            id=1,
            name=Car.classname,
            version=1,
            fields_diff=FieldsDiff(
                {
                    "doors": Field("doors", int, 3),
                    "color": Field("color", str, "blue"),
                    "owner": Field("owner", Optional[list[ModelRef[Owner]]], None),
                }
            ),
        )

        serialized = initial.avro_bytes
        self.assertIsInstance(serialized, bytes)

        deserialized = AggregateDiff.from_avro_bytes(serialized)
        self.assertEqual(initial, deserialized)


if __name__ == "__main__":
    unittest.main()

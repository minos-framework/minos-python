"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    NULL_UUID,
    Field,
    FieldsDiff,
    ModelRef,
    ModelType,
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


class TestFieldsDiff(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        async with FakeBroker() as broker, FakeRepository() as repository, FakeSnapshot() as snapshot:
            self.car_one = Car(3, "blue", id=1, version=1, _broker=broker, _repository=repository, _snapshot=snapshot)
            self.car_two = Car(5, "red", id=1, version=2, _broker=broker, _repository=repository, _snapshot=snapshot)
            self.car_three = Car(
                5, "yellow", id=1, version=3, _broker=broker, _repository=repository, _snapshot=snapshot
            )
            self.car_four = Car(3, "blue", id=2, version=1, _broker=broker, _repository=repository, _snapshot=snapshot)

    def test_constructor(self):
        fields = {"doors": Field("doors", int, 5), "color": Field("color", str, "red")}
        difference = FieldsDiff(fields)
        self.assertEqual(fields, difference.fields)

    def test_model_type(self):
        fields = {Field("doors", int, 5), Field("color", str, "red")}
        difference = FieldsDiff(fields)
        # noinspection PyTypeChecker
        self.assertEqual(ModelType.build(FieldsDiff.classname, {"doors": int, "color": str}), difference.model_type)

    def test_from_difference(self):
        expected = FieldsDiff(
            {"version": Field("version", int, 2), "doors": Field("doors", int, 5), "color": Field("color", str, "red")}
        )
        observed = FieldsDiff.from_difference(self.car_two, self.car_one)
        self.assertEqual(expected, observed)

    def test_from_difference_with_ignore(self):
        expected = FieldsDiff({"doors": Field("doors", int, 5), "color": Field("color", str, "red")})
        observed = FieldsDiff.from_difference(self.car_two, self.car_one, ignore=["uuid", "version"])
        self.assertEqual(expected, observed)

    def test_with_difference_not_hashable(self):
        expected = FieldsDiff({"values": Field("values", list[int], [1, 2, 3])})

        model_type = ModelType.build("Foo", {"values": list[int]})
        a, b = model_type(values=[0]), model_type(values=[1, 2, 3])
        observed = FieldsDiff.from_difference(b, a)
        self.assertEqual(expected, observed)

    def test_from_model(self):
        expected = FieldsDiff(
            {
                "uuid": Field("uuid", UUID, NULL_UUID),
                "version": Field("version", int, 2),
                "doors": Field("doors", int, 5),
                "color": Field("color", str, "red"),
                "owner": Field("owner", Optional[list[ModelRef[Owner]]], None),
            }
        )
        observed = FieldsDiff.from_model(self.car_two)
        self.assertEqual(expected, observed)

    def test_from_model_with_ignore(self):
        expected = FieldsDiff(
            {
                "doors": Field("doors", int, 5),
                "color": Field("color", str, "red"),
                "owner": Field("owner", Optional[list[ModelRef[Owner]]], None),
            }
        )
        observed = FieldsDiff.from_model(self.car_two, ignore=["uuid", "version"])
        self.assertEqual(expected, observed)

    def test_simplify(self):
        expected = FieldsDiff({"doors": Field("doors", int, 5), "color": Field("color", str, "yellow")})

        first = FieldsDiff.from_difference(self.car_two, self.car_one, ignore=["uuid", "version"])
        second = FieldsDiff.from_difference(self.car_three, self.car_two, ignore=["uuid", "version"])
        observed = FieldsDiff.simplify(first, second)

        self.assertEqual(expected, observed)

    def test_avro_schema(self):
        expected = [
            {
                "fields": [{"name": "doors", "type": "int"}, {"name": "color", "type": "string"}],
                "name": "FieldsDiff",
                "namespace": "minos.common.model.dynamic.diff",
                "type": "record",
            }
        ]
        diff = FieldsDiff({"doors": Field("doors", int, 5), "color": Field("color", str, "yellow")})
        self.assertEqual(expected, diff.avro_schema)

    def test_avro_data(self):
        expected = {"color": "yellow", "doors": 5}
        diff = FieldsDiff({"doors": Field("doors", int, 5), "color": Field("color", str, "yellow")})
        self.assertEqual(expected, diff.avro_data)

    def test_avro_bytes(self):
        diff = FieldsDiff({"doors": Field("doors", int, 5), "color": Field("color", str, "yellow")})
        self.assertIsInstance(diff.avro_bytes, bytes)

    def test_from_avro_bytes(self):
        initial = FieldsDiff({"doors": Field("doors", int, 5), "color": Field("color", str, "yellow")})
        observed = FieldsDiff.from_avro_bytes(initial.avro_bytes)
        self.assertEqual(initial, observed)


if __name__ == "__main__":
    unittest.main()

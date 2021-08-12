"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from unittest.mock import (
    patch,
)

from minos.common import (
    NULL_UUID,
    Action,
    Difference,
    DifferenceContainer,
    IncrementalDifference,
    ModelType,
)
from tests.aggregate_classes import (
    Car,
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

    def test_differences(self):
        fields = [Difference("doors", 5), Difference("color", "red")]
        difference = DifferenceContainer(fields)
        expected = {"doors": [Difference("doors", 5)], "color": [Difference("color", "red")]}
        self.assertEqual(expected, difference.differences)

    def test_get_attr(self):
        fields = [Difference("doors", 5), Difference("color", "red")]
        difference = DifferenceContainer(fields)
        self.assertEqual(Difference("doors", 5), difference["doors"])

    def test_get_attr_list(self):
        fields = [
            IncrementalDifference("doors", 5, Action.CREATE),
            IncrementalDifference("doors", 3, Action.CREATE),
            Difference("color", "red"),
        ]
        difference = DifferenceContainer(fields)
        expected = [IncrementalDifference("doors", 5, Action.CREATE), IncrementalDifference("doors", 3, Action.CREATE)]
        self.assertEqual(expected, difference["doors"])

    def test_model_type(self):
        fields = [Difference("doors", 5), Difference("color", "red")]
        difference = DifferenceContainer(fields)
        # noinspection PyTypeChecker
        self.assertEqual(ModelType.build(DifferenceContainer.classname, {"doors": str}), difference.model_type)

    def test_from_difference(self):
        expected = DifferenceContainer([Difference("version", 2), Difference("doors", 5), Difference("color", "red")])
        observed = DifferenceContainer.from_difference(self.car_two, self.car_one)
        self.assertEqual(expected, observed)

    def test_from_difference_with_ignore(self):
        expected = DifferenceContainer([Difference("doors", 5), Difference("color", "red")])
        observed = DifferenceContainer.from_difference(self.car_two, self.car_one, ignore={"uuid", "version"})
        self.assertEqual(expected, observed)

    def test_with_difference_not_hashable(self):
        expected = DifferenceContainer([Difference("values", [1, 2, 3])])

        model_type = ModelType.build("Foo", {"values": list[int]})
        a, b = model_type(values=[0]), model_type(values=[1, 2, 3])
        observed = DifferenceContainer.from_difference(b, a)
        self.assertEqual(expected, observed)

    def test_from_model(self):
        expected = DifferenceContainer(
            [
                Difference("uuid", NULL_UUID),
                Difference("version", 2),
                Difference("doors", 5),
                Difference("color", "red"),
                Difference("owner", None),
            ]
        )
        observed = DifferenceContainer.from_model(self.car_two)
        self.assertEqual(expected, observed)

    def test_from_model_with_ignore(self):
        expected = DifferenceContainer([Difference("doors", 5), Difference("color", "red"), Difference("owner", None)])
        observed = DifferenceContainer.from_model(self.car_two, ignore={"uuid", "version"})
        self.assertEqual(expected, observed)

    def test_avro_schema(self):
        expected = [
            {
                "fields": [
                    {
                        "name": "hello",
                        "type": {
                            "fields": [{"name": "name", "type": "string"}, {"name": "value", "type": "int"}],
                            "name": "Difference",
                            "namespace": "minos.common.model.dynamic.diff.hola",
                            "type": "record",
                        },
                    },
                    {
                        "name": "goodbye",
                        "type": {
                            "fields": [{"name": "name", "type": "string"}, {"name": "value", "type": "string"}],
                            "name": "Difference",
                            "namespace": "minos.common.model.dynamic.diff.adios",
                            "type": "record",
                        },
                    },
                ],
                "name": "DifferenceContainer",
                "namespace": "minos.common.model.dynamic.diff",
                "type": "record",
            }
        ]
        with patch("minos.common.DifferenceContainer.generate_random_str", side_effect=["hello", "goodbye"]):
            diff = DifferenceContainer([Difference("doors", 5), Difference("color", "yellow")])
        with patch("minos.common.AvroSchemaEncoder.generate_random_str", side_effect=["hola", "adios"]):
            self.assertEqual(expected, diff.avro_schema)

    def test_avro_data(self):
        expected = {"hello": {"name": "doors", "value": 5}, "goodbye": {"name": "color", "value": "yellow"}}

        with patch("minos.common.DifferenceContainer.generate_random_str", side_effect=["hello", "goodbye"]):
            diff = DifferenceContainer([Difference("doors", 5), Difference("color", "yellow")])

        self.assertEqual(expected, diff.avro_data)

    def test_avro_bytes(self):
        diff = DifferenceContainer([Difference("doors", 5), Difference("color", "yellow")])
        self.assertIsInstance(diff.avro_bytes, bytes)

    def test_from_avro_bytes(self):
        initial = DifferenceContainer([Difference("doors", 5), Difference("color", "yellow")])
        observed = DifferenceContainer.from_avro_bytes(initial.avro_bytes)
        self.assertEqual(initial, observed)


if __name__ == "__main__":
    unittest.main()

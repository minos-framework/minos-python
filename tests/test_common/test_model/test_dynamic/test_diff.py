import unittest
from datetime import (
    datetime,
)
from typing import (
    Optional,
)
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
)

from minos.common import (
    NULL_DATETIME,
    NULL_UUID,
    Action,
    FieldDiff,
    FieldDiffContainer,
    IncrementalFieldDiff,
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


class TestFieldDiffContainer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        async with FakeBroker() as broker, FakeRepository() as repository, FakeSnapshot() as snapshot:
            self.car_one = Car(3, "blue", id=1, version=1, _broker=broker, _repository=repository, _snapshot=snapshot)
            self.car_two = Car(5, "red", id=1, version=2, _broker=broker, _repository=repository, _snapshot=snapshot)
            self.car_three = Car(
                5, "yellow", id=1, version=3, _broker=broker, _repository=repository, _snapshot=snapshot
            )
            self.car_four = Car(3, "blue", id=2, version=1, _broker=broker, _repository=repository, _snapshot=snapshot)

    def test_model_type(self):
        fields = [FieldDiff("doors", int, 5), FieldDiff("color", str, "red")]
        difference = FieldDiffContainer(fields)
        # noinspection PyTypeChecker
        self.assertEqual(ModelType.build(FieldDiffContainer.classname, {"doors": str}), difference.model_type)

    def test_from_difference(self):
        expected = FieldDiffContainer(
            [FieldDiff("version", int, 2), FieldDiff("doors", int, 5), FieldDiff("color", str, "red")]
        )
        observed = FieldDiffContainer.from_difference(self.car_two, self.car_one)
        self.assertEqual(expected, observed)

    def test_from_difference_with_ignore(self):
        expected = FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "red")])
        observed = FieldDiffContainer.from_difference(
            self.car_two, self.car_one, ignore={"uuid", "version", "created_at", "updated_at"}
        )
        self.assertEqual(expected, observed)

    def test_with_difference_not_hashable(self):
        expected = FieldDiffContainer([FieldDiff("numbers", list[int], [1, 2, 3])])

        model_type = ModelType.build("Foo", {"numbers": list[int]})
        a, b = model_type([0]), model_type([1, 2, 3])
        observed = FieldDiffContainer.from_difference(b, a)
        self.assertEqual(expected, observed)

    def test_from_model(self):
        expected = FieldDiffContainer(
            [
                FieldDiff("uuid", UUID, NULL_UUID),
                FieldDiff("version", int, 2),
                FieldDiff("created_at", datetime, NULL_DATETIME),
                FieldDiff("updated_at", datetime, NULL_DATETIME),
                FieldDiff("doors", int, 5),
                FieldDiff("color", str, "red"),
                FieldDiff("owner", Optional[list[ModelRef[Owner]]], None),
            ]
        )
        observed = FieldDiffContainer.from_model(self.car_two)
        self.assertEqual(expected, observed)

    def test_from_model_with_ignore(self):
        expected = FieldDiffContainer(
            [
                FieldDiff("doors", int, 5),
                FieldDiff("color", str, "red"),
                FieldDiff("owner", Optional[list[ModelRef[Owner]]], None),
            ]
        )
        observed = FieldDiffContainer.from_model(self.car_two, ignore={"uuid", "version", "created_at", "updated_at"})
        self.assertEqual(expected, observed)

    def test_avro_schema(self):
        expected = [
            {
                "fields": [
                    {
                        "name": "hello",
                        "type": {
                            "fields": [{"name": "name", "type": "string"}, {"name": "value", "type": "int"}],
                            "name": "FieldDiff",
                            "namespace": "minos.common.model.dynamic.diff.hola",
                            "type": "record",
                        },
                    },
                    {
                        "name": "goodbye",
                        "type": {
                            "fields": [{"name": "name", "type": "string"}, {"name": "value", "type": "string"}],
                            "name": "FieldDiff",
                            "namespace": "minos.common.model.dynamic.diff.adios",
                            "type": "record",
                        },
                    },
                ],
                "name": "FieldDiffContainer",
                "namespace": "minos.common.model.dynamic.diff.uno",
                "type": "record",
            }
        ]
        with patch("minos.common.FieldDiffContainer.generate_random_str", side_effect=["hello", "goodbye"]):
            diff = FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "yellow")])
        with patch("minos.common.AvroSchemaEncoder.generate_random_str", side_effect=["uno", "hola", "adios"]):
            self.assertEqual(expected, diff.avro_schema)

    def test_avro_data(self):
        expected = {"hello": {"name": "doors", "value": 5}, "goodbye": {"name": "color", "value": "yellow"}}

        with patch("minos.common.FieldDiffContainer.generate_random_str", side_effect=["hello", "goodbye"]):
            diff = FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "yellow")])

        self.assertEqual(expected, diff.avro_data)

    def test_avro_bytes(self):
        diff = FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "yellow")])
        self.assertIsInstance(diff.avro_bytes, bytes)

    def test_from_avro_bytes(self):
        initial = FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "yellow")])
        observed = FieldDiffContainer.from_avro_bytes(initial.avro_bytes)
        self.assertEqual(initial, observed)

    def test_repr(self):
        fields = [
            IncrementalFieldDiff("doors", int, 5, Action.CREATE),
            IncrementalFieldDiff("doors", int, 3, Action.CREATE),
            FieldDiff("color", str, "red"),
        ]
        difference = FieldDiffContainer(fields)
        expected = f"FieldDiffContainer(doors=[{fields[0]}, {fields[1]}], color={fields[2]})"
        self.assertEqual(expected, repr(difference))


class TestFieldDiffContainerAccessors(unittest.TestCase):
    def setUp(self) -> None:
        self.fields_diff = FieldDiffContainer(
            [
                IncrementalFieldDiff("doors", int, 5, Action.CREATE),
                IncrementalFieldDiff("doors", int, 3, Action.CREATE),
                FieldDiff("color", str, "red"),
                IncrementalFieldDiff("seats", int, 1, Action.CREATE),
            ]
        )

    def test_as_dict(self):
        expected = {
            "color": FieldDiff("color", str, "red"),
            "doors": [
                IncrementalFieldDiff("doors", int, 5, Action.CREATE),
                IncrementalFieldDiff("doors", int, 3, Action.CREATE),
            ],
            "seats": [IncrementalFieldDiff("seats", int, 1, Action.CREATE)],
        }
        self.assertEqual(expected, dict(self.fields_diff))

    def test_keys(self):
        self.assertEqual({"doors", "color", "seats"}, set(self.fields_diff.keys()))

    def test_values(self):
        expected = [
            [
                IncrementalFieldDiff("doors", int, 5, Action.CREATE),
                IncrementalFieldDiff("doors", int, 3, Action.CREATE),
            ],
            FieldDiff("color", str, "red"),
            [IncrementalFieldDiff("seats", int, 1, Action.CREATE)],
        ]
        self.assertEqual(expected, list(self.fields_diff.values()))

    def test_flatten_values(self):
        expected = [
            IncrementalFieldDiff("doors", int, 5, Action.CREATE),
            IncrementalFieldDiff("doors", int, 3, Action.CREATE),
            FieldDiff("color", str, "red"),
            IncrementalFieldDiff("seats", int, 1, Action.CREATE),
        ]
        self.assertEqual(expected, list(self.fields_diff.flatten_values()))

    def test_flatten_items(self):
        expected = [
            ("doors", IncrementalFieldDiff("doors", int, 5, Action.CREATE)),
            ("doors", IncrementalFieldDiff("doors", int, 3, Action.CREATE)),
            ("color", FieldDiff("color", str, "red")),
            ("seats", IncrementalFieldDiff("seats", int, 1, Action.CREATE)),
        ]
        self.assertEqual(expected, list(self.fields_diff.flatten_items()))

    def test_get_attr_single(self):
        observed = self.fields_diff["color"]
        expected = FieldDiff("color", str, "red")
        self.assertEqual(expected, observed)

    def test_get_attr_multiple_two(self):
        observed = self.fields_diff["doors"]
        expected = [
            IncrementalFieldDiff("doors", int, 5, Action.CREATE),
            IncrementalFieldDiff("doors", int, 3, Action.CREATE),
        ]
        self.assertEqual(expected, observed)

    def test_get_attr_multiple_one(self):
        observed = self.fields_diff["seats"]
        expected = [IncrementalFieldDiff("seats", int, 1, Action.CREATE)]
        self.assertEqual(expected, observed)

    def test_get_one_single(self):
        observed = self.fields_diff.get_one("color")
        expected = FieldDiff("color", str, "red")
        self.assertEqual(expected, observed)

    def test_get_one_multiple_two(self):
        observed = self.fields_diff.get_one("doors")
        expected = [
            IncrementalFieldDiff("doors", int, 5, Action.CREATE),
            IncrementalFieldDiff("doors", int, 3, Action.CREATE),
        ]
        self.assertEqual(expected, observed)

    def test_get_one_multiple_one(self):
        observed = self.fields_diff.get_one("seats")
        expected = [IncrementalFieldDiff("seats", int, 1, Action.CREATE)]
        self.assertEqual(expected, observed)

    def test_get_one_single_value(self):
        observed = self.fields_diff.get_one("color", return_diff=False)
        expected = "red"
        self.assertEqual(expected, observed)

    def test_get_one_multiple_values_two(self):
        observed = self.fields_diff.get_one("doors", return_diff=False)
        expected = [5, 3]
        self.assertEqual(expected, observed)

    def test_get_one_multiple_values_one(self):
        observed = self.fields_diff.get_one("seats", return_diff=False)
        expected = [1]
        self.assertEqual(expected, observed)

    def test_get_all(self):
        observed = self.fields_diff.get_all()
        expected = {
            "color": FieldDiff("color", str, "red"),
            "doors": [
                IncrementalFieldDiff("doors", int, 5, Action.CREATE),
                IncrementalFieldDiff("doors", int, 3, Action.CREATE),
            ],
            "seats": [IncrementalFieldDiff("seats", int, 1, Action.CREATE)],
        }
        self.assertEqual(expected, observed)

    def test_get_all_values(self):
        observed = self.fields_diff.get_all(return_diff=False)
        expected = {"doors": [5, 3], "color": "red", "seats": [1]}
        self.assertEqual(expected, observed)


class TestFieldDiffContainerRaises(unittest.IsolatedAsyncioTestCase):
    def test_multiple_types(self):
        with self.assertRaises(ValueError):
            FieldDiffContainer([IncrementalFieldDiff("doors", int, 5, Action.CREATE), FieldDiff("doors", int, 3)])

    def test_multiple_not_incremental(self):
        with self.assertRaises(ValueError):
            FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("doors", int, 3)])


if __name__ == "__main__":
    unittest.main()

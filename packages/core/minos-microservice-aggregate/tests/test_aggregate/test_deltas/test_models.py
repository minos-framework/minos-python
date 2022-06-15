import unittest
import warnings
from typing import (
    Optional,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    Action,
    Delta,
    FieldDiff,
    FieldDiffContainer,
    IncrementalFieldDiff,
    Ref,
)
from minos.common import (
    classname,
    current_datetime,
)
from tests.utils import (
    AggregateTestCase,
    Car,
    Owner,
)


class TestDelta(AggregateTestCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()

        self.uuid = uuid4()
        self.uuid_another = uuid4()

        self.initial = Car(3, "blue", uuid=self.uuid, version=1)
        self.final = Car(5, "yellow", uuid=self.uuid, version=3)
        self.another = Car(3, "blue", uuid=self.uuid_another, version=1)

        self.diff = Delta(
            uuid=self.uuid,
            type_=classname(Car),
            version=1,
            action=Action.CREATE,
            created_at=self.initial.updated_at,
            fields_diff=FieldDiffContainer(
                [
                    FieldDiff("doors", int, 3),
                    FieldDiff("color", str, "blue"),
                    FieldDiff("owner", Optional[Ref[Owner]], None),
                ]
            ),
        )

    def test_simplified_name(self):
        self.assertEqual("Car", self.diff.simplified_name)

    def test_total_ordering(self):
        values = [
            Delta.from_entity(Car(3, "blue", version=4)),
            Delta.from_entity(Car(3, "blue", version=1)),
            Delta.from_entity(Car(3, "blue", version=3)),
            Delta.from_entity(Car(3, "blue", version=2)),
            Delta.from_entity(Owner("foo", "bar", version=4, updated_at=current_datetime())),
            Delta.from_entity(Owner("foo", "bar", version=3, updated_at=current_datetime())),
        ]
        observed = sorted(values)

        expected = [values[5], values[4], values[1], values[3], values[2], values[0]]
        self.assertEqual(expected, observed)

    def test_from_entity(self):
        observed = Delta.from_entity(self.initial)
        self.assertEqual(self.diff, observed)

    def test_from_entity_deleted(self):
        expected = Delta(
            uuid=self.uuid,
            type_=Car.classname,
            version=1,
            action=Action.DELETE,
            created_at=self.initial.updated_at,
            fields_diff=FieldDiffContainer.empty(),
        )
        observed = Delta.from_entity(self.initial, action=Action.DELETE)
        self.assertEqual(expected, observed)

    def test_from_difference(self):
        expected = Delta(
            uuid=self.uuid,
            type_=Car.classname,
            version=3,
            action=Action.UPDATE,
            created_at=self.final.updated_at,
            fields_diff=FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "yellow")]),
        )
        observed = Delta.from_difference(self.final, self.initial)
        self.assertEqual(expected, observed)

    def test_from_difference_raises(self):
        with self.assertRaises(ValueError):
            Delta.from_difference(self.initial, self.another)

    def test_avro_serialization(self):
        serialized = self.diff.avro_bytes
        self.assertIsInstance(serialized, bytes)

        deserialized = Delta.from_avro_bytes(serialized)
        self.assertEqual(self.diff, deserialized)

    def test_decompose(self):
        aggr = Delta(
            uuid=self.uuid,
            type_=Car.classname,
            version=3,
            action=Action.UPDATE,
            created_at=current_datetime(),
            fields_diff=FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "yellow")]),
        )

        expected = [
            Delta(
                uuid=self.uuid,
                type_=Car.classname,
                version=3,
                action=Action.UPDATE,
                created_at=aggr.created_at,
                fields_diff=FieldDiffContainer([FieldDiff("doors", int, 5)]),
            ),
            Delta(
                uuid=self.uuid,
                type_=Car.classname,
                version=3,
                action=Action.UPDATE,
                created_at=aggr.created_at,
                fields_diff=FieldDiffContainer([FieldDiff("color", str, "yellow")]),
            ),
        ]

        observed = aggr.decompose()
        self.assertEqual(expected, observed)


class TestDeltaAccessors(unittest.TestCase):
    def setUp(self) -> None:
        self.diff = Delta(
            uuid=uuid4(),
            type_=classname(Car),
            version=1,
            action=Action.CREATE,
            created_at=current_datetime(),
            fields_diff=FieldDiffContainer(
                [
                    IncrementalFieldDiff("doors", int, 5, Action.CREATE),
                    IncrementalFieldDiff("doors", int, 3, Action.CREATE),
                    FieldDiff("color", str, "red"),
                ]
            ),
        )

    def test_fields_diff(self):
        expected = FieldDiffContainer(
            [
                IncrementalFieldDiff("doors", int, 5, Action.CREATE),
                IncrementalFieldDiff("doors", int, 3, Action.CREATE),
                FieldDiff("color", str, "red"),
            ]
        )
        self.assertEqual(expected, self.diff.fields_diff)

    def test_getattr_single(self):
        self.assertEqual("red", self.diff.color)

    def test_getattr_multiple(self):
        self.assertEqual([5, 3], self.diff.doors)

    def test_getattr_raises(self):
        with self.assertRaises(AttributeError):
            self.diff.wheels

    def test_get_one_single(self):
        expected = self.diff.get_field("color")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            observed = self.diff.get_one("color")
        self.assertEqual(expected, observed)

    def test_get_one_single_diff(self):
        expected = self.diff.get_field("color", return_diff=True)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            observed = self.diff.get_one("color", return_diff=True)
        self.assertEqual(expected, observed)

    def test_get_one_multiple(self):
        expected = self.diff.get_field("doors")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            observed = self.diff.get_one("doors")
        self.assertEqual(expected, observed)

    def test_get_one_multiple_diff(self):
        expected = self.diff.get_field("doors", return_diff=True)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            observed = self.diff.get_one("doors", return_diff=True)
        self.assertEqual(expected, observed)

    def test_has_field(self):
        self.assertTrue(self.diff.has_field("color"))
        self.assertFalse(self.diff.has_field("something"))

    def test_get_field_single(self):
        observed = self.diff.get_field("color")
        expected = "red"
        self.assertEqual(expected, observed)

    def test_get_field_single_diff(self):
        observed = self.diff.get_field("color", return_diff=True)
        expected = FieldDiff("color", str, "red")
        self.assertEqual(expected, observed)

    def test_get_field_multiple(self):
        observed = self.diff.get_field("doors")
        expected = [5, 3]
        self.assertEqual(expected, observed)

    def test_get_field_multiple_diff(self):
        observed = self.diff.get_field("doors", return_diff=True)
        expected = [
            IncrementalFieldDiff("doors", int, 5, Action.CREATE),
            IncrementalFieldDiff("doors", int, 3, Action.CREATE),
        ]
        self.assertEqual(expected, observed)

    def test_get_all(self):
        expected = self.diff.get_fields()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            observed = self.diff.get_all()
        self.assertEqual(expected, observed)

    def test_get_all_diffs(self):
        expected = self.diff.get_fields(return_diff=True)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            # noinspection PyDeprecation
            observed = self.diff.get_all(return_diff=True)
        self.assertEqual(expected, observed)

    def test_get_fields(self):
        observed = self.diff.get_fields()
        expected = {
            "color": "red",
            "doors": [5, 3],
        }
        self.assertEqual(expected, observed)

    def test_get_fields_diffs(self):
        observed = self.diff.get_fields(return_diff=True)
        expected = {
            "color": FieldDiff("color", str, "red"),
            "doors": [
                IncrementalFieldDiff("doors", int, 5, Action.CREATE),
                IncrementalFieldDiff("doors", int, 3, Action.CREATE),
            ],
        }
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

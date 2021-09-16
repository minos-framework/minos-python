import unittest
from typing import (
    Optional,
)
from uuid import (
    uuid4,
)

from minos.common import (
    Action,
    AggregateDiff,
    FieldDiff,
    FieldDiffContainer,
    IncrementalFieldDiff,
    ModelRef,
    current_datetime,
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
        self.uuid = uuid4()
        self.uuid_another = uuid4()

        async with FakeBroker() as b, FakeRepository() as r, FakeSnapshot() as s:
            self.initial = Car(3, "blue", uuid=self.uuid, version=1, _broker=b, _repository=r, _snapshot=s)
            self.final = Car(5, "yellow", uuid=self.uuid, version=3, _broker=b, _repository=r, _snapshot=s)
            self.another = Car(3, "blue", uuid=self.uuid_another, version=1, _broker=b, _repository=r, _snapshot=s)

        self.diff = AggregateDiff(
            uuid=self.uuid,
            name=Car.classname,
            version=1,
            action=Action.CREATE,
            created_at=self.initial.updated_at,
            fields_diff=FieldDiffContainer(
                [
                    FieldDiff("doors", int, 3),
                    FieldDiff("color", str, "blue"),
                    FieldDiff("owner", Optional[list[ModelRef[Owner]]], None),
                ]
            ),
        )

    def test_from_aggregate(self):
        observed = AggregateDiff.from_aggregate(self.initial)
        self.assertEqual(self.diff, observed)

    def test_from_deleted_aggregate(self):
        expected = AggregateDiff(
            uuid=self.uuid,
            name=Car.classname,
            version=1,
            action=Action.DELETE,
            created_at=self.initial.updated_at,
            fields_diff=FieldDiffContainer.empty(),
        )
        observed = AggregateDiff.from_deleted_aggregate(self.initial)
        self.assertEqual(expected, observed)

    def test_from_difference(self):
        expected = AggregateDiff(
            uuid=self.uuid,
            name=Car.classname,
            version=3,
            action=Action.UPDATE,
            created_at=self.final.updated_at,
            fields_diff=FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "yellow")]),
        )
        observed = AggregateDiff.from_difference(self.final, self.initial)
        self.assertEqual(expected, observed)

    def test_from_difference_raises(self):
        with self.assertRaises(ValueError):
            AggregateDiff.from_difference(self.initial, self.another)

    def test_avro_serialization(self):
        serialized = self.diff.avro_bytes
        self.assertIsInstance(serialized, bytes)

        deserialized = AggregateDiff.from_avro_bytes(serialized)
        self.assertEqual(self.diff, deserialized)

    def test_decompose(self):
        aggr = AggregateDiff(
            uuid=self.uuid,
            name=Car.classname,
            version=3,
            action=Action.UPDATE,
            created_at=current_datetime(),
            fields_diff=FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "yellow")]),
        )

        expected = [
            AggregateDiff(
                uuid=self.uuid,
                name=Car.classname,
                version=3,
                action=Action.UPDATE,
                created_at=aggr.created_at,
                fields_diff=FieldDiffContainer([FieldDiff("doors", int, 5)]),
            ),
            AggregateDiff(
                uuid=self.uuid,
                name=Car.classname,
                version=3,
                action=Action.UPDATE,
                created_at=aggr.created_at,
                fields_diff=FieldDiffContainer([FieldDiff("color", str, "yellow")]),
            ),
        ]

        observed = aggr.decompose()
        self.assertEqual(expected, observed)


class TestAggregateDiffAccessors(unittest.TestCase):
    def setUp(self) -> None:
        self.diff = AggregateDiff(
            uuid=uuid4(),
            name="src.domain.Car",
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
        observed = self.diff.get_one("color")
        expected = "red"
        self.assertEqual(expected, observed)

    def test_get_one_single_diff(self):
        observed = self.diff.get_one("color", return_diff=True)
        expected = FieldDiff("color", str, "red")
        self.assertEqual(expected, observed)

    def test_get_one_multiple(self):
        observed = self.diff.get_one("doors")
        expected = [5, 3]
        self.assertEqual(expected, observed)

    def test_get_one_multiple_diff(self):
        observed = self.diff.get_one("doors", return_diff=True)
        expected = [
            IncrementalFieldDiff("doors", int, 5, Action.CREATE),
            IncrementalFieldDiff("doors", int, 3, Action.CREATE),
        ]
        self.assertEqual(expected, observed)

    def test_get_all(self):
        observed = self.diff.get_all()
        expected = {
            "color": "red",
            "doors": [5, 3],
        }
        self.assertEqual(expected, observed)

    def test_get_all_diffs(self):
        observed = self.diff.get_all(return_diff=True)
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

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
    uuid4,
)

from minos.common import (
    Action,
    AggregateDiff,
    FieldDiff,
    FieldDiffContainer,
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
            fields_diff=FieldDiffContainer(
                [
                    FieldDiff("doors", int, 3),
                    FieldDiff("color", str, "blue"),
                    FieldDiff("owner", Optional[list[ModelRef[Owner]]], None),
                ]
            ),
        )

    def test_getattr(self):
        self.assertEqual(3, self.diff.doors)

    def test_getattr_raises(self):
        with self.assertRaises(AttributeError):
            self.diff.wheels

    def test_fields_diff(self):
        fields_diff = FieldDiffContainer(
            [
                FieldDiff("doors", int, 3),
                FieldDiff("color", str, "blue"),
                FieldDiff("owner", Optional[list[ModelRef[Owner]]], None),
            ]
        )
        diff = AggregateDiff(self.uuid, Car.classname, 1, Action.CREATE, fields_diff)
        self.assertEqual(fields_diff, diff.fields_diff)

    def test_from_aggregate(self):
        observed = AggregateDiff.from_aggregate(self.initial)
        self.assertEqual(self.diff, observed)

    def test_from_deleted_aggregate(self):
        expected = AggregateDiff(self.uuid, Car.classname, 1, Action.DELETE, FieldDiffContainer.empty())
        observed = AggregateDiff.from_deleted_aggregate(self.initial)
        self.assertEqual(expected, observed)

    def test_from_difference(self):
        expected = AggregateDiff(
            uuid=self.uuid,
            name=Car.classname,
            version=3,
            action=Action.UPDATE,
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
        expected = [
            AggregateDiff(
                uuid=self.uuid,
                name=Car.classname,
                version=3,
                action=Action.UPDATE,
                fields_diff=FieldDiffContainer([FieldDiff("doors", int, 5)]),
            ),
            AggregateDiff(
                uuid=self.uuid,
                name=Car.classname,
                version=3,
                action=Action.UPDATE,
                fields_diff=FieldDiffContainer([FieldDiff("color", str, "yellow")]),
            ),
        ]

        aggr = AggregateDiff(
            uuid=self.uuid,
            name=Car.classname,
            version=3,
            action=Action.UPDATE,
            fields_diff=FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "yellow")]),
        )
        observed = aggr.decompose()
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

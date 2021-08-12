"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    Action,
    AggregateDiff,
    Field,
    FieldsDiff,
)
from tests.aggregate_classes import (
    Car,
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

    def test_diff(self):
        expected = AggregateDiff(
            uuid=self.uuid,
            name=Car.classname,
            version=3,
            action=Action.UPDATE,
            differences=FieldsDiff({"doors": Field("doors", int, 5), "color": Field("color", str, "yellow")}),
        )
        observed = self.final.diff(self.initial)
        self.assertEqual(expected, observed)

    def test_apply_diff(self):
        diff = AggregateDiff(
            uuid=self.uuid,
            name=Car.classname,
            version=3,
            action=Action.UPDATE,
            differences=FieldsDiff({"doors": Field("doors", int, 5), "color": Field("color", str, "yellow")}),
        )
        self.initial.apply_diff(diff)
        self.assertEqual(self.final, self.initial)

    def test_apply_diff_raises(self):
        diff = AggregateDiff(
            uuid=self.uuid_another,
            name=Car.classname,
            version=3,
            action=Action.UPDATE,
            differences=FieldsDiff({"doors": Field("doors", int, 5), "color": Field("color", str, "yellow")}),
        )
        with self.assertRaises(ValueError):
            self.initial.apply_diff(diff)

    def test_get_attr(self):
        diff = AggregateDiff(
            uuid=self.uuid,
            name=Car.classname,
            version=3,
            action=Action.UPDATE,
            differences=FieldsDiff({"doors": Field("doors", int, 5), "color": Field("color", str, "yellow")}),
        )
        self.assertEqual(5, diff.doors)

    def test_get_attr_raises(self):
        diff = AggregateDiff(
            uuid=self.uuid, name=Car.classname, version=3, action=Action.UPDATE, differences=FieldsDiff.empty(),
        )
        with self.assertRaises(AttributeError):
            diff.doors


if __name__ == "__main__":
    unittest.main()

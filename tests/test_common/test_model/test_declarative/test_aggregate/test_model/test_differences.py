import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    Action,
    AggregateDiff,
    FieldDiff,
    FieldDiffContainer,
    current_datetime,
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
        self.uuid = uuid4()
        self.uuid_another = uuid4()

        self.initial_datetime = current_datetime()
        self.final_datetime = current_datetime()
        self.another_datetime = current_datetime()

        async with FakeBroker() as b, FakeRepository() as r, FakeSnapshot() as s:
            self.initial = Car(
                3,
                "blue",
                uuid=self.uuid,
                version=1,
                created_at=self.initial_datetime,
                updated_at=self.initial_datetime,
                _broker=b,
                _repository=r,
                _snapshot=s,
            )
            self.final = Car(
                5,
                "yellow",
                uuid=self.uuid,
                version=3,
                created_at=self.initial_datetime,
                updated_at=self.final_datetime,
                _broker=b,
                _repository=r,
                _snapshot=s,
            )
            self.another = Car(
                3,
                "blue",
                uuid=self.uuid_another,
                created_at=self.another_datetime,
                updated_at=self.another_datetime,
                version=1,
                _broker=b,
                _repository=r,
                _snapshot=s,
            )

    def test_diff(self):
        expected = AggregateDiff(
            uuid=self.uuid,
            name=Car.classname,
            version=3,
            action=Action.UPDATE,
            created_at=self.final_datetime,
            fields_diff=FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "yellow")]),
        )
        observed = self.final.diff(self.initial)
        self.assertEqual(expected, observed)

    def test_apply_diff(self):
        diff = AggregateDiff(
            uuid=self.uuid,
            name=Car.classname,
            version=3,
            action=Action.UPDATE,
            created_at=self.final_datetime,
            fields_diff=FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "yellow")]),
        )
        self.initial.apply_diff(diff)
        self.assertEqual(self.final, self.initial)

    def test_apply_diff_raises(self):
        diff = AggregateDiff(
            uuid=self.uuid_another,
            name=Car.classname,
            version=3,
            action=Action.UPDATE,
            created_at=current_datetime(),
            fields_diff=FieldDiffContainer([FieldDiff("doors", int, 5), FieldDiff("color", str, "yellow")]),
        )
        with self.assertRaises(ValueError):
            self.initial.apply_diff(diff)


if __name__ == "__main__":
    unittest.main()

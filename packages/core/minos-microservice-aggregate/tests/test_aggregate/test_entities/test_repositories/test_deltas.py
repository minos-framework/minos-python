import unittest
from typing import (
    Optional,
)

from minos.aggregate import (
    Action,
    Delta,
    EntityRepository,
    FieldDiff,
    FieldDiffContainer,
    Ref,
)
from tests.utils import (
    AggregateTestCase,
    Car,
    Owner,
)


class TestEntityRepositoryDelta(AggregateTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.repository = EntityRepository()

    async def test_create(self):
        car, delta = await self.repository.create(Car, doors=3, color="blue")

        self.assertEqual(
            Delta(
                uuid=car.uuid,
                type_=Car.classname,
                version=1,
                action=Action.CREATE,
                created_at=car.created_at,
                fields_diff=FieldDiffContainer(
                    [
                        FieldDiff("doors", int, 3),
                        FieldDiff("color", str, "blue"),
                        FieldDiff("owner", Optional[Ref[Owner]], None),
                    ]
                ),
            ),
            delta,
        )

    async def test_update(self):
        car, _ = await self.repository.create(Car, doors=3, color="blue")

        _, delta = await self.repository.update(car, color="red")

        self.assertEqual(
            Delta(
                uuid=car.uuid,
                type_=Car.classname,
                version=2,
                action=Action.UPDATE,
                created_at=car.updated_at,
                fields_diff=FieldDiffContainer([FieldDiff("color", str, "red")]),
            ),
            delta,
        )

    async def test_delete(self):
        car, _ = await self.repository.create(Car, doors=3, color="blue")

        delta = await self.repository.delete(car)

        self.assertEqual(
            Delta(
                uuid=car.uuid,
                type_=Car.classname,
                version=2,
                action=Action.DELETE,
                created_at=car.updated_at,
                fields_diff=FieldDiffContainer.empty(),
            ),
            delta,
        )


if __name__ == "__main__":
    unittest.main()

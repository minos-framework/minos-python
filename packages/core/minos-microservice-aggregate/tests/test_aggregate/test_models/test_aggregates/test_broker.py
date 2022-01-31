import unittest
from typing import (
    Optional,
)

from minos.aggregate import (
    Action,
    AggregateDiff,
    FieldDiff,
    FieldDiffContainer,
    Ref,
)
from minos.networks import (
    BrokerMessageV1,
)
from tests.utils import (
    Car,
    MinosTestCase,
    Owner,
)


class TestAggregate(MinosTestCase):
    async def test_create(self):
        car = await Car.create(doors=3, color="blue")

        observed = self.broker_publisher.messages

        self.assertEqual(1, len(observed))
        self.assertIsInstance(observed[0], BrokerMessageV1)
        self.assertEqual("CarCreated", observed[0].topic)
        self.assertEqual(
            AggregateDiff(
                uuid=car.uuid,
                name=Car.classname,
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
            observed[0].content,
        )

    async def test_update(self):
        car = await Car.create(doors=3, color="blue")
        self.broker_publisher.messages.clear()

        await car.update(color="red")

        observed = self.broker_publisher.messages

        self.assertEqual(2, len(observed))
        self.assertIsInstance(observed[0], BrokerMessageV1)
        self.assertEqual("CarUpdated", observed[0].topic)
        self.assertEqual(
            AggregateDiff(
                uuid=car.uuid,
                name=Car.classname,
                version=2,
                action=Action.UPDATE,
                created_at=car.updated_at,
                fields_diff=FieldDiffContainer([FieldDiff("color", str, "red")]),
            ),
            observed[0].content,
        )
        self.assertIsInstance(observed[1], BrokerMessageV1)
        self.assertEqual("CarUpdated.color", observed[1].topic)
        self.assertEqual(
            AggregateDiff(
                uuid=car.uuid,
                name=Car.classname,
                version=2,
                action=Action.UPDATE,
                created_at=car.updated_at,
                fields_diff=FieldDiffContainer([FieldDiff("color", str, "red")]),
            ),
            observed[1].content,
        )

    async def test_delete(self):
        car = await Car.create(doors=3, color="blue")
        self.broker_publisher.messages.clear()

        await car.delete()

        observed = self.broker_publisher.messages

        self.assertEqual(1, len(observed))
        self.assertIsInstance(observed[0], BrokerMessageV1)
        self.assertEqual("CarDeleted", observed[0].topic)
        self.assertEqual(
            AggregateDiff(
                uuid=car.uuid,
                name=Car.classname,
                version=2,
                action=Action.DELETE,
                created_at=car.updated_at,
                fields_diff=FieldDiffContainer.empty(),
            ),
            observed[0].content,
        )


if __name__ == "__main__":
    unittest.main()

import unittest
from typing import (
    Optional,
)
from unittest.mock import (
    AsyncMock,
    call,
)

from minos.aggregate import (
    Action,
    AggregateDiff,
    FieldDiff,
    FieldDiffContainer,
    ModelRef,
)
from minos.networks import (
    BrokerMessageStrategy,
)
from tests.utils import (
    Car,
    MinosTestCase,
    Owner,
)


class TestAggregate(MinosTestCase):
    async def test_create(self):
        mock = AsyncMock()
        self.broker_publisher.send = mock

        car = await Car.create(doors=3, color="blue")

        self.assertEqual(
            [
                call(
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
                                FieldDiff("owner", Optional[ModelRef[Owner]], None),
                            ]
                        ),
                    ),
                    "CarCreated",
                    strategy=BrokerMessageStrategy.MULTICAST,
                )
            ],
            mock.call_args_list,
        )

    async def test_update(self):
        mock = AsyncMock()
        self.broker_publisher.send = mock

        car = await Car.create(doors=3, color="blue")
        mock.reset_mock()

        await car.update(color="red")

        self.assertEqual(
            [
                call(
                    AggregateDiff(
                        uuid=car.uuid,
                        name=Car.classname,
                        version=2,
                        action=Action.UPDATE,
                        created_at=car.updated_at,
                        fields_diff=FieldDiffContainer([FieldDiff("color", str, "red")]),
                    ),
                    "CarUpdated",
                    strategy=BrokerMessageStrategy.MULTICAST,
                ),
                call(
                    AggregateDiff(
                        uuid=car.uuid,
                        name=Car.classname,
                        version=2,
                        action=Action.UPDATE,
                        created_at=car.updated_at,
                        fields_diff=FieldDiffContainer([FieldDiff("color", str, "red")]),
                    ),
                    "CarUpdated.color",
                    strategy=BrokerMessageStrategy.MULTICAST,
                ),
            ],
            mock.call_args_list,
        )

    async def test_delete(self):
        mock = AsyncMock()
        self.broker_publisher.send = mock

        car = await Car.create(doors=3, color="blue")
        mock.reset_mock()

        await car.delete()

        self.assertEqual(
            [
                call(
                    AggregateDiff(
                        uuid=car.uuid,
                        name=Car.classname,
                        version=2,
                        action=Action.DELETE,
                        created_at=car.updated_at,
                        fields_diff=FieldDiffContainer.empty(),
                    ),
                    "CarDeleted",
                    strategy=BrokerMessageStrategy.MULTICAST,
                )
            ],
            mock.call_args_list,
        )


if __name__ == "__main__":
    unittest.main()

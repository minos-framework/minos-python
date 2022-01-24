import unittest
from typing import (
    Optional,
)
from unittest.mock import (
    AsyncMock,
)

from minos.aggregate import (
    Action,
    AggregateDiff,
    FieldDiff,
    FieldDiffContainer,
    ModelRef,
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
        send_mock = AsyncMock()
        self.broker_publisher.send = send_mock

        car = await Car.create(doors=3, color="blue")

        self.assertEqual(1, len(send_mock.call_args_list))
        self.assertIsInstance(send_mock.call_args_list[0].args[0], BrokerMessageV1)
        self.assertEqual("CarCreated", send_mock.call_args_list[0].args[0].topic)
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
                        FieldDiff("owner", Optional[ModelRef[Owner]], None),
                    ]
                ),
            ),
            send_mock.call_args_list[0].args[0].content,
        )

    async def test_update(self):
        send_mock = AsyncMock()
        self.broker_publisher.send = send_mock

        car = await Car.create(doors=3, color="blue")
        send_mock.reset_mock()

        await car.update(color="red")

        self.assertEqual(2, len(send_mock.call_args_list))
        self.assertIsInstance(send_mock.call_args_list[0].args[0], BrokerMessageV1)
        self.assertEqual("CarUpdated", send_mock.call_args_list[0].args[0].topic)
        self.assertEqual(
            AggregateDiff(
                uuid=car.uuid,
                name=Car.classname,
                version=2,
                action=Action.UPDATE,
                created_at=car.updated_at,
                fields_diff=FieldDiffContainer([FieldDiff("color", str, "red")]),
            ),
            send_mock.call_args_list[0].args[0].content,
        )
        self.assertIsInstance(send_mock.call_args_list[1].args[0], BrokerMessageV1)
        self.assertEqual("CarUpdated.color", send_mock.call_args_list[1].args[0].topic)
        self.assertEqual(
            AggregateDiff(
                uuid=car.uuid,
                name=Car.classname,
                version=2,
                action=Action.UPDATE,
                created_at=car.updated_at,
                fields_diff=FieldDiffContainer([FieldDiff("color", str, "red")]),
            ),
            send_mock.call_args_list[1].args[0].content,
        )

    async def test_delete(self):
        send_mock = AsyncMock()
        self.broker_publisher.send = send_mock

        car = await Car.create(doors=3, color="blue")
        send_mock.reset_mock()

        await car.delete()

        self.assertEqual(1, len(send_mock.call_args_list))
        self.assertIsInstance(send_mock.call_args_list[0].args[0], BrokerMessageV1)
        self.assertEqual("CarDeleted", send_mock.call_args_list[0].args[0].topic)
        self.assertEqual(
            AggregateDiff(
                uuid=car.uuid,
                name=Car.classname,
                version=2,
                action=Action.DELETE,
                created_at=car.updated_at,
                fields_diff=FieldDiffContainer.empty(),
            ),
            send_mock.call_args_list[0].args[0].content,
        )


if __name__ == "__main__":
    unittest.main()

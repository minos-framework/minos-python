import unittest
from typing import (
    Optional,
)
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    Action,
    Aggregate,
    Event,
    FieldDiff,
    FieldDiffContainer,
    IncrementalFieldDiff,
    Ref,
)
from minos.common import (
    Config,
    NotProvidedException,
    current_datetime,
)
from minos.networks import (
    BrokerMessageV1,
    InMemoryBrokerPublisher,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    AggregateTestCase,
    Car,
    Order,
    OrderAggregate,
    Owner,
)


class TestAggregate(AggregateTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.aggregate = OrderAggregate.from_config(self.config)

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await self.aggregate.setup()

    async def asyncTearDown(self) -> None:
        await self.aggregate.destroy()
        await super().asyncTearDown()

    async def test_root(self):
        self.assertEqual(Order, self.aggregate.root)

    def test_root_raises(self):
        with self.assertRaises(TypeError):
            Aggregate.from_config(CONFIG_FILE_PATH)

    async def test_from_config(self):
        self.assertEqual(self.transaction_repository, self.aggregate.transaction_repository)
        self.assertEqual(self.event_repository, self.aggregate.event_repository)
        self.assertEqual(self.snapshot_repository, self.aggregate.snapshot_repository)
        self.assertEqual(self.broker_publisher, self.aggregate.broker_publisher)

    async def test_from_config_with_custom_publisher(self):
        with patch.object(Config, "get_aggregate", return_value={"publisher": {"client": InMemoryBrokerPublisher}}):
            async with OrderAggregate.from_config(CONFIG_FILE_PATH) as aggregate:
                self.assertEqual(self.transaction_repository, aggregate.transaction_repository)
                self.assertEqual(self.event_repository, aggregate.event_repository)
                self.assertEqual(self.snapshot_repository, aggregate.snapshot_repository)
                self.assertIsInstance(aggregate.broker_publisher, InMemoryBrokerPublisher)
                self.assertNotEqual(self.broker_publisher, aggregate.broker_publisher)

    def test_from_config_raises(self):
        with self.assertRaises(NotProvidedException):
            OrderAggregate.from_config(CONFIG_FILE_PATH, transaction_repository=None)
        with self.assertRaises(NotProvidedException):
            OrderAggregate.from_config(CONFIG_FILE_PATH, event_repository=None)
        with self.assertRaises(NotProvidedException):
            OrderAggregate.from_config(CONFIG_FILE_PATH, snapshot_repository=None)
        with self.assertRaises(NotProvidedException):
            OrderAggregate.from_config(CONFIG_FILE_PATH, broker_publisher=None)

    async def test_call(self):
        uuid = await self.aggregate.create_order()
        self.assertIsInstance(uuid, UUID)

    async def test_send_domain_event_none(self):
        await self.aggregate.send_domain_event(None)
        self.assertEqual(list(), self.broker_publisher.messages)

    async def test_send_domain_event_create(self):
        delta = Event(
            uuid=uuid4(),
            name=Car.classname,
            version=1,
            action=Action.CREATE,
            created_at=current_datetime(),
            fields_diff=FieldDiffContainer(
                [
                    FieldDiff("doors", int, 3),
                    FieldDiff("color", str, "blue"),
                    FieldDiff("owner", Optional[Ref[Owner]], None),
                ]
            ),
        )
        await self.aggregate.send_domain_event(delta)

        observed = self.broker_publisher.messages

        self.assertEqual(1, len(observed))
        self.assertIsInstance(observed[0], BrokerMessageV1)
        self.assertEqual("CarCreated", observed[0].topic)
        self.assertEqual(delta, observed[0].content)

    async def test_send_domain_event_update(self):
        delta = Event(
            uuid=uuid4(),
            name=Car.classname,
            version=2,
            action=Action.UPDATE,
            created_at=current_datetime(),
            fields_diff=FieldDiffContainer(
                [
                    FieldDiff("color", str, "red"),
                    IncrementalFieldDiff("doors", int, 5, Action.CREATE),
                ]
            ),
        )

        await self.aggregate.send_domain_event(delta)

        observed = self.broker_publisher.messages

        self.assertEqual(3, len(observed))
        self.assertIsInstance(observed[0], BrokerMessageV1)
        self.assertEqual("CarUpdated", observed[0].topic)
        self.assertEqual(delta, observed[0].content)

        self.assertIsInstance(observed[1], BrokerMessageV1)
        self.assertEqual("CarUpdated.color", observed[1].topic)
        self.assertEqual(
            Event(
                uuid=delta.uuid,
                name=Car.classname,
                version=2,
                action=Action.UPDATE,
                created_at=delta.created_at,
                fields_diff=FieldDiffContainer([FieldDiff("color", str, "red")]),
            ),
            observed[1].content,
        )

        self.assertIsInstance(observed[2], BrokerMessageV1)
        self.assertEqual("CarUpdated.doors.create", observed[2].topic)
        self.assertEqual(
            Event(
                uuid=delta.uuid,
                name=Car.classname,
                version=2,
                action=Action.UPDATE,
                created_at=delta.created_at,
                fields_diff=FieldDiffContainer([IncrementalFieldDiff("doors", int, 5, Action.CREATE)]),
            ),
            observed[2].content,
        )

    async def test_send_domain_event_delete(self):
        delta = Event(
            uuid=uuid4(),
            name=Car.classname,
            version=2,
            action=Action.DELETE,
            created_at=current_datetime(),
            fields_diff=FieldDiffContainer.empty(),
        )

        await self.aggregate.send_domain_event(delta)

        observed = self.broker_publisher.messages

        self.assertEqual(1, len(observed))
        self.assertIsInstance(observed[0], BrokerMessageV1)
        self.assertEqual("CarDeleted", observed[0].topic)
        self.assertEqual(delta, observed[0].content)


if __name__ == "__main__":
    unittest.main()

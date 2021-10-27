import unittest

from minos.common import (
    EntitySet,
    MinosSnapshotDeletedAggregateException,
    PostgreSqlRepository,
    PostgreSqlSnapshot,
    ValueObjectSet,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.aggregate_classes import (
    Car,
    Order,
    OrderItem,
    Review,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
    FakeTransactionRepository,
)


class TestAggregateWithPostgreSql(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.transaction_repository = FakeTransactionRepository()
        self.repository = PostgreSqlRepository.from_config(
            self.config, event_broker=FakeBroker(), transaction_repository=self.transaction_repository
        )
        self.snapshot = PostgreSqlSnapshot.from_config(
            self.config, repository=self.repository, transaction_repository=self.transaction_repository
        )

        self.kwargs = {
            "_repository": self.repository,
            "_snapshot": self.snapshot,
        }

    async def test_create_update_delete(self):
        async with self.repository, self.snapshot:
            car = await Car.create(doors=3, color="blue", **self.kwargs)
            uuid = car.uuid

            await car.update(color="red")
            expected = Car(
                3, "red", uuid=uuid, version=2, created_at=car.created_at, updated_at=car.updated_at, **self.kwargs
            )
            self.assertEqual(expected, car)
            self.assertEqual(car, await Car.get(car.uuid, **self.kwargs))

            await car.update(doors=5)
            expected = Car(
                5, "red", uuid=uuid, version=3, created_at=car.created_at, updated_at=car.updated_at, **self.kwargs
            )
            self.assertEqual(expected, car)
            self.assertEqual(car, await Car.get(car.uuid, **self.kwargs))

            await car.delete()
            with self.assertRaises(MinosSnapshotDeletedAggregateException):
                await Car.get(car.uuid, **self.kwargs)

            car = await Car.create(doors=3, color="blue", **self.kwargs)
            uuid = car.uuid

            await car.update(color="red")
            expected = Car(
                3, "red", uuid=uuid, version=2, created_at=car.created_at, updated_at=car.updated_at, **self.kwargs
            )
            self.assertEqual(expected, await Car.get(car.uuid, **self.kwargs))

            await car.delete()
            with self.assertRaises(MinosSnapshotDeletedAggregateException):
                await Car.get(car.uuid, **self.kwargs)

    async def test_entity_set_value_object_set(self):
        async with self.repository, self.snapshot:
            order = await Order.create(products=EntitySet(), reviews=ValueObjectSet(), **self.kwargs)
            item = OrderItem(24)
            order.products.add(item)

            await order.save()

            recovered = await Order.get(order.uuid, **self.kwargs)
            self.assertEqual(order, recovered)

            item.amount = 36
            order.products.add(item)
            await order.save()

            recovered = await Order.get(order.uuid, **self.kwargs)
            self.assertEqual(order, recovered)

            order.products.remove(item)
            await order.save()

            recovered = await Order.get(order.uuid, **self.kwargs)
            self.assertEqual(order, recovered)

            order.reviews.add(Review("GoodReview"))
            await order.save()

            recovered = await Order.get(order.uuid, **self.kwargs)
            self.assertEqual(order, recovered)

            order.reviews.discard(Review("GoodReview"))
            await order.save()

            recovered = await Order.get(order.uuid, **self.kwargs)
            self.assertEqual(order, recovered)


if __name__ == "__main__":
    unittest.main()

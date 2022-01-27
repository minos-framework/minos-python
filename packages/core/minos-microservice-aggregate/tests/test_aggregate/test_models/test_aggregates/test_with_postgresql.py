import unittest

from minos.aggregate import (
    DeletedAggregateException,
    EntitySet,
    PostgreSqlEventRepository,
    PostgreSqlSnapshotRepository,
    PostgreSqlTransactionRepository,
    ValueObjectSet,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from tests.utils import (
    BASE_PATH,
    Car,
    MinosTestCase,
    Order,
    OrderItem,
    Review,
)


class TestAggregateWithPostgreSql(MinosTestCase, PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self):
        super().setUp()

        self.transaction_repository = PostgreSqlTransactionRepository.from_config(self.config)

        self.event_repository = PostgreSqlEventRepository.from_config(
            self.config, transaction_repository=self.transaction_repository
        )
        self.snapshot_repository = PostgreSqlSnapshotRepository.from_config(
            self.config, event_repository=self.event_repository, transaction_repository=self.transaction_repository
        )

    async def test_create_update_delete(self):
        car = await Car.create(doors=3, color="blue")
        uuid = car.uuid

        await car.update(color="red")
        expected = Car(3, "red", uuid=uuid, version=2, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, car)
        self.assertEqual(car, await Car.get(car.uuid))

        await car.update(doors=5)
        expected = Car(5, "red", uuid=uuid, version=3, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, car)
        self.assertEqual(car, await Car.get(car.uuid))

        await car.delete()
        with self.assertRaises(DeletedAggregateException):
            await Car.get(car.uuid)

        car = await Car.create(doors=3, color="blue")
        uuid = car.uuid

        await car.update(color="red")
        expected = Car(3, "red", uuid=uuid, version=2, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, await Car.get(car.uuid))

        await car.delete()
        with self.assertRaises(DeletedAggregateException):
            await Car.get(car.uuid)

    async def test_entity_set_value_object_set(self):
        order = await Order.create(products=EntitySet(), reviews=ValueObjectSet())
        item = OrderItem(24)
        order.products.add(item)

        await order.save()

        recovered = await Order.get(order.uuid)
        self.assertEqual(order, recovered)

        item.amount = 36
        order.products.add(item)
        await order.save()

        recovered = await Order.get(order.uuid)
        self.assertEqual(order, recovered)

        order.products.remove(item)
        await order.save()

        recovered = await Order.get(order.uuid)
        self.assertEqual(order, recovered)

        order.reviews.add(Review("GoodReview"))
        await order.save()

        recovered = await Order.get(order.uuid)
        self.assertEqual(order, recovered)

        order.reviews.discard(Review("GoodReview"))
        await order.save()

        recovered = await Order.get(order.uuid)
        self.assertEqual(order, recovered)


if __name__ == "__main__":
    unittest.main()

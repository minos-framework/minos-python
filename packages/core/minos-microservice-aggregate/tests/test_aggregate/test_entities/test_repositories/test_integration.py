import unittest
from uuid import (
    uuid4,
)

from minos.aggregate import (
    AlreadyDeletedException,
    EntityRepository,
    EntitySet,
    ValueObjectSet,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
)
from tests.utils import (
    AggregateTestCase,
    Car,
    Order,
    OrderItem,
    Review,
)


class IntegrationTestEntityRepository(AggregateTestCase, DatabaseMinosTestCase):
    def setUp(self):
        super().setUp()

        self.repository = EntityRepository()

    async def test_create_update_delete(self):
        car, _ = await self.repository.create(Car, doors=3, color="blue")
        uuid = car.uuid

        await self.repository.update(car, color="red")
        expected = Car(3, "red", uuid=uuid, version=2, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, car)
        self.assertEqual(car, await self.repository.get(Car, car.uuid))

        await self.repository.update(car, doors=5)
        expected = Car(5, "red", uuid=uuid, version=3, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, car)
        self.assertEqual(car, await self.repository.get(Car, car.uuid))

        await self.repository.delete(car)
        with self.assertRaises(AlreadyDeletedException):
            await self.repository.get(Car, car.uuid)

        car, _ = await self.repository.create(Car, doors=3, color="blue")
        uuid = car.uuid

        await self.repository.update(car, color="red")
        expected = Car(3, "red", uuid=uuid, version=2, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, await self.repository.get(Car, car.uuid))

        await self.repository.delete(car)
        with self.assertRaises(AlreadyDeletedException):
            await self.repository.get(Car, car.uuid)

    async def test_entity_set_value_object_set(self):
        order, _ = await self.repository.create(Order, products=EntitySet(), reviews=ValueObjectSet())
        item = OrderItem("foo", uuid=uuid4())
        order.products.add(item)

        await self.repository.save(order)

        recovered = await self.repository.get(Order, order.uuid)
        self.assertEqual(order, recovered)

        item.name = "bar"
        order.products.add(item)
        await self.repository.save(order)

        recovered = await self.repository.get(Order, order.uuid)
        self.assertEqual(order, recovered)

        order.products.remove(item)
        await self.repository.save(order)

        recovered = await self.repository.get(Order, order.uuid)
        self.assertEqual(order, recovered)

        order.reviews.add(Review("GoodReview"))
        await self.repository.save(order)

        recovered = await self.repository.get(Order, order.uuid)
        self.assertEqual(order, recovered)

        order.reviews.discard(Review("GoodReview"))
        await self.repository.save(order)

        recovered = await self.repository.get(Order, order.uuid)
        self.assertEqual(order, recovered)


if __name__ == "__main__":
    unittest.main()

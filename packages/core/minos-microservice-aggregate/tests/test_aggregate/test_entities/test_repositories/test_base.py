import unittest
from asyncio import (
    gather,
)
from operator import (
    itemgetter,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    AlreadyDeletedException,
    Condition,
    Delta,
    DeltaRepositoryException,
    EntityRepository,
    NotFoundException,
    Ordering,
)
from minos.common import (
    NULL_UUID,
    NotProvidedException,
    current_datetime,
)
from tests.utils import (
    AggregateTestCase,
    Car,
)


class TestEntityRepository(AggregateTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.repository = EntityRepository()

    def test_delta(self):
        self.assertEqual(self.delta_repository, self.repository.delta)

    def test_snapshot(self):
        self.assertEqual(self.snapshot_repository, self.repository.snapshot)

    def test_constructor_raises(self):
        with self.assertRaises(NotProvidedException):
            EntityRepository(delta=None)
        with self.assertRaises(NotProvidedException):
            EntityRepository(snapshot=None)

    async def test_create_from_type(self):
        observed, _ = await self.repository.create(Car, doors=3, color="blue")
        expected = Car(
            3,
            "blue",
            uuid=observed.uuid,
            version=1,
            created_at=observed.created_at,
            updated_at=observed.updated_at,
        )
        self.assertEqual(expected, observed)

    async def test_create_from_instance(self):
        instance = Car(doors=3, color="blue")
        observed, _ = await self.repository.create(instance)
        self.assertEqual(instance, observed)

    async def test_create_raises(self):
        with self.assertRaises(DeltaRepositoryException):
            await self.repository.create(Car, uuid=uuid4(), doors=3, color="blue")
        with self.assertRaises(DeltaRepositoryException):
            await self.repository.create(Car, version=1, doors=3, color="blue")
        with self.assertRaises(DeltaRepositoryException):
            await self.repository.create(Car, created_at=current_datetime(), doors=3, color="blue")
        with self.assertRaises(DeltaRepositoryException):
            await self.repository.create(Car, updated_at=current_datetime(), doors=3, color="blue")
        with self.assertRaises(DeltaRepositoryException):
            await self.repository.create(Car(doors=3, color="blue"), "foo")
        with self.assertRaises(DeltaRepositoryException):
            await self.repository.create(Car(doors=3, color="blue"), foo="bar")

    async def test_classname(self):
        self.assertEqual("tests.utils.Car", Car.classname)

    async def test_get_all(self):
        instances = list(
            map(
                itemgetter(0),
                await gather(
                    self.repository.create(Car, doors=5, color="blue"),
                    self.repository.create(Car, doors=3, color="red"),
                    self.repository.create(Car, doors=1, color="blue"),
                ),
            )
        )
        ordering = Ordering.ASC("doors")
        iterable = self.repository.get_all(Car, ordering)
        observed = [v async for v in iterable]

        expected = sorted(instances, key=lambda car: car.doors)
        self.assertEqual(expected, observed)

    async def test_find(self):
        originals = set(
            map(
                itemgetter(0),
                await gather(
                    self.repository.create(Car, doors=3, color="blue"),
                    self.repository.create(Car, doors=3, color="red"),
                    self.repository.create(Car, doors=5, color="blue"),
                ),
            )
        )
        condition = Condition.IN("uuid", {o.uuid for o in originals})
        ordering = Ordering.ASC("doors")
        limit = 10
        iterable = self.repository.find(Car, condition, ordering, limit)
        recovered = {v async for v in iterable}

        self.assertEqual(originals, recovered)

    async def test_find_one(self):
        originals = list(
            map(
                itemgetter(0),
                await gather(
                    self.repository.create(Car, doors=3, color="blue"),
                    self.repository.create(Car, doors=3, color="red"),
                    self.repository.create(Car, doors=5, color="blue"),
                ),
            )
        )
        condition = Condition.IN("uuid", {originals[0].uuid})
        observed = await self.repository.find_one(Car, condition)
        expected = originals[0]

        self.assertEqual(expected, observed)

    async def test_get(self):
        original, _ = await self.repository.create(Car, doors=3, color="blue")
        recovered = await self.repository.get(Car, original.uuid)

        self.assertEqual(original, recovered)

    async def test_get_raises(self):
        with self.assertRaises(NotFoundException):
            await self.repository.get(Car, NULL_UUID)

    async def test_update(self):
        car, _ = await self.repository.create(Car, doors=3, color="blue")

        await self.repository.update(car, color="red")
        expected = Car(3, "red", uuid=car.uuid, version=2, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, car)
        self.assertEqual(car, await self.repository.get(Car, car.uuid))

        observed, delta = await self.repository.update(car, doors=5)
        self.assertIsInstance(delta, Delta)

        expected = Car(5, "red", uuid=car.uuid, version=3, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, observed)
        self.assertEqual(observed, await self.repository.get(Car, expected.uuid))

    async def test_update_no_changes(self):
        car, _ = await self.repository.create(Car, doors=3, color="blue")

        observed, delta = await self.repository.update(car, color="blue")
        self.assertIsNone(delta)

        expected = Car(3, "blue", uuid=car.uuid, version=1, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, observed)
        self.assertEqual(observed, await self.repository.get(Car, expected.uuid))

    async def test_update_raises(self):
        with self.assertRaises(DeltaRepositoryException):
            await self.repository.update(Car(3, "blue"), version=1)
        with self.assertRaises(DeltaRepositoryException):
            await self.repository.update(Car(3, "blue"), created_at=current_datetime())
        with self.assertRaises(DeltaRepositoryException):
            await self.repository.update(Car(3, "blue"), updated_at=current_datetime())

    async def test_refresh(self):
        car, _ = await self.repository.create(Car, doors=3, color="blue")
        uuid = car.uuid

        car2 = await self.repository.get(Car, uuid)
        await self.repository.update(car2, color="red")
        await self.repository.update(car2, doors=5)

        self.assertEqual(
            Car(3, "blue", uuid=uuid, version=1, created_at=car.created_at, updated_at=car.updated_at),
            car,
        )
        await self.repository.refresh(car)
        self.assertEqual(
            Car(5, "red", uuid=uuid, version=3, created_at=car.created_at, updated_at=car.updated_at),
            car,
        )

    async def test_save_create(self):
        car = Car(3, "blue")
        car.color = "red"
        car.doors = 5

        with self.assertRaises(NotFoundException):
            await self.repository.refresh(car)

        await self.repository.save(car)
        self.assertEqual(
            Car(5, "red", uuid=car.uuid, version=1, created_at=car.created_at, updated_at=car.updated_at),
            car,
        )

    async def test_save_update(self):
        car, _ = await self.repository.create(Car, doors=3, color="blue")

        uuid = car.uuid

        car.color = "red"
        car.doors = 5

        expected = Car(3, "blue", uuid=uuid, version=1, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, await self.repository.get(Car, uuid))

        await self.repository.save(car)
        expected = Car(5, "red", uuid=uuid, version=2, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, car)

        expected = Car(5, "red", uuid=uuid, version=2, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, await self.repository.get(Car, uuid))

    async def test_save_raises(self):
        with self.assertRaises(DeltaRepositoryException):
            await self.repository.save(Car(3, "blue", uuid=uuid4()))
        with self.assertRaises(DeltaRepositoryException):
            await self.repository.save(Car(3, "blue", version=1))

    async def test_delete(self):
        car, _ = await self.repository.create(Car, doors=3, color="blue")
        await self.repository.delete(car)
        with self.assertRaises(AlreadyDeletedException):
            await self.repository.get(Car, car.uuid)


if __name__ == "__main__":
    unittest.main()

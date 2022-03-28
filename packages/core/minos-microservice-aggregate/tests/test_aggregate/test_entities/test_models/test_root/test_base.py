import unittest
from asyncio import (
    gather,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    AlreadyDeletedException,
    Condition,
    EventRepositoryException,
    NotFoundException,
    Ordering,
)
from minos.common import (
    NULL_UUID,
    current_datetime,
)
from tests.utils import (
    Car,
    MinosTestCase,
)


class TestRootEntity(MinosTestCase):
    async def test_create(self):
        observed = await Car.create(doors=3, color="blue")
        expected = Car(
            3,
            "blue",
            uuid=observed.uuid,
            version=1,
            created_at=observed.created_at,
            updated_at=observed.updated_at,
        )
        self.assertEqual(expected, observed)

    async def test_create_raises(self):
        with self.assertRaises(EventRepositoryException):
            await Car.create(uuid=uuid4(), doors=3, color="blue")
        with self.assertRaises(EventRepositoryException):
            await Car.create(version=1, doors=3, color="blue")
        with self.assertRaises(EventRepositoryException):
            await Car.create(created_at=current_datetime(), doors=3, color="blue")
        with self.assertRaises(EventRepositoryException):
            await Car.create(updated_at=current_datetime(), doors=3, color="blue")

    async def test_classname(self):
        self.assertEqual("tests.utils.Car", Car.classname)

    async def test_get_all(self):
        instances = list(
            await gather(
                Car.create(doors=5, color="blue"),
                Car.create(doors=3, color="red"),
                Car.create(doors=1, color="blue"),
            )
        )
        ordering = Ordering.ASC("doors")
        iterable = Car.get_all(ordering)
        observed = [v async for v in iterable]

        expected = sorted(instances, key=lambda car: car.doors)
        self.assertEqual(expected, observed)

    async def test_find(self):
        originals = set(
            await gather(
                Car.create(doors=3, color="blue"),
                Car.create(doors=3, color="red"),
                Car.create(doors=5, color="blue"),
            )
        )
        condition = Condition.IN("uuid", {o.uuid for o in originals})
        ordering = Ordering.ASC("doors")
        limit = 10
        iterable = Car.find(condition, ordering, limit)
        recovered = {v async for v in iterable}

        self.assertEqual(originals, recovered)

    async def test_get(self):
        original = await Car.create(doors=3, color="blue")
        recovered = await Car.get(original.uuid)

        self.assertEqual(original, recovered)

    async def test_get_raises(self):
        with self.assertRaises(NotFoundException):
            await Car.get(NULL_UUID)

    async def test_update(self):
        car = await Car.create(doors=3, color="blue")

        await car.update(color="red")
        expected = Car(3, "red", uuid=car.uuid, version=2, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, car)
        self.assertEqual(car, await Car.get(car.uuid))

        await car.update(doors=5)
        expected = Car(5, "red", uuid=car.uuid, version=3, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, car)
        self.assertEqual(car, await Car.get(car.uuid))

    async def test_update_no_changes(self):
        car = await Car.create(doors=3, color="blue")

        await car.update(color="blue")
        expected = Car(3, "blue", uuid=car.uuid, version=1, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, car)
        self.assertEqual(car, await Car.get(car.uuid))

    async def test_update_raises(self):
        with self.assertRaises(EventRepositoryException):
            await Car(3, "blue").update(version=1)
        with self.assertRaises(EventRepositoryException):
            await Car(3, "blue").update(created_at=current_datetime())
        with self.assertRaises(EventRepositoryException):
            await Car(3, "blue").update(updated_at=current_datetime())

    async def test_refresh(self):
        car = await Car.create(doors=3, color="blue")
        uuid = car.uuid

        car2 = await Car.get(uuid)
        await car2.update(color="red")
        await car2.update(doors=5)

        self.assertEqual(
            Car(3, "blue", uuid=uuid, version=1, created_at=car.created_at, updated_at=car.updated_at),
            car,
        )
        await car.refresh()
        self.assertEqual(
            Car(5, "red", uuid=uuid, version=3, created_at=car.created_at, updated_at=car.updated_at),
            car,
        )

    async def test_save_create(self):
        car = Car(3, "blue")
        car.color = "red"
        car.doors = 5

        with self.assertRaises(NotFoundException):
            await car.refresh()

        await car.save()
        self.assertEqual(
            Car(5, "red", uuid=car.uuid, version=1, created_at=car.created_at, updated_at=car.updated_at),
            car,
        )

    async def test_save_update(self):
        car = await Car.create(doors=3, color="blue")

        uuid = car.uuid

        car.color = "red"
        car.doors = 5

        expected = Car(3, "blue", uuid=uuid, version=1, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, await Car.get(uuid))

        await car.save()
        expected = Car(5, "red", uuid=uuid, version=2, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, car)

        expected = Car(5, "red", uuid=uuid, version=2, created_at=car.created_at, updated_at=car.updated_at)
        self.assertEqual(expected, await Car.get(uuid))

    async def test_save_raises(self):
        with self.assertRaises(EventRepositoryException):
            await Car(3, "blue", uuid=uuid4()).save()
        with self.assertRaises(EventRepositoryException):
            await Car(3, "blue", version=1).save()

    async def test_delete(self):
        car = await Car.create(doors=3, color="blue")
        await car.delete()
        with self.assertRaises(AlreadyDeletedException):
            await Car.get(car.uuid)


if __name__ == "__main__":
    unittest.main()

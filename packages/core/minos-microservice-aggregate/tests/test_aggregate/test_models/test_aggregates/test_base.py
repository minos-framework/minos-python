import unittest
from asyncio import (
    gather,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    AggregateNotFoundException,
    Condition,
    DeletedAggregateException,
    EventRepositoryException,
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


class TestAggregate(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.kwargs = {"_repository": self.event_repository, "_snapshot": self.snapshot_repository}

    async def test_create(self):
        observed = await Car.create(doors=3, color="blue", **self.kwargs)
        expected = Car(
            3,
            "blue",
            uuid=observed.uuid,
            version=1,
            created_at=observed.created_at,
            updated_at=observed.updated_at,
            **self.kwargs
        )
        self.assertEqual(expected, observed)

    async def test_create_raises(self):
        with self.assertRaises(EventRepositoryException):
            await Car.create(uuid=uuid4(), doors=3, color="blue", **self.kwargs)
        with self.assertRaises(EventRepositoryException):
            await Car.create(version=1, doors=3, color="blue", **self.kwargs)
        with self.assertRaises(EventRepositoryException):
            await Car.create(created_at=current_datetime(), doors=3, color="blue", **self.kwargs)
        with self.assertRaises(EventRepositoryException):
            await Car.create(updated_at=current_datetime(), doors=3, color="blue", **self.kwargs)

    async def test_classname(self):
        self.assertEqual("tests.utils.Car", Car.classname)

    async def test_find(self):
        originals = set(
            await gather(
                Car.create(doors=3, color="blue", **self.kwargs),
                Car.create(doors=3, color="red", **self.kwargs),
                Car.create(doors=5, color="blue", **self.kwargs),
            )
        )
        condition = Condition.IN("uuid", {o.uuid for o in originals})
        ordering = Ordering.ASC("doors")
        limit = 10
        iterable = Car.find(condition, ordering, limit, **self.kwargs)
        recovered = {v async for v in iterable}

        self.assertEqual(originals, recovered)

    async def test_get(self):
        original = await Car.create(doors=3, color="blue", **self.kwargs)
        recovered = await Car.get(original.uuid, **self.kwargs)

        self.assertEqual(original, recovered)

    async def test_get_raises(self):
        with self.assertRaises(AggregateNotFoundException):
            await Car.get(NULL_UUID, **self.kwargs)

    async def test_update(self):
        car = await Car.create(doors=3, color="blue", **self.kwargs)

        await car.update(color="red")
        expected = Car(
            3, "red", uuid=car.uuid, version=2, created_at=car.created_at, updated_at=car.updated_at, **self.kwargs
        )
        self.assertEqual(expected, car)
        self.assertEqual(car, await Car.get(car.uuid, **self.kwargs))

        await car.update(doors=5)
        expected = Car(
            5, "red", uuid=car.uuid, version=3, created_at=car.created_at, updated_at=car.updated_at, **self.kwargs
        )
        self.assertEqual(expected, car)
        self.assertEqual(car, await Car.get(car.uuid, **self.kwargs))

    async def test_update_no_changes(self):
        car = await Car.create(doors=3, color="blue", **self.kwargs)

        await car.update(color="blue")
        expected = Car(
            3, "blue", uuid=car.uuid, version=1, created_at=car.created_at, updated_at=car.updated_at, **self.kwargs
        )
        self.assertEqual(expected, car)
        self.assertEqual(car, await Car.get(car.uuid, **self.kwargs))

    async def test_update_raises(self):
        with self.assertRaises(EventRepositoryException):
            await Car(3, "blue", **self.kwargs).update(version=1)
        with self.assertRaises(EventRepositoryException):
            await Car(3, "blue", **self.kwargs).update(created_at=current_datetime())
        with self.assertRaises(EventRepositoryException):
            await Car(3, "blue", **self.kwargs).update(updated_at=current_datetime())

    async def test_refresh(self):
        car = await Car.create(doors=3, color="blue", **self.kwargs)
        uuid = car.uuid

        car2 = await Car.get(uuid, **self.kwargs)
        await car2.update(color="red", **self.kwargs)
        await car2.update(doors=5, **self.kwargs)

        self.assertEqual(
            Car(3, "blue", uuid=uuid, version=1, created_at=car.created_at, updated_at=car.updated_at, **self.kwargs),
            car,
        )
        await car.refresh()
        self.assertEqual(
            Car(5, "red", uuid=uuid, version=3, created_at=car.created_at, updated_at=car.updated_at, **self.kwargs),
            car,
        )

    async def test_save_create(self):
        car = Car(3, "blue", **self.kwargs)
        car.color = "red"
        car.doors = 5

        with self.assertRaises(AggregateNotFoundException):
            await car.refresh()

        await car.save()
        self.assertEqual(
            Car(
                5, "red", uuid=car.uuid, version=1, created_at=car.created_at, updated_at=car.updated_at, **self.kwargs
            ),
            car,
        )

    async def test_save_update(self):
        car = await Car.create(doors=3, color="blue", **self.kwargs)

        uuid = car.uuid

        car.color = "red"
        car.doors = 5

        expected = Car(
            3, "blue", uuid=uuid, version=1, created_at=car.created_at, updated_at=car.updated_at, **self.kwargs
        )
        self.assertEqual(
            expected, await Car.get(uuid, **self.kwargs),
        )

        await car.save()
        expected = Car(
            5, "red", uuid=uuid, version=2, created_at=car.created_at, updated_at=car.updated_at, **self.kwargs
        )
        self.assertEqual(expected, car)

        expected = Car(
            5, "red", uuid=uuid, version=2, created_at=car.created_at, updated_at=car.updated_at, **self.kwargs
        )
        self.assertEqual(
            expected, await Car.get(uuid, **self.kwargs),
        )

    async def test_save_raises(self):
        with self.assertRaises(EventRepositoryException):
            await Car(3, "blue", uuid=uuid4(), **self.kwargs).save()
        with self.assertRaises(EventRepositoryException):
            await Car(3, "blue", version=1, **self.kwargs).save()

    async def test_delete(self):
        car = await Car.create(doors=3, color="blue", **self.kwargs)
        await car.delete()
        with self.assertRaises(DeletedAggregateException):
            await Car.get(car.uuid, **self.kwargs)


if __name__ == "__main__":
    unittest.main()

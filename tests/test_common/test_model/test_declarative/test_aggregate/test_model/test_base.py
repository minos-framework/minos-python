import unittest
from asyncio import (
    gather,
)
from uuid import (
    uuid4,
)

from minos.common import (
    NULL_UUID,
    Condition,
    InMemoryRepository,
    InMemorySnapshot,
    MinosRepositoryException,
    MinosSnapshotAggregateNotFoundException,
    MinosSnapshotDeletedAggregateException,
    Ordering,
    current_datetime,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    FakeBroker,
)


class TestAggregate(unittest.IsolatedAsyncioTestCase):
    async def test_create(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            observed = await Car.create(doors=3, color="blue", _broker=b, _repository=r, _snapshot=s)
            expected = Car(
                3,
                "blue",
                uuid=observed.uuid,
                version=1,
                created_at=observed.created_at,
                updated_at=observed.updated_at,
                _broker=b,
                _repository=r,
                _snapshot=s,
            )
            self.assertEqual(expected, observed)

    async def test_create_raises(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            with self.assertRaises(MinosRepositoryException):
                await Car.create(uuid=uuid4(), doors=3, color="blue", _broker=b, _repository=r, _snapshot=s)
            with self.assertRaises(MinosRepositoryException):
                await Car.create(version=1, doors=3, color="blue", _broker=b, _repository=r, _snapshot=s)
            with self.assertRaises(MinosRepositoryException):
                await Car.create(
                    created_at=current_datetime(), doors=3, color="blue", _broker=b, _repository=r, _snapshot=s
                )
            with self.assertRaises(MinosRepositoryException):
                await Car.create(
                    updated_at=current_datetime(), doors=3, color="blue", _broker=b, _repository=r, _snapshot=s
                )

    async def test_classname(self):
        self.assertEqual("tests.aggregate_classes.Car", Car.classname)

    async def test_find(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            originals = set(
                await gather(
                    Car.create(doors=3, color="blue", _broker=b, _repository=r, _snapshot=s),
                    Car.create(doors=3, color="red", _broker=b, _repository=r, _snapshot=s),
                    Car.create(doors=5, color="blue", _broker=b, _repository=r, _snapshot=s),
                )
            )
            condition = Condition.IN("uuid", {o.uuid for o in originals})
            ordering = Ordering.ASC("doors")
            limit = 10
            iterable = Car.find(condition, ordering, limit, _broker=b, _repository=r, _snapshot=s)
            recovered = {v async for v in iterable}

            self.assertEqual(originals, recovered)

    async def test_get(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            original = await Car.create(doors=3, color="blue", _broker=b, _repository=r, _snapshot=s)
            recovered = await Car.get(original.uuid, _broker=b, _repository=r, _snapshot=s)

            self.assertEqual(original, recovered)

    async def test_get_raises(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            with self.assertRaises(MinosSnapshotAggregateNotFoundException):
                await Car.get(NULL_UUID, _broker=b, _repository=r, _snapshot=s)

    async def test_update(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            car = await Car.create(doors=3, color="blue", _broker=b, _repository=r, _snapshot=s)

            await car.update(color="red")
            expected = Car(
                3,
                "red",
                uuid=car.uuid,
                version=2,
                created_at=car.created_at,
                updated_at=car.updated_at,
                _broker=b,
                _repository=r,
                _snapshot=s,
            )
            self.assertEqual(expected, car)
            self.assertEqual(car, await Car.get(car.uuid, _broker=b, _repository=r, _snapshot=s))

            await car.update(doors=5)
            expected = Car(
                5,
                "red",
                uuid=car.uuid,
                version=3,
                created_at=car.created_at,
                updated_at=car.updated_at,
                _broker=b,
                _repository=r,
                _snapshot=s,
            )
            self.assertEqual(expected, car)
            self.assertEqual(car, await Car.get(car.uuid, _broker=b, _repository=r, _snapshot=s))

    async def test_update_no_changes(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            car = await Car.create(doors=3, color="blue", _broker=b, _repository=r, _snapshot=s)

            await car.update(color="blue")
            expected = Car(
                3,
                "blue",
                uuid=car.uuid,
                version=1,
                created_at=car.created_at,
                updated_at=car.updated_at,
                _broker=b,
                _repository=r,
                _snapshot=s,
            )
            self.assertEqual(expected, car)
            self.assertEqual(car, await Car.get(car.uuid, _broker=b, _repository=r, _snapshot=s))

    async def test_update_raises(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            with self.assertRaises(MinosRepositoryException):
                await Car(3, "blue", _broker=b, _repository=r, _snapshot=s).update(version=1)
            with self.assertRaises(MinosRepositoryException):
                await Car(3, "blue", _broker=b, _repository=r, _snapshot=s).update(created_at=current_datetime())
            with self.assertRaises(MinosRepositoryException):
                await Car(3, "blue", _broker=b, _repository=r, _snapshot=s).update(updated_at=current_datetime())

    async def test_refresh(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            car = await Car.create(doors=3, color="blue", _broker=b, _repository=r, _snapshot=s)
            uuid = car.uuid

            car2 = await Car.get(uuid, _broker=b, _repository=r, _snapshot=s)
            await car2.update(color="red", _broker=b, _repository=r, _snapshot=s)
            await car2.update(doors=5, _broker=b, _repository=r, _snapshot=s)

            self.assertEqual(
                Car(
                    3,
                    "blue",
                    uuid=uuid,
                    version=1,
                    created_at=car.created_at,
                    updated_at=car.updated_at,
                    _broker=b,
                    _repository=r,
                    _snapshot=s,
                ),
                car,
            )
            await car.refresh()
            self.assertEqual(
                Car(
                    5,
                    "red",
                    uuid=uuid,
                    version=3,
                    created_at=car.created_at,
                    updated_at=car.updated_at,
                    _broker=b,
                    _repository=r,
                    _snapshot=s,
                ),
                car,
            )

    async def test_save_create(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            car = Car(3, "blue", _broker=b, _repository=r, _snapshot=s)
            car.color = "red"
            car.doors = 5

            with self.assertRaises(MinosSnapshotAggregateNotFoundException):
                await car.refresh()

            await car.save()
            self.assertEqual(
                Car(
                    5,
                    "red",
                    uuid=car.uuid,
                    version=1,
                    created_at=car.created_at,
                    updated_at=car.updated_at,
                    _broker=b,
                    _repository=r,
                    _snapshot=s,
                ),
                car,
            )

    async def test_save_update(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            car = await Car.create(doors=3, color="blue", _broker=b, _repository=r, _snapshot=s)

            uuid = car.uuid

            car.color = "red"
            car.doors = 5

            expected = Car(
                3,
                "blue",
                uuid=uuid,
                version=1,
                created_at=car.created_at,
                updated_at=car.updated_at,
                _broker=b,
                _repository=r,
                _snapshot=s,
            )
            self.assertEqual(
                expected, await Car.get(uuid, _broker=b, _repository=r, _snapshot=s),
            )

            await car.save()
            expected = Car(
                5,
                "red",
                uuid=uuid,
                version=2,
                created_at=car.created_at,
                updated_at=car.updated_at,
                _broker=b,
                _repository=r,
                _snapshot=s,
            )
            self.assertEqual(expected, car)

            expected = Car(
                5,
                "red",
                uuid=uuid,
                version=2,
                created_at=car.created_at,
                updated_at=car.updated_at,
                _broker=b,
                _repository=r,
                _snapshot=s,
            )
            self.assertEqual(
                expected, await Car.get(uuid, _broker=b, _repository=r, _snapshot=s),
            )

    async def test_save_raises(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            with self.assertRaises(MinosRepositoryException):
                await Car(3, "blue", uuid=uuid4(), _broker=b, _repository=r, _snapshot=s).save()
            with self.assertRaises(MinosRepositoryException):
                await Car(3, "blue", version=1, _broker=b, _repository=r, _snapshot=s).save()

    async def test_delete(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            car = await Car.create(doors=3, color="blue", _broker=b, _repository=r, _snapshot=s)
            await car.delete()
            with self.assertRaises(MinosSnapshotDeletedAggregateException):
                await Car.get(car.uuid, _broker=b, _repository=r, _snapshot=s)


if __name__ == "__main__":
    unittest.main()

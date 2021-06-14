"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    InMemoryRepository,
    InMemorySnapshot,
    MinosRepositoryAggregateNotFoundException,
    MinosRepositoryDeletedAggregateException,
    MinosRepositoryManuallySetAggregateIdException,
    MinosRepositoryManuallySetAggregateVersionException,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    FakeBroker,
)


class TestAggregate(unittest.IsolatedAsyncioTestCase):
    async def test_create(self):
        async with FakeBroker() as broker, InMemoryRepository() as repository, InMemorySnapshot() as snapshot:
            car = await Car.create(doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot)
            self.assertEqual(Car(1, 1, 3, "blue", _broker=broker, _repository=repository, _snapshot=snapshot), car)

    async def test_create_raises(self):
        async with FakeBroker() as broker, InMemoryRepository() as repository, InMemorySnapshot() as snapshot:
            with self.assertRaises(MinosRepositoryManuallySetAggregateIdException):
                await Car.create(
                    id=1, doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot
                )
            with self.assertRaises(MinosRepositoryManuallySetAggregateVersionException):
                await Car.create(
                    version=1, doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot
                )

    async def test_classname(self):
        self.assertEqual("tests.aggregate_classes.Car", Car.classname)

    async def test_get(self):
        async with FakeBroker() as broker, InMemoryRepository() as repository, InMemorySnapshot() as snapshot:
            originals = [
                await Car.create(doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot),
                await Car.create(doors=3, color="red", _broker=broker, _repository=repository, _snapshot=snapshot),
                await Car.create(doors=5, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot),
            ]
            iterable = Car.get([o.id for o in originals], _broker=broker, _repository=repository, _snapshot=snapshot)
            recovered = [v async for v in iterable]

            self.assertEqual(originals, recovered)

    async def test_get_one(self):
        async with FakeBroker() as broker, InMemoryRepository() as repository, InMemorySnapshot() as snapshot:
            original = await Car.create(
                doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot
            )
            recovered = await Car.get_one(original.id, _broker=broker, _repository=repository, _snapshot=snapshot)

            self.assertEqual(original, recovered)

    async def test_get_one_raises(self):
        async with FakeBroker() as broker, InMemoryRepository() as repository, InMemorySnapshot() as snapshot:
            with self.assertRaises(MinosRepositoryAggregateNotFoundException):
                await Car.get_one(0, _broker=broker, _repository=repository, _snapshot=snapshot)

    async def test_update(self):
        async with FakeBroker() as broker, InMemoryRepository() as repository, InMemorySnapshot() as snapshot:
            car = await Car.create(doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot)

            await car.update(color="red")
            self.assertEqual(Car(1, 2, 3, "red", _broker=broker, _repository=repository, _snapshot=snapshot), car)
            self.assertEqual(car, await Car.get_one(car.id, _broker=broker, _repository=repository, _snapshot=snapshot))

            await car.update(doors=5)
            self.assertEqual(Car(1, 3, 5, "red", _broker=broker, _repository=repository, _snapshot=snapshot), car)
            self.assertEqual(car, await Car.get_one(car.id, _broker=broker, _repository=repository, _snapshot=snapshot))

    async def test_update_raises(self):
        async with FakeBroker() as broker, InMemoryRepository() as repository, InMemorySnapshot() as snapshot:
            with self.assertRaises(MinosRepositoryManuallySetAggregateVersionException):
                await Car(1, 1, 3, "blue", _broker=broker, _repository=repository, _snapshot=snapshot).update(version=1)

    async def test_refresh(self):
        async with FakeBroker() as broker, InMemoryRepository() as repository, InMemorySnapshot() as snapshot:
            car = await Car.create(doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot)
            car2 = await Car.get_one(car.id, _broker=broker, _repository=repository, _snapshot=snapshot)
            await car2.update(color="red", _broker=broker, _repository=repository, _snapshot=snapshot)
            await car2.update(doors=5, _broker=broker, _repository=repository, _snapshot=snapshot)

            self.assertEqual(Car(1, 1, 3, "blue", _broker=broker, _repository=repository, _snapshot=snapshot), car)
            await car.refresh()
            self.assertEqual(Car(1, 3, 5, "red", _broker=broker, _repository=repository, _snapshot=snapshot), car)

    async def test_delete(self):
        async with FakeBroker() as broker, InMemoryRepository() as repository, InMemorySnapshot() as snapshot:
            car = await Car.create(doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot)
            await car.delete()
            with self.assertRaises(MinosRepositoryDeletedAggregateException):
                await Car.get_one(car.id, _broker=broker, _repository=repository, _snapshot=snapshot)


if __name__ == "__main__":
    unittest.main()

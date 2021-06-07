"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import sys
import unittest

from dependency_injector import (
    containers,
    providers,
)

from minos.common import (
    MinosConfig,
    MinosInMemoryRepository,
    MinosRepositoryAggregateNotFoundException,
    MinosRepositoryDeletedAggregateException,
    MinosRepositoryManuallySetAggregateIdException,
    MinosRepositoryManuallySetAggregateVersionException,
    MinosRepositoryNonProvidedException,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    BASE_PATH,
)


class TestAggregate(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config_file_path = BASE_PATH / "test_config.yml"
        self.config = MinosConfig(path=str(self.config_file_path))

        self.container = containers.DynamicContainer()
        self.container.config = providers.Object(self.config)
        self.container.wire(modules=[sys.modules[__name__]])

    def tearDown(self):
        self.container.unwire()

    async def test_create(self):
        async with MinosInMemoryRepository() as repository:
            car = await Car.create(doors=3, color="blue", _repository=repository)
            self.assertEqual(Car(1, 1, 3, "blue"), car)

    async def test_create_raises(self):
        async with MinosInMemoryRepository() as repository:
            with self.assertRaises(MinosRepositoryManuallySetAggregateIdException):
                await Car.create(id=1, doors=3, color="blue", _repository=repository)
            with self.assertRaises(MinosRepositoryManuallySetAggregateVersionException):
                await Car.create(version=1, doors=3, color="blue", _repository=repository)
        with self.assertRaises(MinosRepositoryNonProvidedException):
            await Car.create(doors=3, color="blue")

    async def test_classname(self):
        self.assertEqual("tests.aggregate_classes.Car", Car.classname)

    async def test_get(self):
        async with MinosInMemoryRepository() as repository:
            originals = [
                await Car.create(doors=3, color="blue", _repository=repository),
                await Car.create(doors=3, color="red", _repository=repository),
                await Car.create(doors=5, color="blue", _repository=repository),
            ]
            recovered = await Car.get([o.id for o in originals], _repository=repository)

            self.assertEqual(originals, recovered)

    async def test_get_one(self):
        async with MinosInMemoryRepository() as repository:
            original = await Car.create(doors=3, color="blue", _repository=repository)
            recovered = await Car.get_one(original.id, _repository=repository)

            self.assertEqual(original, recovered)

    async def test_get_one_raises(self):
        async with MinosInMemoryRepository() as repository:
            with self.assertRaises(MinosRepositoryAggregateNotFoundException):
                await Car.get_one(0, _repository=repository)
        with self.assertRaises(MinosRepositoryNonProvidedException):
            await Car.get_one(1)

    async def test_update(self):
        async with MinosInMemoryRepository() as repository:
            car = await Car.create(doors=3, color="blue", _repository=repository)

            await car.update(color="red")
            self.assertEqual(Car(1, 2, 3, "red"), car)
            self.assertEqual(car, await Car.get_one(car.id, _repository=repository))

            await car.update(doors=5)
            self.assertEqual(Car(1, 3, 5, "red"), car)
            self.assertEqual(car, await Car.get_one(car.id, _repository=repository))

    async def test_update_raises(self):
        async with MinosInMemoryRepository() as repository:
            with self.assertRaises(MinosRepositoryManuallySetAggregateVersionException):
                await Car(1, 1, 3, "blue", _repository=repository).update(version=1)
        with self.assertRaises(MinosRepositoryNonProvidedException):
            await Car(1, 1, 3, "blue").update(doors=1)

    async def test_refresh(self):
        async with MinosInMemoryRepository() as repository:
            car = await Car.create(doors=3, color="blue", _repository=repository)
            car2 = await Car.get_one(car.id, _repository=repository)
            await car2.update(color="red", _repository=repository)
            await car2.update(doors=5, _repository=repository)

            self.assertEqual(Car(1, 1, 3, "blue"), car)
            await car.refresh()
            self.assertEqual(Car(1, 3, 5, "red"), car)

    async def test_delete(self):
        async with MinosInMemoryRepository() as repository:
            car = await Car.create(doors=3, color="blue", _repository=repository)
            await car.delete()
            with self.assertRaises(MinosRepositoryDeletedAggregateException):
                await Car.get_one(car.id, _repository=repository)

    async def test_delete_raises(self):
        with self.assertRaises(MinosRepositoryNonProvidedException):
            await Car(1, 1, 3, "blue").delete()


if __name__ == "__main__":
    unittest.main()

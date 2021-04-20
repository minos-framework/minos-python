"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    MinosInMemoryRepository,
    MinosRepositoryDeletedAggregateException,
    MinosRepositoryAggregateNotFoundException,
)
from tests.aggregate_classes import (
    Car,
)


class TestAggregate(unittest.TestCase):
    def test_create(self):
        with MinosInMemoryRepository() as repository:
            car = Car.create(doors=3, color="blue", _repository=repository)
            self.assertEqual(Car(1, 1, 3, "blue"), car)

    def test_get(self):
        with MinosInMemoryRepository() as repository:
            originals = [
                Car.create(doors=3, color="blue", _repository=repository),
                Car.create(doors=3, color="red", _repository=repository),
                Car.create(doors=5, color="blue", _repository=repository),
            ]
            recovered = Car.get([o.id for o in originals], _repository=repository)

            self.assertEqual(originals, recovered)

    def test_get_one(self):
        with MinosInMemoryRepository() as repository:
            original = Car.create(doors=3, color="blue", _repository=repository)
            recovered = Car.get_one(original.id, _repository=repository)

            self.assertEqual(original, recovered)

    def test_get_one_raises(self):
        with MinosInMemoryRepository() as repository:
            with self.assertRaises(MinosRepositoryAggregateNotFoundException):
                Car.get_one(0, _repository=repository)

    def test_update(self):
        with MinosInMemoryRepository() as repository:
            car = Car.create(doors=3, color="blue", _repository=repository)

            car.update(color="red")
            self.assertEqual(Car(1, 2, 3, "red"), car)
            self.assertEqual(car, Car.get_one(car.id, _repository=repository))

            car.update(doors=5)
            self.assertEqual(Car(1, 3, 5, "red"), car)
            self.assertEqual(car, Car.get_one(car.id, _repository=repository))

    def test_update_cls(self):
        with MinosInMemoryRepository() as repository:
            car = Car.create(doors=3, color="blue", _repository=repository)

            Car.update(car.id, color="red", _repository=repository)
            self.assertEqual(Car(1, 2, 3, "red"), Car.get_one(car.id, _repository=repository))

            Car.update(car.id, doors=5, _repository=repository)
            self.assertEqual(Car(1, 3, 5, "red"), Car.get_one(car.id, _repository=repository))

    def test_refresh(self):
        with MinosInMemoryRepository() as repository:
            car = Car.create(doors=3, color="blue", _repository=repository)
            Car.update(car.id, color="red", _repository=repository)
            Car.update(car.id, doors=5, _repository=repository)

            self.assertEqual(Car(1, 1, 3, "blue"), car)
            car.refresh()
            self.assertEqual(Car(1, 3, 5, "red"), car)

    def test_delete(self):
        with MinosInMemoryRepository() as repository:
            car = Car.create(doors=3, color="blue", _repository=repository)
            car.delete()
            with self.assertRaises(MinosRepositoryDeletedAggregateException):
                Car.get_one(car.id, _repository=repository)

    def test_delete_cls(self):
        with MinosInMemoryRepository() as repository:
            car = Car.create(doors=3, color="blue", _repository=repository)
            Car.delete(car.id, _repository=repository)
            with self.assertRaises(MinosRepositoryDeletedAggregateException):
                Car.get_one(car.id, _repository=repository)


if __name__ == "__main__":
    unittest.main()

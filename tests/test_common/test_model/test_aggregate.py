"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from tests.aggregate_classes import Car


class TestAggregate(unittest.TestCase):
    def test_create(self):
        car = Car.create(doors=3, color="blue")
        self.assertEqual(Car(0, 0, 3, "blue"), car)

    def test_update(self):
        car = Car.create(doors=3, color="blue")

        car.update(color="red")
        self.assertEqual(Car(0, 1, 3, "red"), car)

        car.update(doors=5)
        self.assertEqual(Car(0, 2, 5, "red"), car)


if __name__ == "__main__":
    unittest.main()

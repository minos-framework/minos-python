"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    FakeBroker,
    FakeRepository,
)


class TestAggregate(unittest.IsolatedAsyncioTestCase):
    async def test_create(self):
        async with FakeBroker() as broker, FakeRepository() as repository:
            await Car.create(doors=3, color="blue", _broker=broker, _repository=repository)
            self.assertEqual(
                [
                    {
                        "items": [Car(9999, 1, doors=3, color="blue", _broker=broker, _repository=repository)],
                        "topic": "CarCreated",
                    }
                ],
                broker.calls_kwargs,
            )

    async def test_update(self):
        async with FakeBroker() as broker, FakeRepository() as repository:
            car = await Car.create(doors=3, color="blue", _broker=broker, _repository=repository)
            broker.reset_mock()

            await car.update(color="red")
            self.assertEqual(
                [
                    {
                        "items": [Car(9999, 2, doors=3, color="red", _broker=broker, _repository=repository)],
                        "topic": "CarUpdated",
                    }
                ],
                broker.calls_kwargs,
            )

    async def test_delete(self):
        async with FakeBroker() as broker, FakeRepository() as repository:
            car = await Car.create(doors=3, color="blue", _broker=broker, _repository=repository)
            broker.reset_mock()

            await car.delete()
            self.assertEqual(
                [
                    {
                        "items": [Car(9999, 1, doors=3, color="blue", _broker=broker, _repository=repository)],
                        "topic": "CarDeleted",
                    }
                ],
                broker.calls_kwargs,
            )


if __name__ == "__main__":
    unittest.main()

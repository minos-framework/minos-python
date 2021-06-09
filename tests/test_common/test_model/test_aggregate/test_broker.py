"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    InMemorySnapshot,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    FakeBroker,
    FakeRepository,
)


class TestAggregate(unittest.IsolatedAsyncioTestCase):
    async def test_create(self):
        async with FakeBroker() as broker, FakeRepository() as repository, InMemorySnapshot() as snapshot:
            await Car.create(doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot)
            self.assertEqual(
                [
                    {
                        "items": [
                            Car(
                                9999,
                                1,
                                doors=3,
                                color="blue",
                                _broker=broker,
                                _repository=repository,
                                _snapshot=snapshot,
                            )
                        ],
                        "topic": "CarCreated",
                    }
                ],
                broker.calls_kwargs,
            )

    async def test_update(self):
        async with FakeBroker() as broker, FakeRepository() as repository, InMemorySnapshot() as snapshot:
            car = await Car.create(doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot)
            broker.reset_mock()

            await car.update(color="red")
            self.assertEqual(
                [
                    {
                        "items": [
                            Car(
                                9999,
                                2,
                                doors=3,
                                color="red",
                                _broker=broker,
                                _repository=repository,
                                _snapshot=snapshot,
                            )
                        ],
                        "topic": "CarUpdated",
                    }
                ],
                broker.calls_kwargs,
            )

    async def test_delete(self):
        async with FakeBroker() as broker, FakeRepository() as repository, InMemorySnapshot() as snapshot:
            car = await Car.create(doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot)
            broker.reset_mock()

            await car.delete()
            self.assertEqual(
                [
                    {
                        "items": [
                            Car(
                                9999,
                                1,
                                doors=3,
                                color="blue",
                                _broker=broker,
                                _repository=repository,
                                _snapshot=snapshot,
                            )
                        ],
                        "topic": "CarDeleted",
                    }
                ],
                broker.calls_kwargs,
            )


if __name__ == "__main__":
    unittest.main()

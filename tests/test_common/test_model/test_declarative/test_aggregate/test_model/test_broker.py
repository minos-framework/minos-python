"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from typing import (
    Optional,
)

from minos.common import (
    AggregateDiff,
    Field,
    FieldsDiff,
    InMemoryRepository,
    InMemorySnapshot,
    ModelRef,
)
from tests.aggregate_classes import (
    Car,
    Owner,
)
from tests.utils import (
    FakeBroker,
)


class TestAggregate(unittest.IsolatedAsyncioTestCase):
    async def test_create(self):
        async with FakeBroker() as broker, InMemoryRepository() as repository, InMemorySnapshot() as snapshot:
            await Car.create(doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot)
            self.assertEqual(
                [
                    {
                        "items": [
                            AggregateDiff(
                                id=1,
                                name=Car.classname,
                                version=1,
                                fields_diff=FieldsDiff(
                                    {
                                        "doors": Field("doors", int, 3),
                                        "color": Field("color", str, "blue"),
                                        "owner": Field("owner", Optional[list[ModelRef[Owner]]], None),
                                    }
                                ),
                            )
                        ],
                        "topic": "CarCreated",
                    }
                ],
                broker.calls_kwargs,
            )

    async def test_update(self):
        async with FakeBroker() as broker, InMemoryRepository() as repository, InMemorySnapshot() as snapshot:
            car = await Car.create(doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot)
            broker.reset_mock()

            await car.update(color="red")
            self.assertEqual(
                [
                    {
                        "items": [
                            AggregateDiff(
                                id=1,
                                name=Car.classname,
                                version=1,
                                fields_diff=FieldsDiff({"color": Field("color", str, "red")}),
                            )
                        ],
                        "topic": "CarUpdated",
                    }
                ],
                broker.calls_kwargs,
            )

    async def test_delete(self):
        async with FakeBroker() as broker, InMemoryRepository() as repository, InMemorySnapshot() as snapshot:
            car = await Car.create(doors=3, color="blue", _broker=broker, _repository=repository, _snapshot=snapshot)
            broker.reset_mock()

            await car.delete()
            self.assertEqual(
                [
                    {
                        "items": [AggregateDiff(id=1, name=Car.classname, version=1, fields_diff=FieldsDiff.empty())],
                        "topic": "CarDeleted",
                    }
                ],
                broker.calls_kwargs,
            )


if __name__ == "__main__":
    unittest.main()

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
    Action,
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
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            car = await Car.create(doors=3, color="blue", _broker=b, _repository=r, _snapshot=s)
            self.assertEqual(
                [
                    {
                        "data": AggregateDiff(
                            uuid=car.uuid,
                            name=Car.classname,
                            version=1,
                            action=Action.CREATE,
                            differences=FieldsDiff(
                                {
                                    "doors": Field("doors", int, 3),
                                    "color": Field("color", str, "blue"),
                                    "owner": Field("owner", Optional[list[ModelRef[Owner]]], None),
                                }
                            ),
                        ),
                        "topic": "CarCreated",
                    }
                ],
                b.calls_kwargs,
            )

    async def test_update(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            car = await Car.create(doors=3, color="blue", _broker=b, _repository=r, _snapshot=s)
            b.reset_mock()

            await car.update(color="red")
            self.assertEqual(
                [
                    {
                        "data": AggregateDiff(
                            uuid=car.uuid,
                            name=Car.classname,
                            version=2,
                            action=Action.UPDATE,
                            differences=FieldsDiff({"color": Field("color", str, "red")}),
                        ),
                        "topic": "CarUpdated",
                    },
                    {
                        "data": AggregateDiff(
                            uuid=car.uuid,
                            name=Car.classname,
                            version=2,
                            action=Action.UPDATE,
                            differences=FieldsDiff({"color": Field("color", str, "red")}),
                        ),
                        "topic": "CarUpdated.color",
                    },
                ],
                b.calls_kwargs,
            )

    async def test_delete(self):
        async with FakeBroker() as b, InMemoryRepository() as r, InMemorySnapshot() as s:
            car = await Car.create(doors=3, color="blue", _broker=b, _repository=r, _snapshot=s)
            b.reset_mock()

            await car.delete()
            self.assertEqual(
                [
                    {
                        "data": AggregateDiff(
                            car.uuid,
                            name=Car.classname,
                            version=2,
                            action=Action.DELETE,
                            differences=FieldsDiff.empty(),
                        ),
                        "topic": "CarDeleted",
                    }
                ],
                b.calls_kwargs,
            )


if __name__ == "__main__":
    unittest.main()

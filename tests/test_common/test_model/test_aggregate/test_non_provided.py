"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    MinosBrokerNonProvidedException,
    MinosRepositoryNonProvidedException,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    FakeBroker,
)


class TestAggregateNonProvided(unittest.IsolatedAsyncioTestCase):
    async def test_create_raises(self):
        with self.assertRaises(MinosBrokerNonProvidedException):
            await Car.create(doors=3, color="blue")
        async with FakeBroker() as broker:
            with self.assertRaises(MinosRepositoryNonProvidedException):
                await Car.create(doors=3, color="blue", _broker=broker)

    async def test_get_one_raises(self):
        with self.assertRaises(MinosBrokerNonProvidedException):
            await Car.get_one(1)

        async with FakeBroker() as broker:
            with self.assertRaises(MinosRepositoryNonProvidedException):
                await Car.get_one(1, _broker=broker)

    async def test_update_raises(self):
        with self.assertRaises(MinosBrokerNonProvidedException):
            await Car(1, 1, 3, "blue").update(doors=1)

        async with FakeBroker() as broker:
            with self.assertRaises(MinosRepositoryNonProvidedException):
                await Car(1, 1, 3, "blue", _broker=broker).update(doors=1)

    async def test_delete_raises(self):
        with self.assertRaises(MinosBrokerNonProvidedException):
            await Car(1, 1, 3, "blue").delete()

        async with FakeBroker() as broker:
            with self.assertRaises(MinosRepositoryNonProvidedException):
                await Car(1, 1, 3, "blue", _broker=broker).delete()


if __name__ == "__main__":
    unittest.main()

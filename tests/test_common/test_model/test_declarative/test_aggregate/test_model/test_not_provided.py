"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    MinosBrokerNotProvidedException,
    MinosRepositoryNotProvidedException,
    MinosSnapshotNotProvidedException,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    FakeBroker,
    FakeRepository,
)


class TestAggregateNotProvided(unittest.IsolatedAsyncioTestCase):
    async def test_create_raises(self):
        with self.assertRaises(MinosBrokerNotProvidedException):
            await Car.create(doors=3, color="blue")
        async with FakeBroker() as broker:
            with self.assertRaises(MinosRepositoryNotProvidedException):
                await Car.create(doors=3, color="blue", _broker=broker)
        async with FakeBroker() as broker, FakeRepository() as repository:
            with self.assertRaises(MinosSnapshotNotProvidedException):
                await Car.create(doors=3, color="blue", _broker=broker, _repository=repository)

    async def test_get_one_raises(self):
        with self.assertRaises(MinosBrokerNotProvidedException):
            await Car.get_one(1)

        async with FakeBroker() as broker:
            with self.assertRaises(MinosRepositoryNotProvidedException):
                await Car.get_one(1, _broker=broker)

        async with FakeBroker() as broker, FakeRepository() as repository:
            with self.assertRaises(MinosSnapshotNotProvidedException):
                await Car.get_one(1, _broker=broker, _repository=repository)

    async def test_update_raises(self):
        with self.assertRaises(MinosBrokerNotProvidedException):
            await Car(1, 1, 3, "blue").update(doors=1)

        async with FakeBroker() as broker:
            with self.assertRaises(MinosRepositoryNotProvidedException):
                await Car(1, 1, 3, "blue", _broker=broker).update(doors=1)

        async with FakeBroker() as broker, FakeRepository() as repository:
            with self.assertRaises(MinosSnapshotNotProvidedException):
                await Car(1, 1, 3, "blue", _broker=broker, _repository=repository).update(doors=1)

    async def test_delete_raises(self):
        with self.assertRaises(MinosBrokerNotProvidedException):
            await Car(1, 1, 3, "blue").delete()

        async with FakeBroker() as broker:
            with self.assertRaises(MinosRepositoryNotProvidedException):
                await Car(1, 1, 3, "blue", _broker=broker).delete()

        async with FakeBroker() as broker, FakeRepository() as repository:
            with self.assertRaises(MinosSnapshotNotProvidedException):
                await Car(1, 1, 3, "blue", _broker=broker, _repository=repository).delete()


if __name__ == "__main__":
    unittest.main()

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

    async def test_get_raises(self):
        with self.assertRaises(MinosBrokerNotProvidedException):
            await Car.get(1)

        async with FakeBroker() as broker:
            with self.assertRaises(MinosRepositoryNotProvidedException):
                await Car.get(1, _broker=broker)

        async with FakeBroker() as broker, FakeRepository() as repository:
            with self.assertRaises(MinosSnapshotNotProvidedException):
                await Car.get(1, _broker=broker, _repository=repository)

    async def test_find_raises(self):
        with self.assertRaises(MinosBrokerNotProvidedException):
            # noinspection PyStatementEffect
            [c async for c in Car.find(1)]

        async with FakeBroker() as broker:
            with self.assertRaises(MinosRepositoryNotProvidedException):
                # noinspection PyStatementEffect
                [c async for c in Car.find(1, _broker=broker)]

        async with FakeBroker() as broker, FakeRepository() as repository:
            with self.assertRaises(MinosSnapshotNotProvidedException):
                # noinspection PyStatementEffect
                [c async for c in Car.find(1, _broker=broker, _repository=repository)]


if __name__ == "__main__":
    unittest.main()

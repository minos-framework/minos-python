import unittest

from minos.common import (
    MinosRepositoryNotProvidedException,
    MinosSnapshotNotProvidedException,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    FakeRepository,
)


class TestAggregateNotProvided(unittest.IsolatedAsyncioTestCase):
    async def test_create_raises(self):
        with self.assertRaises(MinosRepositoryNotProvidedException):
            await Car.create(doors=3, color="blue")
        async with FakeRepository() as repository:
            with self.assertRaises(MinosSnapshotNotProvidedException):
                await Car.create(doors=3, color="blue", _repository=repository)

    async def test_get_raises(self):
        with self.assertRaises(MinosSnapshotNotProvidedException):
            await Car.get(1)

    async def test_find_raises(self):
        with self.assertRaises(MinosSnapshotNotProvidedException):
            # noinspection PyStatementEffect
            [c async for c in Car.find(1)]


if __name__ == "__main__":
    unittest.main()

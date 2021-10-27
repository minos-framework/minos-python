import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    Condition,
    MinosRepositoryNotProvidedException,
    MinosSnapshotNotProvidedException,
)
from tests.aggregate_classes import (
    Car,
)
from tests.utils import (
    MinosTestCase,
)


class TestAggregateNotProvided(MinosTestCase):
    async def test_create_raises(self):
        with self.assertRaises(MinosRepositoryNotProvidedException):
            await Car.create(doors=3, color="blue", _repository=None)
        with self.assertRaises(MinosSnapshotNotProvidedException):
            await Car.create(doors=3, color="blue", _snapshot=None)

    async def test_get_raises(self):
        with self.assertRaises(MinosSnapshotNotProvidedException):
            # noinspection PyTypeChecker
            await Car.get(uuid4(), _snapshot=None)

    async def test_find_raises(self):
        with self.assertRaises(MinosSnapshotNotProvidedException):
            # noinspection PyTypeChecker
            [c async for c in Car.find(Condition.TRUE, _snapshot=None)]


if __name__ == "__main__":
    unittest.main()

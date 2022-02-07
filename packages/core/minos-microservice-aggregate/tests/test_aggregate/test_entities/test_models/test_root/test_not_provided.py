import unittest
from uuid import (
    uuid4,
)

from minos.aggregate import (
    Condition,
)
from minos.common import (
    NotProvidedException,
)
from tests.utils import (
    Car,
    MinosTestCase,
)


class TestRootEntityNotProvided(MinosTestCase):
    async def test_create_raises(self):
        with self.assertRaises(NotProvidedException):
            await Car.create(doors=3, color="blue", _repository=None)
        with self.assertRaises(NotProvidedException):
            await Car.create(doors=3, color="blue", _snapshot=None)

    async def test_get_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            await Car.get(uuid4(), _snapshot=None)

    async def test_get_all_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            [c async for c in Car.get_all(_snapshot=None)]

    async def test_find_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            [c async for c in Car.find(Condition.TRUE, _snapshot=None)]


if __name__ == "__main__":
    unittest.main()

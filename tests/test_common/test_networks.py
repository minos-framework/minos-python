import unittest
from abc import (
    ABC,
)

from minos.common import (
    MinosBroker,
    MinosHandler,
    MinosSetup,
)
from tests.model_classes import (
    Foo,
)
from tests.utils import (
    FakeBroker,
)


class TestMinosBroker(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.broker = FakeBroker()

    def test_abstract(self):
        self.assertTrue(issubclass(MinosBroker, (ABC, MinosSetup)))
        self.assertEqual({"send"}, MinosBroker.__abstractmethods__)

    async def test_send(self):
        await self.broker.send([Foo("red"), Foo("red")])
        self.assertEqual(1, self.broker.call_count)
        self.assertEqual({"data": [Foo("red"), Foo("red")]}, self.broker.call_kwargs)


class TestMinosHandler(unittest.IsolatedAsyncioTestCase):
    def test_abstract(self):
        self.assertTrue(issubclass(MinosHandler, (ABC, MinosSetup)))
        self.assertEqual({"get_many", "get_one"}, MinosHandler.__abstractmethods__)


if __name__ == "__main__":
    unittest.main()

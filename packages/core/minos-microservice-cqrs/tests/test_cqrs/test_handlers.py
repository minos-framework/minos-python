import unittest
from unittest.mock import (
    patch,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    Action,
    Delta,
    FieldDiff,
    FieldDiffContainer,
    Ref,
)
from minos.common import (
    current_datetime,
)
from minos.cqrs import (
    PreEventHandler,
)
from minos.networks import (
    BrokerClientPool,
)
from tests.utils import (
    Bar,
)


class TestPreEventHandler(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.uuid = uuid4()
        self.bars = [Bar(uuid4(), 1, "hello"), Bar(uuid4(), 1, "world")]
        self.now = current_datetime()
        self.diff = Delta(
            self.uuid,
            "Foo",
            1,
            Action.CREATE,
            self.now,
            FieldDiffContainer([FieldDiff("bars", list[Ref[Bar]], [b.uuid for b in self.bars])]),
        )

        self.broker_pool = BrokerClientPool({})

    async def test_handle_resolving_dependencies(self):
        value = Delta(
            self.uuid,
            "Foo",
            1,
            Action.CREATE,
            self.now,
            FieldDiffContainer([FieldDiff("bars", list[Ref[Bar]], self.bars)]),
        )

        with patch("minos.aggregate.RefResolver.resolve", return_value=value):
            observed = await PreEventHandler.handle(self.diff, resolve_references=True, broker_pool=self.broker_pool)

        self.assertEqual(value, observed)
        self.assertEqual(self.bars, [b.data for b in observed["bars"]])

    async def test_handle_not_aggregate_diff(self):
        observed = await PreEventHandler.handle(56)
        self.assertEqual(56, observed)

    async def test_handle_without_resolving_references(self):
        observed = await PreEventHandler.handle(self.diff)
        self.assertEqual(self.diff, observed)

    async def test_handle_raises(self):
        with patch("minos.aggregate.RefResolver.resolve", side_effect=ValueError):
            observed = await PreEventHandler.handle(self.diff, resolve_references=True, broker_pool=self.broker_pool)

        expected = Delta(
            self.uuid,
            "Foo",
            1,
            Action.CREATE,
            self.now,
            FieldDiffContainer([FieldDiff("bars", list[Ref[Bar]], [b.uuid for b in self.bars])]),
        )
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

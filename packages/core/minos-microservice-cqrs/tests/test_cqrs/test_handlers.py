import unittest
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.aggregate import (
    Action,
    Delta,
    FieldDiff,
    FieldDiffContainer,
    Ref,
    RefResolver,
)
from minos.common import (
    ModelType,
    current_datetime,
)
from minos.cqrs import (
    PreEventHandler,
)

Bar = ModelType.build("Bar", uuid=UUID, version=int, name=str)


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

    async def test_handle_resolving_dependencies(self):
        value = Delta(
            self.uuid,
            "Foo",
            1,
            Action.CREATE,
            self.now,
            FieldDiffContainer([FieldDiff("bars", list[Ref[Bar]], self.bars)]),
        )

        with patch.object(RefResolver, "resolve", return_value=value):
            observed = await PreEventHandler.handle(
                self.diff, resolve_references=True, broker_pool=object(), snapshot_repository=object()
            )

        self.assertEqual(value, observed)
        self.assertEqual(self.bars, [b.data for b in observed["bars"]])

    async def test_handle_not_aggregate_diff(self):
        observed = await PreEventHandler.handle(56)
        self.assertEqual(56, observed)

    async def test_handle_without_resolving_references(self):
        observed = await PreEventHandler.handle(self.diff)
        self.assertEqual(self.diff, observed)

    async def test_handle_raises(self):
        with patch.object(RefResolver, "resolve", side_effect=ValueError):
            observed = await PreEventHandler.handle(
                self.diff, resolve_references=True, broker_pool=object(), snapshot_repository=object()
            )

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

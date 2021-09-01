"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from unittest.mock import (
    AsyncMock,
    patch,
)
from uuid import (
    uuid4,
)

from minos.common import (
    Action,
    AggregateDiff,
    DataTransferObject,
    FieldDiff,
    FieldDiffContainer,
    ModelRef,
    current_datetime,
)
from minos.cqrs import (
    MinosNotAnyMissingReferenceException,
    MinosQueryServiceException,
    PreEventHandler,
)
from minos.saga import (
    Saga,
    SagaContext,
    SagaExecution,
    SagaStatus,
)
from tests.utils import (
    Bar,
    FakeSagaManager,
)


class TestPreEventHandler(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.uuid = uuid4()
        self.bars = [Bar(uuid4(), 1, "hello"), Bar(uuid4(), 1, "world")]
        self.now = current_datetime()
        self.diff = AggregateDiff(
            self.uuid,
            "Foo",
            1,
            Action.CREATE,
            self.now,
            FieldDiffContainer([FieldDiff("bars", list[ModelRef[Bar]], [b.uuid for b in self.bars])]),
        )
        self.saga_manager = FakeSagaManager()

    async def test_handle(self):
        execution = SagaExecution.from_saga(
            (
                Saga("")
                .step()
                .invoke_participant(
                    "GetBars", PreEventHandler.invoke_callback, SagaContext(uuids=list([b.uuid for b in self.bars])),
                )
                .commit(PreEventHandler.commit_callback, SagaContext(diff=self.diff))
            ),
            status=SagaStatus.Finished,
            context=SagaContext(
                diff=AggregateDiff(
                    self.uuid,
                    "Foo",
                    1,
                    Action.CREATE,
                    self.now,
                    FieldDiffContainer([FieldDiff("bars", list[ModelRef[Bar]], self.bars)]),
                )
            ),
        )
        mock = AsyncMock(return_value=execution)

        self.saga_manager.run = mock
        observed = await PreEventHandler.handle(self.diff, self.saga_manager)

        expected = AggregateDiff(
            self.uuid,
            "Foo",
            1,
            Action.CREATE,
            self.now,
            FieldDiffContainer([FieldDiff("bars", list[ModelRef[Bar]], self.bars)]),
        )
        self.assertEqual(expected, observed)

    async def test_handle_empty_missing(self):
        with patch("minos.cqrs.PreEventHandler.build_saga") as mock:
            mock.side_effect = MinosNotAnyMissingReferenceException("")
            observed = await PreEventHandler.handle(self.diff, self.saga_manager)
            self.assertEqual(self.diff, observed)

    async def test_handle_raises(self):
        execution = SagaExecution.from_saga(
            (
                Saga("")
                .step()
                .invoke_participant(
                    "GetBars", PreEventHandler.invoke_callback, SagaContext(uuids=list([b.uuid for b in self.bars]))
                )
                .commit(PreEventHandler.commit_callback, SagaContext(diff=self.diff))
            ),
            status=SagaStatus.Errored,
        )
        mock = AsyncMock(return_value=execution)

        self.saga_manager.run = mock
        with self.assertRaises(MinosQueryServiceException):
            await PreEventHandler.handle(self.diff, self.saga_manager)

    def test_build_saga(self):
        with patch("minos.common.ModelRefExtractor.build") as mock:
            mock.return_value = {"Bar": [b.uuid for b in self.bars]}
            observed = PreEventHandler.build_saga(self.diff)

        expected = (
            Saga("")
            .step()
            .invoke_participant(
                "GetBars", PreEventHandler.invoke_callback, SagaContext(uuids=[b.uuid for b in self.bars])
            )
            .on_reply("Bars")
            .commit(PreEventHandler.commit_callback, SagaContext(diff=self.diff))
        )
        self.assertEqual(expected, observed)

    def test_build_saga_empty_missing(self):
        diff = AggregateDiff(self.uuid, "Foo", 1, Action.CREATE, self.now, FieldDiffContainer.empty())
        with self.assertRaises(MinosNotAnyMissingReferenceException):
            PreEventHandler.build_saga(diff)

    def test_invoke_callback(self):
        context = SagaContext()
        uuids = [uuid4(), uuid4(), uuid4(), uuid4()]
        dto = PreEventHandler.invoke_callback(context, uuids)
        self.assertIsInstance(dto, DataTransferObject)
        self.assertEqual(uuids, dto.uuids)

    def test_commit_callback(self):
        observed = PreEventHandler.commit_callback(SagaContext(bars=self.bars), self.diff)

        expected = SagaContext(
            diff=AggregateDiff(
                self.uuid,
                "Foo",
                1,
                Action.CREATE,
                self.now,
                FieldDiffContainer([FieldDiff("bars", list[ModelRef[Bar]], self.bars)]),
            )
        )
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

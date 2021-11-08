import unittest
from unittest.mock import (
    AsyncMock,
    patch,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    Action,
    AggregateDiff,
    FieldDiff,
    FieldDiffContainer,
    ModelRef,
)
from minos.common import (
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
    SagaRequest,
    SagaResponse,
    SagaResponseStatus,
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
        execution = SagaExecution.from_definition(
            (
                Saga()
                .remote_step(PreEventHandler.on_execute, name="Bar", uuids=[b.uuid for b in self.bars])
                .on_success(PreEventHandler.on_success, name="Bar")
                .commit(PreEventHandler.commit, diff=self.diff)
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

    async def test_handle_not_aggregate_diff(self):
        observed = await PreEventHandler.handle(56, self.saga_manager, resolve_references=False)
        self.assertEqual(56, observed)

    async def test_handle_without_resolving_references(self):
        observed = await PreEventHandler.handle(self.diff, self.saga_manager, resolve_references=False)
        self.assertEqual(self.diff, observed)

    async def test_handle_empty_missing(self):
        with patch("minos.cqrs.PreEventHandler.build_saga") as mock:
            mock.side_effect = MinosNotAnyMissingReferenceException("")
            observed = await PreEventHandler.handle(self.diff, self.saga_manager)
            self.assertEqual(self.diff, observed)

    async def test_handle_raises(self):
        execution = SagaExecution.from_definition(
            (
                Saga()
                .remote_step(PreEventHandler.on_execute, name="Bar", uuids=[b.uuid for b in self.bars])
                .on_success(PreEventHandler.on_success, name="Bar")
                .commit(PreEventHandler.commit, diff=self.diff)
            ),
            status=SagaStatus.Errored,
        )
        mock = AsyncMock(return_value=execution)

        self.saga_manager.run = mock
        with self.assertRaises(MinosQueryServiceException):
            await PreEventHandler.handle(self.diff, self.saga_manager)

    def test_build_saga(self):
        with patch("minos.aggregate.ModelRefExtractor.build") as mock:
            mock.return_value = {"Bar": [b.uuid for b in self.bars]}
            observed = PreEventHandler.build_saga(self.diff)

        expected = (
            Saga()
            .remote_step(PreEventHandler.on_execute, name="Bar", uuids=[b.uuid for b in self.bars])
            .on_success(PreEventHandler.on_success, name="Bar")
            .commit(PreEventHandler.commit, diff=self.diff)
        )
        self.assertEqual(expected, observed)

    def test_build_saga_empty_missing(self):
        diff = AggregateDiff(self.uuid, "Foo", 1, Action.CREATE, self.now, FieldDiffContainer.empty())
        with self.assertRaises(MinosNotAnyMissingReferenceException):
            PreEventHandler.build_saga(diff)

    def test_on_execute(self):
        context = SagaContext()
        uuids = [uuid4(), uuid4(), uuid4(), uuid4()]
        request = PreEventHandler.on_execute(context, "Foo", uuids)
        self.assertEqual(SagaRequest("GetFoos", {"uuids": uuids}), request)

    async def test_on_success(self):
        context = SagaContext()
        values = [1, 2, 3, 4]
        response = SagaResponse(values, SagaResponseStatus.SUCCESS)
        observed = await PreEventHandler.on_success(context, response, "Foo")
        self.assertEqual(SagaContext(foos=values), observed)

    def test_commit(self):
        observed = PreEventHandler.commit(SagaContext(bars=self.bars), self.diff)

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

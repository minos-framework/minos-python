import unittest
from contextlib import (
    suppress,
)
from unittest.mock import (
    patch,
)
from uuid import (
    uuid4,
)

from minos.saga import (
    ConditionalSagaStep,
    ConditionalSagaStepExecution,
    ElseThenAlternative,
    IfThenAlternative,
    Saga,
    SagaContext,
    SagaFailedExecutionStepException,
    SagaPausedExecutionStepException,
    SagaResponse,
    SagaRollbackExecutionStepException,
    SagaStepExecution,
    SagaStepStatus,
)
from tests.utils import (
    DeleteOrderSaga,
    Foo,
    SagaTestCase,
    send_create_order,
)


def _is_one(context):
    return context["option"] == 1


def _is_two(context):
    return context["option"] == 2


class TestConditionalSageStepExecution(SagaTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.execute_kwargs = {
            "execution_uuid": uuid4(),
            "user": uuid4(),
        }

        self.definition = ConditionalSagaStep(
            [
                IfThenAlternative(
                    _is_one,
                    Saga().remote_step(send_create_order).on_success(DeleteOrderSaga.handle_order_success).commit(),
                ),
            ],
            ElseThenAlternative(
                (
                    Saga()
                    .remote_step(send_create_order)
                    .on_success(DeleteOrderSaga.handle_ticket_success_raises)
                    .on_failure(DeleteOrderSaga.send_delete_ticket)
                    .commit()
                )
            ),
        )
        # noinspection PyTypeChecker
        self.execution: ConditionalSagaStepExecution = SagaStepExecution.from_definition(self.definition)

    async def test_execute(self):
        context = SagaContext(option=1)

        with self.assertRaises(SagaPausedExecutionStepException):
            context = await self.execution.execute(context, **self.execute_kwargs)
        self.assertEqual(SagaStepStatus.PausedByOnExecute, self.execution.status)
        self.assertEqual(SagaContext(option=1), context)
        self.assertEqual({"order"}, self.execution.related_services)

        response = SagaResponse(Foo("order"), {"order"})
        with patch("minos.saga.SagaExecution.commit") as commit_mock:
            context = await self.execution.execute(context, response=response, **self.execute_kwargs)
        self.assertEqual(SagaStepStatus.Finished, self.execution.status)
        self.assertEqual({self.config.get_name()}, self.execution.related_services)
        self.assertEqual(SagaContext(option=1, order=Foo("order")), context)
        self.assertEqual(0, commit_mock.call_count)

    async def test_execute_raises_step(self):
        context = SagaContext(option=2)

        with self.assertRaises(SagaPausedExecutionStepException):
            context = await self.execution.execute(context, **self.execute_kwargs)
        self.assertEqual(SagaStepStatus.PausedByOnExecute, self.execution.status)
        self.assertEqual(SagaContext(option=2), context)

        response = SagaResponse(Foo("ticket"), {"ticket"})
        with patch("minos.saga.SagaExecution.rollback") as mock:
            with self.assertRaises(SagaFailedExecutionStepException):
                context = await self.execution.execute(context, response=response, **self.execute_kwargs)
            self.assertEqual(SagaStepStatus.ErroredByOnExecute, self.execution.status)
            self.assertEqual(SagaContext(option=2), context)
            self.assertEqual(1, mock.call_count)

    async def test_execute_empty(self):
        execution = ConditionalSagaStepExecution(ConditionalSagaStep())
        context = await execution.execute(SagaContext(one=1))
        self.assertEqual(SagaContext(one=1), context)
        self.assertEqual(SagaStepStatus.Finished, execution.status)

    async def test_rollback(self):
        with suppress(SagaPausedExecutionStepException):
            await self.execution.execute(SagaContext(option=1), **self.execute_kwargs)
        response = SagaResponse(Foo("order"), {"order"})
        await self.execution.execute(SagaContext(), response=response, **self.execute_kwargs)
        with patch("minos.saga.SagaExecution.rollback") as rollback_mock, patch(
            "minos.saga.SagaExecution.commit"
        ) as reject_mock:
            await self.execution.rollback(SagaContext(), **self.execute_kwargs)
        self.assertEqual(1, rollback_mock.call_count)
        self.assertEqual(0, reject_mock.call_count)

    async def test_rollback_raises_create(self):
        with self.assertRaises(SagaRollbackExecutionStepException):
            await self.execution.rollback(SagaContext())

    async def test_rollback_raises_already(self):
        with suppress(SagaPausedExecutionStepException):
            await self.execution.execute(SagaContext(option=1), **self.execute_kwargs)

        await self.execution.rollback(SagaContext(), **self.execute_kwargs)
        with self.assertRaises(SagaRollbackExecutionStepException):
            await self.execution.rollback(SagaContext(), **self.execute_kwargs)

    def test_raw_created(self):
        expected = {
            "already_rollback": False,
            "cls": "minos.saga.executions.steps.conditional.ConditionalSagaStepExecution",
            "definition": self.definition.raw,
            "inner": None,
            "status": "created",
            "related_services": [],
        }
        self.assertEqual(expected, self.execution.raw)

    async def test_raw_paused(self):
        with suppress(SagaPausedExecutionStepException):
            await self.execution.execute(SagaContext(option=1), **self.execute_kwargs)

        expected = {
            "already_rollback": False,
            "cls": "minos.saga.executions.steps.conditional.ConditionalSagaStepExecution",
            "definition": self.definition.raw,
            "inner": {
                "context": SagaContext(option=1).avro_str,
                "already_rollback": False,
                "definition": self.execution.inner.definition.raw,
                "executed_steps": [],
                "paused_step": {
                    "already_rollback": False,
                    "cls": "minos.saga.executions.steps.remote.RemoteSagaStepExecution",
                    "definition": {
                        "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                        "on_error": None,
                        "on_execute": {"callback": "tests.utils.send_create_order"},
                        "on_failure": None,
                        "on_success": {"callback": "tests.utils.DeleteOrderSaga.handle_order_success"},
                    },
                    "status": "paused-by-on-execute",
                    "related_services": ["order"],
                },
                "status": "paused",
                "user": str(self.execute_kwargs["user"]),
                "uuid": str(self.execute_kwargs["execution_uuid"]),
            },
            "related_services": ["order"],
            "status": "paused-by-on-execute",
        }
        observed = self.execution.raw

        self.assertEqual(
            SagaContext.from_avro_str(expected["inner"].pop("context")),
            SagaContext.from_avro_str(observed["inner"].pop("context")),
        )
        self.assertEqual(expected, observed)

    async def test_raw_finished(self):
        context = SagaContext(option=1)
        with suppress(SagaPausedExecutionStepException):
            context = await self.execution.execute(context, **self.execute_kwargs)
        response = SagaResponse(Foo("order"), {"order"})
        await self.execution.execute(context, response=response, **self.execute_kwargs)

        expected = {
            "already_rollback": False,
            "cls": "minos.saga.executions.steps.conditional.ConditionalSagaStepExecution",
            "definition": self.definition.raw,
            "inner": {
                "context": SagaContext(option=1, order=Foo(foo="order")).avro_str,
                "already_rollback": False,
                "definition": self.execution.inner.definition.raw,
                "executed_steps": [
                    {
                        "already_rollback": False,
                        "cls": "minos.saga.executions.steps.remote.RemoteSagaStepExecution",
                        "definition": {
                            "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                            "on_error": None,
                            "on_execute": {"callback": "tests.utils.send_create_order"},
                            "on_failure": None,
                            "on_success": {"callback": "tests.utils.DeleteOrderSaga.handle_order_success"},
                        },
                        "status": "finished",
                        "related_services": ["order"],
                    }
                ],
                "paused_step": None,
                "status": "finished",
                "user": str(self.execute_kwargs["user"]),
                "uuid": str(self.execute_kwargs["execution_uuid"]),
            },
            "status": "finished",
            "related_services": ["order"],
        }
        observed = self.execution.raw

        self.assertEqual(
            SagaContext.from_avro_str(expected["inner"].pop("context")),
            SagaContext.from_avro_str(observed["inner"].pop("context")),
        )
        self.assertEqual(expected, observed)

    async def test_raw_from_raw(self):
        with suppress(SagaPausedExecutionStepException):
            await self.execution.execute(SagaContext(option=1), **self.execute_kwargs)

        another = SagaStepExecution.from_raw(self.execution.raw)
        self.assertEqual(self.execution, another)


if __name__ == "__main__":
    unittest.main()

import unittest
import uuid
from unittest.mock import (
    MagicMock,
)

from minos.common import (
    CommandStatus,
    MinosConfig,
)
from minos.saga import (
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
    MinosSagaRollbackExecutionStepException,
    SagaContext,
    SagaStep,
    SagaStepExecution,
    SagaStepStatus,
)
from tests.utils import (
    BASE_PATH,
    Foo,
    NaiveBroker,
    fake_reply,
    handle_ticket_success,
    handle_ticket_success_raises,
    send_create_ticket,
    send_create_ticket_raises,
    send_delete_ticket,
)


class TestSagaStepExecution(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(path=BASE_PATH / "config.yml")
        self.broker = NaiveBroker()
        self.execute_kwargs = {
            "definition_name": "FoodAdd",
            "execution_uuid": uuid.uuid4(),
            "broker": self.broker,
            "reply_topic": "FooAdd",
        }

        self.publish_mock = MagicMock(side_effect=self.broker.send)
        self.broker.send = self.publish_mock

    async def test_execute_on_execute(self):
        step = SagaStep(send_create_ticket)
        context = SagaContext()
        execution = SagaStepExecution(step)

        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(context, **self.execute_kwargs)
        self.assertEqual(1, self.publish_mock.call_count)

        self.assertEqual(SagaStepStatus.PausedOnSuccess, execution.status)

        reply = fake_reply(status=CommandStatus.SUCCESS)
        await execution.execute(context, reply=reply, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.Finished, execution.status)

    async def test_execute_on_execute_errored(self):
        step = SagaStep(send_create_ticket_raises).on_failure(send_delete_ticket)
        context = SagaContext()
        execution = SagaStepExecution(step)

        with self.assertRaises(MinosSagaFailedExecutionStepException):
            await execution.execute(context, **self.execute_kwargs)
        self.assertEqual(0, self.publish_mock.call_count)

        self.assertEqual(SagaStepStatus.ErroredOnExecute, execution.status)

    async def test_execute_on_execute_errored_reply(self):
        step = SagaStep(send_create_ticket)
        context = SagaContext()
        execution = SagaStepExecution(step)

        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(context, **self.execute_kwargs)
        self.assertEqual(1, self.publish_mock.call_count)

        self.assertEqual(SagaStepStatus.PausedOnSuccess, execution.status)

        reply = fake_reply(status=CommandStatus.ERROR)
        with self.assertRaises(MinosSagaFailedExecutionStepException):
            await execution.execute(context, reply=reply, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.ErroredOnSuccess, execution.status)

    async def test_execute_on_execute_with_on_success(self):
        step = SagaStep(send_create_ticket).on_success(handle_ticket_success)
        context = SagaContext()
        execution = SagaStepExecution(step)

        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(context, **self.execute_kwargs)
        self.assertEqual(1, self.publish_mock.call_count)

        self.assertEqual(SagaStepStatus.PausedOnSuccess, execution.status)

        reply = fake_reply(Foo("foo"))
        await execution.execute(context, reply=reply, **self.execute_kwargs)
        self.assertEqual(SagaStepStatus.Finished, execution.status)

    async def test_execute_on_success(self):
        step = SagaStep(send_create_ticket).on_success(handle_ticket_success)
        context = SagaContext()
        execution = SagaStepExecution(step)

        reply = fake_reply(Foo("hello"))
        context = await execution.execute(context, reply=reply, **self.execute_kwargs)
        self.assertEqual(SagaContext(ticket=Foo("hello")), context)
        self.assertEqual(SagaStepStatus.Finished, execution.status)

    async def test_execute_on_success_errored(self):
        step = SagaStep(send_create_ticket).on_success(handle_ticket_success_raises)
        context = SagaContext()
        execution = SagaStepExecution(step)

        reply = fake_reply(Foo("foo"))
        with self.assertRaises(MinosSagaFailedExecutionStepException):
            await execution.execute(context, reply=reply, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.ErroredOnSuccess, execution.status)

    async def test_rollback(self):
        step = SagaStep(send_create_ticket).on_success(handle_ticket_success_raises).on_failure(send_delete_ticket)
        context = SagaContext()
        execution = SagaStepExecution(step)

        with self.assertRaises(MinosSagaRollbackExecutionStepException):
            await execution.rollback(context, **self.execute_kwargs)
        self.assertEqual(0, self.publish_mock.call_count)

        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(context, **self.execute_kwargs)

        self.assertEqual(1, self.publish_mock.call_count)
        self.publish_mock.reset_mock()

        await execution.rollback(context, **self.execute_kwargs)
        self.assertEqual(1, self.publish_mock.call_count)

        self.publish_mock.reset_mock()
        with self.assertRaises(MinosSagaRollbackExecutionStepException):
            await execution.rollback(context, **self.execute_kwargs)
        self.assertEqual(0, self.publish_mock.call_count)

    def test_raw(self):
        definition = SagaStep(send_create_ticket).on_success(handle_ticket_success).on_failure(send_delete_ticket)
        execution = SagaStepExecution(definition)

        expected = {
            "already_rollback": False,
            "definition": {
                "on_execute": {"callback": "tests.utils.send_create_ticket"},
                "on_success": {"callback": "tests.utils.handle_ticket_success"},
                "on_failure": {"callback": "tests.utils.send_delete_ticket"},
            },
            "status": "created",
        }
        self.assertEqual(expected, execution.raw)

    def test_from_raw(self):

        raw = {
            "already_rollback": False,
            "definition": {
                "on_execute": {"callback": "tests.utils.send_create_ticket"},
                "on_success": {"callback": "tests.utils.handle_ticket_success"},
                "on_failure": {"callback": "tests.utils.send_delete_ticket"},
            },
            "status": "created",
        }
        expected = SagaStepExecution(
            SagaStep(send_create_ticket).on_success(handle_ticket_success).on_failure(send_delete_ticket)
        )
        observed = SagaStepExecution.from_raw(raw)
        self.assertEqual(expected, observed)

    def test_from_raw_already(self):
        expected = SagaStepExecution(
            SagaStep(send_create_ticket).on_success(handle_ticket_success).on_failure(send_delete_ticket)
        )
        observed = SagaStepExecution.from_raw(expected)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

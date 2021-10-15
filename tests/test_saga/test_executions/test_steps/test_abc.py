import unittest
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

from minos.common import (
    MinosConfig,
)
from minos.saga import (
    LocalSagaStep,
    LocalSagaStepExecution,
    RemoteSagaStep,
    RemoteSagaStepExecution,
    SagaStepExecution,
)
from tests.utils import (
    BASE_PATH,
    NaiveBroker,
    handle_ticket_error,
    handle_ticket_success,
    send_create_ticket,
    send_delete_ticket,
)


class TestSagaStepExecution(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(path=BASE_PATH / "config.yml")
        self.broker = NaiveBroker()
        self.execute_kwargs = {
            "definition_name": "FoodAdd",
            "execution_uuid": uuid4(),
            "broker": self.broker,
            "reply_topic": "FooAdd",
            "user": uuid4(),
        }

        self.publish_mock = AsyncMock()
        self.broker.send = self.publish_mock

    def test_from_raw(self):
        raw = {
            "already_rollback": False,
            "definition": {
                "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                "on_execute": {"callback": "tests.utils.send_create_ticket"},
                "on_success": {"callback": "tests.utils.handle_ticket_success"},
                "on_error": {"callback": "tests.utils.handle_ticket_error"},
                "on_failure": {"callback": "tests.utils.send_delete_ticket"},
            },
            "status": "created",
        }
        expected = RemoteSagaStepExecution(
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        observed = RemoteSagaStepExecution.from_raw(raw)
        self.assertEqual(expected, observed)

    def test_from_raw_with_cls(self):
        raw = {
            "already_rollback": False,
            "cls": "minos.saga.executions.steps.remote.RemoteSagaStepExecution",
            "definition": {
                "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                "on_execute": {"callback": "tests.utils.send_create_ticket"},
                "on_success": {"callback": "tests.utils.handle_ticket_success"},
                "on_error": {"callback": "tests.utils.handle_ticket_error"},
                "on_failure": {"callback": "tests.utils.send_delete_ticket"},
            },
            "status": "created",
        }
        expected = RemoteSagaStepExecution(
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        observed = SagaStepExecution.from_raw(raw)
        self.assertEqual(expected, observed)

    def test_from_raw_already(self):
        expected = RemoteSagaStepExecution(
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        observed = SagaStepExecution.from_raw(expected)
        self.assertEqual(expected, observed)

    def test_from_raw_raises(self):
        with self.assertRaises(TypeError):
            SagaStepExecution.from_raw({"cls": "datetime.datetime"})

    def test_from_step(self):
        self.assertIsInstance(SagaStepExecution.from_step(RemoteSagaStep()), RemoteSagaStepExecution)
        self.assertIsInstance(SagaStepExecution.from_step(LocalSagaStep()), LocalSagaStepExecution)
        with self.assertRaises(TypeError):
            SagaStepExecution.from_step(123)

    def test_raw(self):
        definition = (
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        execution = RemoteSagaStepExecution(definition)

        expected = {
            "already_rollback": False,
            "cls": "minos.saga.executions.steps.remote.RemoteSagaStepExecution",
            "definition": {
                "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                "on_execute": {"callback": "tests.utils.send_create_ticket"},
                "on_success": {"callback": "tests.utils.handle_ticket_success"},
                "on_error": {"callback": "tests.utils.handle_ticket_error"},
                "on_failure": {"callback": "tests.utils.send_delete_ticket"},
            },
            "status": "created",
        }
        self.assertEqual(expected, execution.raw)


if __name__ == "__main__":
    unittest.main()

import unittest

from minos.saga import (
    LocalSagaStep,
    LocalSagaStepExecution,
    RemoteSagaStep,
    RemoteSagaStepExecution,
    SagaStepExecution,
)
from tests.utils import (
    handle_ticket_error,
    handle_ticket_success,
    send_create_ticket,
    send_delete_ticket,
)


class TestSagaStepExecution(unittest.IsolatedAsyncioTestCase):
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

    def test_from_definition(self):
        self.assertIsInstance(SagaStepExecution.from_definition(RemoteSagaStep()), RemoteSagaStepExecution)
        self.assertIsInstance(SagaStepExecution.from_definition(LocalSagaStep()), LocalSagaStepExecution)
        with self.assertRaises(TypeError):
            SagaStepExecution.from_definition(123)

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
            "related_services": None,
            "status": "created",
        }
        self.assertEqual(expected, execution.raw)


if __name__ == "__main__":
    unittest.main()

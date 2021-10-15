import unittest
import warnings
from unittest.mock import (
    MagicMock,
    call,
)

from minos.saga import (
    EmptySagaStepException,
    MultipleOnErrorException,
    MultipleOnExecuteException,
    MultipleOnFailureException,
    MultipleOnSuccessException,
    RemoteSagaStep,
    Saga,
    SagaNotDefinedException,
    SagaOperation,
    SagaStep,
    UndefinedOnExecuteException,
)
from tests.utils import (
    commit_callback,
    handle_ticket_error,
    handle_ticket_success,
    send_create_ticket,
    send_delete_ticket,
)


class TestSagaStep(unittest.TestCase):
    def test_on_execute_constructor(self):
        step = RemoteSagaStep(on_execute=send_create_ticket)
        self.assertEqual(SagaOperation(send_create_ticket), step.on_execute_operation)

    def test_on_execute_method(self):
        step = RemoteSagaStep().on_execute(send_create_ticket)
        self.assertEqual(SagaOperation(send_create_ticket), step.on_execute_operation)

    def test_on_execute_multiple_raises(self):
        with self.assertRaises(MultipleOnExecuteException):
            RemoteSagaStep(send_create_ticket).on_execute(send_create_ticket)

    def test_on_failure_constructor(self):
        step = RemoteSagaStep(on_failure=send_delete_ticket)
        self.assertEqual(SagaOperation(send_delete_ticket), step.on_failure_operation)

    def test_on_failure_method(self):
        step = RemoteSagaStep().on_failure(send_delete_ticket)
        self.assertEqual(SagaOperation(send_delete_ticket), step.on_failure_operation)

    def test_on_failure_multiple_raises(self):
        with self.assertRaises(MultipleOnFailureException):
            RemoteSagaStep().on_failure(send_delete_ticket).on_failure(send_delete_ticket)

    def test_on_success_constructor(self):
        step = RemoteSagaStep(on_success=handle_ticket_success)
        self.assertEqual(SagaOperation(handle_ticket_success), step.on_success_operation)

    def test_on_success_method(self):
        step = RemoteSagaStep().on_success(handle_ticket_success)
        self.assertEqual(SagaOperation(handle_ticket_success), step.on_success_operation)

    def test_on_success_multiple_raises(self):
        with self.assertRaises(MultipleOnSuccessException):
            RemoteSagaStep().on_success(handle_ticket_success).on_success(handle_ticket_success)

    def test_on_error_constructor(self):
        step = RemoteSagaStep(on_error=handle_ticket_error)
        self.assertEqual(SagaOperation(handle_ticket_error), step.on_error_operation)

    def test_on_error_method(self):
        step = RemoteSagaStep().on_error(handle_ticket_error)
        self.assertEqual(SagaOperation(handle_ticket_error), step.on_error_operation)

    def test_on_error_multiple_raises(self):
        with self.assertRaises(MultipleOnErrorException):
            RemoteSagaStep().on_error(handle_ticket_error).on_error(handle_ticket_error)

    def test_step(self):
        step = RemoteSagaStep(send_create_ticket, saga=Saga())
        mock = MagicMock(side_effect=step.remote)
        step.remote = mock
        with warnings.catch_warnings():
            step = step.remote()

        self.assertEqual(1, mock.call_count)
        self.assertIsInstance(step, RemoteSagaStep)

    def test_remote_validates(self):
        step = RemoteSagaStep(saga=Saga())
        mock = MagicMock(return_value=True)
        step.validate = mock
        step.remote()
        self.assertEqual(1, mock.call_count)

    def test_remote_raises_not_saga(self):
        with self.assertRaises(SagaNotDefinedException):
            RemoteSagaStep(send_create_ticket).remote()

    def test_commit(self):
        saga = Saga()
        step = RemoteSagaStep(send_create_ticket, saga=saga)
        mock = MagicMock(return_value=True)
        saga.commit = mock
        step.commit(commit_callback)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(commit_callback), mock.call_args)

    def test_commit_validates(self):
        step = RemoteSagaStep(send_create_ticket, saga=Saga())
        mock = MagicMock(return_value=True)
        step.validate = mock
        step.commit()
        self.assertEqual(1, mock.call_count)

    def test_submit(self):
        expected = Saga()
        observed = RemoteSagaStep(send_create_ticket, saga=expected).commit()
        self.assertEqual(expected, observed)

    def test_submit_raises(self):
        with self.assertRaises(SagaNotDefinedException):
            RemoteSagaStep(send_create_ticket).commit()

    def test_validate_raises_empty(self):
        with self.assertRaises(EmptySagaStepException):
            RemoteSagaStep(None).validate()

    def test_validate_raises_non_on_execute(self):
        with self.assertRaises(UndefinedOnExecuteException):
            RemoteSagaStep(None).on_failure(send_delete_ticket).validate()

    def test_raw(self):
        step = (
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )

        expected = {
            "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
            "on_execute": {"callback": "tests.utils.send_create_ticket"},
            "on_failure": {"callback": "tests.utils.send_delete_ticket"},
            "on_success": {"callback": "tests.utils.handle_ticket_success"},
            "on_error": {"callback": "tests.utils.handle_ticket_error"},
        }
        self.assertEqual(expected, step.raw)

    def test_from_raw(self):
        raw = {
            "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
            "on_execute": {"callback": "tests.utils.send_create_ticket"},
            "on_failure": {"callback": "tests.utils.send_delete_ticket"},
            "on_success": {"callback": "tests.utils.handle_ticket_success"},
            "on_error": {"callback": "tests.utils.handle_ticket_error"},
        }

        expected = (
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        self.assertEqual(expected, SagaStep.from_raw(raw))

    def test_from_raw_already(self):
        expected = (
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        observed = SagaStep.from_raw(expected)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

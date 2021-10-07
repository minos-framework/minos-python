import unittest
from unittest.mock import (
    MagicMock,
    call,
)

from minos.saga import (
    MinosMultipleOnExecuteException,
    MinosMultipleOnFailureException,
    MinosMultipleOnSuccessException,
    MinosSagaEmptyStepException,
    MinosSagaNotDefinedException,
    MinosUndefinedOnExecuteException,
    Saga,
    SagaOperation,
    SagaStep,
)
from tests.utils import (
    commit_callback,
    handle_ticket_success,
    send_create_ticket,
    send_delete_ticket,
)


class TestSagaStep(unittest.TestCase):
    def test_on_execute_constructor(self):
        step = SagaStep(on_execute=send_create_ticket)
        self.assertEqual(SagaOperation(send_create_ticket), step.on_execute_operation)

    def test_on_execute_method(self):
        step = SagaStep().on_execute(send_create_ticket)
        self.assertEqual(SagaOperation(send_create_ticket), step.on_execute_operation)

    def test_on_execute_multiple_raises(self):
        with self.assertRaises(MinosMultipleOnExecuteException):
            SagaStep(send_create_ticket).on_execute(send_create_ticket)

    def test_on_failure_constructor(self):
        step = SagaStep(on_failure=send_delete_ticket)
        self.assertEqual(SagaOperation(send_delete_ticket), step.on_failure_operation)

    def test_on_failure_method(self):
        step = SagaStep().on_failure(send_delete_ticket)
        self.assertEqual(SagaOperation(send_delete_ticket), step.on_failure_operation)

    def test_on_failure_multiple_raises(self):
        with self.assertRaises(MinosMultipleOnFailureException):
            SagaStep().on_failure(send_delete_ticket).on_failure(send_delete_ticket)

    def test_on_success_constructor(self):
        step = SagaStep(on_success=handle_ticket_success)
        self.assertEqual(SagaOperation(handle_ticket_success), step.on_success_operation)

    def test_on_success_method(self):
        step = SagaStep().on_success(handle_ticket_success)
        self.assertEqual(SagaOperation(handle_ticket_success), step.on_success_operation)

    def test_on_success_multiple_raises(self):
        with self.assertRaises(MinosMultipleOnSuccessException):
            SagaStep().on_success(handle_ticket_success).on_success(handle_ticket_success)

    def test_step_validates(self):
        step = SagaStep(saga=Saga())
        mock = MagicMock(return_value=True)
        step.validate = mock
        step.step()
        self.assertEqual(1, mock.call_count)

    def test_step_raises_not_saga(self):
        with self.assertRaises(MinosSagaNotDefinedException):
            SagaStep(send_create_ticket).step()

    def test_commit(self):
        saga = Saga()
        step = SagaStep(send_create_ticket, saga=saga)
        mock = MagicMock(return_value=True)
        saga.commit = mock
        step.commit(commit_callback)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(commit_callback), mock.call_args)

    def test_commit_validates(self):
        step = SagaStep(send_create_ticket, saga=Saga())
        mock = MagicMock(return_value=True)
        step.validate = mock
        step.commit()
        self.assertEqual(1, mock.call_count)

    def test_submit(self):
        expected = Saga()
        observed = SagaStep(send_create_ticket, saga=expected).commit()
        self.assertEqual(expected, observed)

    def test_submit_raises(self):
        with self.assertRaises(MinosSagaNotDefinedException):
            SagaStep(send_create_ticket).commit()

    def test_validate_raises_empty(self):
        with self.assertRaises(MinosSagaEmptyStepException):
            SagaStep(None).validate()

    def test_validate_raises_non_on_execute(self):
        with self.assertRaises(MinosUndefinedOnExecuteException):
            SagaStep(None).on_failure(send_delete_ticket).validate()

    def test_raw(self):
        step = SagaStep(send_create_ticket).on_success(handle_ticket_success).on_failure(send_delete_ticket)

        expected = {
            "on_execute": {"callback": "tests.utils.send_create_ticket"},
            "on_failure": {"callback": "tests.utils.send_delete_ticket"},
            "on_success": {"callback": "tests.utils.handle_ticket_success"},
        }
        self.assertEqual(expected, step.raw)

    def test_from_raw(self):
        raw = {
            "on_execute": {"callback": "tests.utils.send_create_ticket"},
            "on_failure": {"callback": "tests.utils.send_delete_ticket"},
            "on_success": {"callback": "tests.utils.handle_ticket_success"},
        }

        expected = SagaStep(send_create_ticket).on_success(handle_ticket_success).on_failure(send_delete_ticket)
        self.assertEqual(expected, SagaStep.from_raw(raw))

    def test_from_raw_already(self):
        expected = SagaStep(send_create_ticket).on_success(handle_ticket_success).on_failure(send_delete_ticket)
        observed = SagaStep.from_raw(expected)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

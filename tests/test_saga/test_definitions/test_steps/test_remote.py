import unittest

from minos.saga import (
    EmptySagaStepException,
    MultipleOnErrorException,
    MultipleOnExecuteException,
    MultipleOnFailureException,
    MultipleOnSuccessException,
    RemoteSagaStep,
    SagaOperation,
    SagaStep,
    UndefinedOnExecuteException,
)
from tests.utils import (
    handle_ticket_error,
    handle_ticket_success,
    send_create_ticket,
    send_delete_ticket,
)


class TestRemoteSagaStep(unittest.TestCase):
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

    def test_equals(self):
        base = (
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        another = (
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        self.assertEqual(another, base)

        another = (
            RemoteSagaStep(send_delete_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        self.assertNotEqual(another, base)

        another = (
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_error)
            .on_error(handle_ticket_error)
            .on_failure(send_delete_ticket)
        )
        self.assertNotEqual(another, base)

        another = (
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_success)
            .on_failure(send_delete_ticket)
        )
        self.assertNotEqual(another, base)

        another = (
            RemoteSagaStep(send_create_ticket)
            .on_success(handle_ticket_success)
            .on_error(handle_ticket_error)
            .on_failure(send_create_ticket)
        )
        self.assertNotEqual(another, base)


if __name__ == "__main__":
    unittest.main()

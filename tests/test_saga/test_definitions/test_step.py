import unittest
from unittest.mock import (
    MagicMock,
    call,
)

from minos.saga import (
    MinosMultipleInvokeParticipantException,
    MinosMultipleOnReplyException,
    MinosMultipleWithCompensationException,
    MinosSagaEmptyStepException,
    MinosSagaNotDefinedException,
    MinosUndefinedInvokeParticipantException,
    Saga,
    SagaStep,
)
from tests.utils import (
    commit_callback,
    handle_ticket_success,
    send_create_ticket,
    send_delete_ticket,
)


class TestSagaStep(unittest.TestCase):
    def test_invoke_participant_multiple_raises(self):
        with self.assertRaises(MinosMultipleInvokeParticipantException):
            SagaStep().invoke_participant(send_create_ticket).invoke_participant(send_create_ticket)

    def test_with_compensation_multiple_raises(self):
        with self.assertRaises(MinosMultipleWithCompensationException):
            SagaStep().with_compensation(send_delete_ticket).with_compensation(send_delete_ticket)

    def test_on_reply_multiple_raises(self):
        with self.assertRaises(MinosMultipleOnReplyException):
            SagaStep().on_reply(handle_ticket_success).on_reply(handle_ticket_success)

    def test_step_validates(self):
        step = SagaStep(Saga())
        mock = MagicMock(return_value=True)
        step.validate = mock
        step.step()
        self.assertEqual(1, mock.call_count)

    def test_step_raises_not_saga(self):
        with self.assertRaises(MinosSagaNotDefinedException):
            SagaStep().invoke_participant(send_create_ticket).step()

    def test_commit(self):
        saga = Saga()
        step = SagaStep(saga).invoke_participant(send_create_ticket)
        mock = MagicMock(return_value=True)
        saga.commit = mock
        step.commit(commit_callback)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(commit_callback), mock.call_args)

    def test_commit_validates(self):
        step = SagaStep(Saga())
        mock = MagicMock(return_value=True)
        step.validate = mock
        step.commit()
        self.assertEqual(1, mock.call_count)

    def test_submit(self):
        expected = Saga()
        observed = SagaStep(expected).invoke_participant(send_create_ticket).commit()
        self.assertEqual(expected, observed)

    def test_submit_raises(self):
        with self.assertRaises(MinosSagaNotDefinedException):
            SagaStep().invoke_participant(send_create_ticket).commit()

    def test_validate_raises_empty(self):
        with self.assertRaises(MinosSagaEmptyStepException):
            SagaStep().validate()

    def test_validate_raises_non_invoke_participant(self):
        with self.assertRaises(MinosUndefinedInvokeParticipantException):
            SagaStep().with_compensation(send_delete_ticket).validate()

    def test_has_reply_true(self):
        step = SagaStep().invoke_participant(send_create_ticket).on_reply(handle_ticket_success)
        self.assertTrue(step.has_reply)

    def test_has_reply_false(self):
        step = SagaStep().invoke_participant(send_create_ticket)
        self.assertFalse(step.has_reply)

    def test_raw(self):
        step = (
            SagaStep()
            .invoke_participant(send_create_ticket)
            .with_compensation(send_delete_ticket)
            .on_reply(handle_ticket_success)
        )

        expected = {
            "invoke_participant": {"callback": "tests.utils.send_create_ticket"},
            "with_compensation": {"callback": "tests.utils.send_delete_ticket"},
            "on_reply": {"callback": "tests.utils.handle_ticket_success"},
        }
        self.assertEqual(expected, step.raw)

    def test_from_raw(self):
        raw = {
            "invoke_participant": {"callback": "tests.utils.send_create_ticket"},
            "with_compensation": {"callback": "tests.utils.send_delete_ticket"},
            "on_reply": {"callback": "tests.utils.handle_ticket_success"},
        }

        expected = (
            SagaStep()
            .invoke_participant(send_create_ticket)
            .with_compensation(send_delete_ticket)
            .on_reply(handle_ticket_success)
        )
        self.assertEqual(expected, SagaStep.from_raw(raw))

    def test_from_raw_already(self):
        expected = (
            SagaStep()
            .invoke_participant(send_create_ticket)
            .with_compensation(send_delete_ticket)
            .on_reply(handle_ticket_success)
        )
        observed = SagaStep.from_raw(expected)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

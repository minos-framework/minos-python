"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    MinosException,
)
from minos.saga import (
    MinosAlreadyOnSagaException,
    MinosMultipleInvokeParticipantException,
    MinosMultipleOnReplyException,
    MinosMultipleWithCompensationException,
    MinosSagaEmptyStepException,
    MinosSagaException,
    MinosSagaExecutionException,
    MinosSagaExecutionNotFoundException,
    MinosSagaExecutionStepException,
    MinosSagaFailedExecutionStepException,
    MinosSagaNotDefinedException,
    MinosSagaPausedExecutionStepException,
    MinosSagaRollbackExecutionException,
    MinosSagaRollbackExecutionStepException,
    MinosSagaStepException,
    MinosUndefinedInvokeParticipantException,
)


class TestExceptions(unittest.TestCase):
    def test_type(self):
        self.assertTrue(issubclass(MinosSagaException, MinosException))

    def test_step(self):
        self.assertTrue(issubclass(MinosSagaStepException, MinosException))

    def test_step_saga_not_defined(self):
        self.assertTrue(issubclass(MinosSagaNotDefinedException, MinosSagaStepException))

    def test_step_saga_not_defined_repr(self):
        expected = (
            "MinosSagaNotDefinedException(message=\"A 'SagaStep' "
            "must have a 'Saga' instance to call call this method.\")"
        )
        self.assertEqual(expected, repr(MinosSagaNotDefinedException()))

    def test_step_empty(self):
        self.assertTrue(issubclass(MinosSagaEmptyStepException, MinosSagaStepException))

    def test_step_empty_repr(self):
        expected = "MinosSagaEmptyStepException(message=\"A 'SagaStep' must have at least one defined action.\")"
        self.assertEqual(expected, repr(MinosSagaEmptyStepException()))

    def test_step_multiple_invoke_participant(self):
        self.assertTrue(issubclass(MinosMultipleInvokeParticipantException, MinosSagaStepException))

    def test_step_multiple_invoke_participant_repr(self):
        expected = (
            "MinosMultipleInvokeParticipantException(message=\"A 'SagaStep' can "
            "only define one 'invoke_participant' method.\")"
        )
        self.assertEqual(expected, repr(MinosMultipleInvokeParticipantException()))

    def test_step_multiple_with_compensation(self):
        self.assertTrue(issubclass(MinosMultipleWithCompensationException, MinosSagaStepException))

    def test_step_multiple_with_compensation_repr(self):
        expected = (
            "MinosMultipleWithCompensationException(message=\"A 'SagaStep'"
            " can only define one 'with_compensation' method.\")"
        )
        self.assertEqual(expected, repr(MinosMultipleWithCompensationException()))

    def test_step_multiple_on_reply(self):
        self.assertTrue(issubclass(MinosMultipleOnReplyException, MinosSagaStepException))

    def test_step_multiple_on_reply_repr(self):
        expected = "MinosMultipleOnReplyException(message=\"A 'SagaStep' can only define one 'on_reply' method.\")"
        self.assertEqual(expected, repr(MinosMultipleOnReplyException()))

    def test_step_already_on_saga(self):
        self.assertTrue(issubclass(MinosAlreadyOnSagaException, MinosSagaStepException))

    def test_step_already_on_saga_repr(self):
        expected = "MinosAlreadyOnSagaException(message=\"A 'SagaStep' can only belong to one 'Saga' simultaneously.\")"
        self.assertEqual(expected, repr(MinosAlreadyOnSagaException()))

    def test_step_undefined_invoke_participant(self):
        self.assertTrue(issubclass(MinosUndefinedInvokeParticipantException, MinosSagaStepException))

    def test_step_undefined_invoke_participant_repr(self):
        expected = (
            "MinosUndefinedInvokeParticipantException(message=\"A 'SagaStep' "
            "must define at least the 'invoke_participant' logic.\")"
        )
        self.assertEqual(expected, repr(MinosUndefinedInvokeParticipantException()))

    def test_execution(self):
        self.assertTrue(issubclass(MinosSagaExecutionException, MinosException))

    def test_execution_not_found(self):
        self.assertTrue(issubclass(MinosSagaExecutionNotFoundException, MinosException))

    def test_execution_rollback(self):
        self.assertTrue(issubclass(MinosSagaRollbackExecutionException, MinosSagaExecutionException))

    def test_execution_step(self):
        self.assertTrue(issubclass(MinosSagaExecutionStepException, MinosException))

    def test_execution_step_failed_step(self):
        self.assertTrue(issubclass(MinosSagaFailedExecutionStepException, MinosSagaExecutionStepException))

    def test_execution_step_failed_step_repr(self):
        expected = (
            'MinosSagaFailedExecutionStepException(message="There was '
            "a failure while 'SagaExecutionStep' was executing.\")"
        )

        self.assertEqual(expected, repr(MinosSagaFailedExecutionStepException()))

    def test_execution_step_paused_step(self):
        self.assertTrue(issubclass(MinosSagaPausedExecutionStepException, MinosSagaExecutionStepException))

    def test_execution_step_paused_step_repr(self):
        expected = (
            'MinosSagaPausedExecutionStepException(message="There was '
            "a pause while 'SagaExecutionStep' was executing.\")"
        )
        self.assertEqual(expected, repr(MinosSagaPausedExecutionStepException()))

    def test_execution_step_rollback(self):
        self.assertTrue(issubclass(MinosSagaRollbackExecutionStepException, MinosSagaExecutionStepException))


if __name__ == "__main__":
    unittest.main()

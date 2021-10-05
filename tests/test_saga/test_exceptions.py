import unittest

from minos.common import (
    MinosException,
)
from minos.saga import (
    MinosAlreadyOnSagaException,
    MinosMultipleOnExecuteException,
    MinosMultipleOnFailureException,
    MinosMultipleOnSuccessException,
    MinosSagaEmptyStepException,
    MinosSagaException,
    MinosSagaExecutionException,
    MinosSagaExecutionNotFoundException,
    MinosSagaFailedExecutionStepException,
    MinosSagaNotDefinedException,
    MinosSagaPausedExecutionStepException,
    MinosSagaRollbackExecutionException,
    MinosSagaRollbackExecutionStepException,
    MinosSagaStepException,
    MinosSagaStepExecutionException,
    MinosUndefinedOnExecuteException,
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

    def test_step_multiple_on_execute(self):
        self.assertTrue(issubclass(MinosMultipleOnExecuteException, MinosSagaStepException))

    def test_step_multiple_on_execute_repr(self):
        expected = (
            "MinosMultipleOnExecuteException(message=\"A 'SagaStep' can " "only define one 'on_execute' method.\")"
        )
        self.assertEqual(expected, repr(MinosMultipleOnExecuteException()))

    def test_step_multiple_on_failure(self):
        self.assertTrue(issubclass(MinosMultipleOnFailureException, MinosSagaStepException))

    def test_step_multiple_on_failure_repr(self):
        expected = (
            "MinosMultipleOnFailureException(message=\"A 'SagaStep'" " can only define one 'on_failure' method.\")"
        )
        self.assertEqual(expected, repr(MinosMultipleOnFailureException()))

    def test_step_multiple_on_success(self):
        self.assertTrue(issubclass(MinosMultipleOnSuccessException, MinosSagaStepException))

    def test_step_multiple_on_success_repr(self):
        expected = "MinosMultipleOnSuccessException(message=\"A 'SagaStep' can only define one 'on_success' method.\")"
        self.assertEqual(expected, repr(MinosMultipleOnSuccessException()))

    def test_step_already_on_saga(self):
        self.assertTrue(issubclass(MinosAlreadyOnSagaException, MinosSagaStepException))

    def test_step_already_on_saga_repr(self):
        expected = "MinosAlreadyOnSagaException(message=\"A 'SagaStep' can only belong to one 'Saga' simultaneously.\")"
        self.assertEqual(expected, repr(MinosAlreadyOnSagaException()))

    def test_step_undefined_on_execute(self):
        self.assertTrue(issubclass(MinosUndefinedOnExecuteException, MinosSagaStepException))

    def test_step_undefined_on_execute_repr(self):
        expected = (
            "MinosUndefinedOnExecuteException(message=\"A 'SagaStep' " "must define at least the 'on_execute' logic.\")"
        )
        self.assertEqual(expected, repr(MinosUndefinedOnExecuteException()))

    def test_execution(self):
        self.assertTrue(issubclass(MinosSagaExecutionException, MinosException))

    def test_execution_not_found(self):
        self.assertTrue(issubclass(MinosSagaExecutionNotFoundException, MinosException))

    def test_execution_rollback(self):
        self.assertTrue(issubclass(MinosSagaRollbackExecutionException, MinosSagaExecutionException))

    def test_execution_step(self):
        self.assertTrue(issubclass(MinosSagaStepExecutionException, MinosException))

    def test_execution_step_failed_step(self):
        self.assertTrue(issubclass(MinosSagaFailedExecutionStepException, MinosSagaStepExecutionException))

    def test_execution_step_failed_step_repr(self):
        expected = (
            'MinosSagaFailedExecutionStepException(message="There was '
            "a failure while 'SagaStepExecution' was executing: ValueError('test')\")"
        )

        self.assertEqual(expected, repr(MinosSagaFailedExecutionStepException(ValueError("test"))))

    def test_execution_step_paused_step(self):
        self.assertTrue(issubclass(MinosSagaPausedExecutionStepException, MinosSagaStepExecutionException))

    def test_execution_step_paused_step_repr(self):
        expected = (
            'MinosSagaPausedExecutionStepException(message="There was '
            "a pause while 'SagaStepExecution' was executing.\")"
        )
        self.assertEqual(expected, repr(MinosSagaPausedExecutionStepException()))

    def test_execution_step_rollback(self):
        self.assertTrue(issubclass(MinosSagaRollbackExecutionStepException, MinosSagaStepExecutionException))


if __name__ == "__main__":
    unittest.main()

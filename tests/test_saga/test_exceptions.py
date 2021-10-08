import unittest

from minos.common import (
    MinosException,
)
from minos.saga import (
    AlreadyOnSagaException,
    EmptySagaStepException,
    MultipleOnErrorException,
    MultipleOnExecuteException,
    MultipleOnFailureException,
    MultipleOnSuccessException,
    SagaException,
    SagaExecutionException,
    SagaExecutionNotFoundException,
    SagaFailedExecutionStepException,
    SagaNotDefinedException,
    SagaPausedExecutionStepException,
    SagaRollbackExecutionException,
    SagaRollbackExecutionStepException,
    SagaStepException,
    SagaStepExecutionException,
    UndefinedOnExecuteException,
)


class TestExceptions(unittest.TestCase):
    def test_type(self):
        self.assertTrue(issubclass(SagaException, MinosException))

    def test_step(self):
        self.assertTrue(issubclass(SagaStepException, MinosException))

    def test_step_saga_not_defined(self):
        self.assertTrue(issubclass(SagaNotDefinedException, SagaStepException))

    def test_step_saga_not_defined_repr(self):
        expected = (
            "SagaNotDefinedException(message=\"A 'SagaStep' " "must have a 'Saga' instance to call call this method.\")"
        )
        self.assertEqual(expected, repr(SagaNotDefinedException()))

    def test_step_empty(self):
        self.assertTrue(issubclass(EmptySagaStepException, SagaStepException))

    def test_step_empty_repr(self):
        expected = "EmptySagaStepException(message=\"A 'SagaStep' must have at least one defined action.\")"
        self.assertEqual(expected, repr(EmptySagaStepException()))

    def test_step_multiple_on_execute(self):
        self.assertTrue(issubclass(MultipleOnExecuteException, SagaStepException))

    def test_step_multiple_on_execute_repr(self):
        expected = "MultipleOnExecuteException(message=\"A 'SagaStep' can " "only define one 'on_execute' method.\")"
        self.assertEqual(expected, repr(MultipleOnExecuteException()))

    def test_step_multiple_on_failure(self):
        self.assertTrue(issubclass(MultipleOnFailureException, SagaStepException))

    def test_step_multiple_on_failure_repr(self):
        expected = "MultipleOnFailureException(message=\"A 'SagaStep'" " can only define one 'on_failure' method.\")"
        self.assertEqual(expected, repr(MultipleOnFailureException()))

    def test_step_multiple_on_success(self):
        self.assertTrue(issubclass(MultipleOnSuccessException, SagaStepException))

    def test_step_multiple_on_success_repr(self):
        expected = "MultipleOnSuccessException(message=\"A 'SagaStep' can only define one 'on_success' method.\")"
        self.assertEqual(expected, repr(MultipleOnSuccessException()))

    def test_step_multiple_on_error(self):
        self.assertTrue(issubclass(MultipleOnErrorException, SagaStepException))

    def test_step_multiple_on_error_repr(self):
        expected = "MultipleOnErrorException(message=\"A 'SagaStep' can only define one 'on_error' method.\")"
        self.assertEqual(expected, repr(MultipleOnErrorException()))

    def test_step_already_on_saga(self):
        self.assertTrue(issubclass(AlreadyOnSagaException, SagaStepException))

    def test_step_already_on_saga_repr(self):
        expected = "AlreadyOnSagaException(message=\"A 'SagaStep' can only belong to one 'Saga' simultaneously.\")"
        self.assertEqual(expected, repr(AlreadyOnSagaException()))

    def test_step_undefined_on_execute(self):
        self.assertTrue(issubclass(UndefinedOnExecuteException, SagaStepException))

    def test_step_undefined_on_execute_repr(self):
        expected = (
            "UndefinedOnExecuteException(message=\"A 'SagaStep' " "must define at least the 'on_execute' logic.\")"
        )
        self.assertEqual(expected, repr(UndefinedOnExecuteException()))

    def test_execution(self):
        self.assertTrue(issubclass(SagaExecutionException, MinosException))

    def test_execution_not_found(self):
        self.assertTrue(issubclass(SagaExecutionNotFoundException, MinosException))

    def test_execution_rollback(self):
        self.assertTrue(issubclass(SagaRollbackExecutionException, SagaExecutionException))

    def test_execution_step(self):
        self.assertTrue(issubclass(SagaStepExecutionException, MinosException))

    def test_execution_step_failed_step(self):
        self.assertTrue(issubclass(SagaFailedExecutionStepException, SagaStepExecutionException))

    def test_execution_step_failed_step_repr(self):
        expected = (
            'SagaFailedExecutionStepException(message="There was '
            "a failure while 'SagaStepExecution' was executing: ValueError('test')\")"
        )

        self.assertEqual(expected, repr(SagaFailedExecutionStepException(ValueError("test"))))

    def test_execution_step_paused_step(self):
        self.assertTrue(issubclass(SagaPausedExecutionStepException, SagaStepExecutionException))

    def test_execution_step_paused_step_repr(self):
        expected = (
            'SagaPausedExecutionStepException(message="There was ' "a pause while 'SagaStepExecution' was executing.\")"
        )
        self.assertEqual(expected, repr(SagaPausedExecutionStepException()))

    def test_execution_step_rollback(self):
        self.assertTrue(issubclass(SagaRollbackExecutionStepException, SagaStepExecutionException))


if __name__ == "__main__":
    unittest.main()

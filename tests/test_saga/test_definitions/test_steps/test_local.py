import unittest

from minos.saga import (
    EmptySagaStepException,
    LocalSagaStep,
    MultipleOnExecuteException,
    MultipleOnFailureException,
    SagaOperation,
    SagaStep,
    UndefinedOnExecuteException,
)
from tests.utils import (
    create_payment,
    delete_payment,
)


class TestLocalSagaStep(unittest.TestCase):
    def test_on_execute_constructor(self):
        step = LocalSagaStep(on_execute=create_payment)
        self.assertEqual(SagaOperation(create_payment), step.on_execute_operation)

    def test_on_execute_method(self):
        step = LocalSagaStep().on_execute(create_payment)
        self.assertEqual(SagaOperation(create_payment), step.on_execute_operation)

    def test_on_execute_multiple_raises(self):
        with self.assertRaises(MultipleOnExecuteException):
            LocalSagaStep(create_payment).on_execute(create_payment)

    def test_on_failure_constructor(self):
        step = LocalSagaStep(on_failure=delete_payment)
        self.assertEqual(SagaOperation(delete_payment), step.on_failure_operation)

    def test_on_failure_method(self):
        step = LocalSagaStep().on_failure(delete_payment)
        self.assertEqual(SagaOperation(delete_payment), step.on_failure_operation)

    def test_on_failure_multiple_raises(self):
        with self.assertRaises(MultipleOnFailureException):
            LocalSagaStep().on_failure(delete_payment).on_failure(delete_payment)

    def test_validate_raises_empty(self):
        with self.assertRaises(EmptySagaStepException):
            LocalSagaStep().validate()

    def test_validate_raises_non_on_execute(self):
        with self.assertRaises(UndefinedOnExecuteException):
            LocalSagaStep().on_failure(delete_payment).validate()

    def test_raw(self):
        step = LocalSagaStep(create_payment).on_failure(delete_payment)

        expected = {
            "cls": "minos.saga.definitions.steps.local.LocalSagaStep",
            "on_execute": {"callback": "tests.utils.create_payment"},
            "on_failure": {"callback": "tests.utils.delete_payment"},
        }
        self.assertEqual(expected, step.raw)

    def test_from_raw(self):
        raw = {
            "cls": "minos.saga.definitions.steps.local.LocalSagaStep",
            "on_execute": {"callback": "tests.utils.create_payment"},
            "on_failure": {"callback": "tests.utils.delete_payment"},
        }

        expected = LocalSagaStep(create_payment).on_failure(delete_payment)
        self.assertEqual(expected, SagaStep.from_raw(raw))

    def test_from_raw_already(self):
        expected = LocalSagaStep(create_payment).on_failure(delete_payment)
        observed = SagaStep.from_raw(expected)
        self.assertEqual(expected, observed)

    def test_equals(self):
        base = LocalSagaStep(create_payment).on_failure(delete_payment)
        another = LocalSagaStep(create_payment).on_failure(delete_payment)
        self.assertEqual(another, base)

        another = LocalSagaStep(delete_payment).on_failure(delete_payment)
        self.assertNotEqual(another, base)

        another = LocalSagaStep(create_payment).on_failure(create_payment)
        self.assertNotEqual(another, base)


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import (
    patch,
)

from minos.saga import (
    ConditionalSagaStep,
    ElseThenAlternative,
    EmptySagaStepException,
    IfThenAlternative,
    SagaOperation,
    SagaStep,
)
from tests.utils import (
    ADD_ORDER,
    CREATE_PAYMENT,
    DELETE_ORDER,
    add_order_condition,
    delete_order_condition,
)


class TestConditionalSagaStep(unittest.TestCase):
    def setUp(self) -> None:
        self.if_then_conditions = [
            IfThenAlternative(add_order_condition, ADD_ORDER),
            IfThenAlternative(delete_order_condition, DELETE_ORDER),
        ]
        self.else_condition = ElseThenAlternative(CREATE_PAYMENT)

    def test_if_then(self):
        pass

    def test_else_then(self):
        pass

    def test_validate_raises_empty(self):
        with self.assertRaises(EmptySagaStepException):
            ConditionalSagaStep().validate()

    def test_raw(self):
        step = ConditionalSagaStep(if_then=self.if_then_conditions, else_then=self.else_condition)

        expected = {
            "cls": "minos.saga.definitions.steps.conditional.ConditionalSagaStep",
            "else_then": self.else_condition.raw,
            "if_then": [self.if_then_conditions[0].raw, self.if_then_conditions[1].raw],
        }
        self.assertEqual(expected, step.raw)

    def test_from_raw(self):
        raw = {
            "cls": "minos.saga.definitions.steps.conditional.ConditionalSagaStep",
            "else_then": self.else_condition.raw,
            "if_then": [self.if_then_conditions[0].raw, self.if_then_conditions[1].raw],
        }

        expected = ConditionalSagaStep(if_then=self.if_then_conditions, else_then=self.else_condition)
        observed = SagaStep.from_raw(raw)
        self.assertEqual(expected, observed)

    def test_from_raw_already(self):
        expected = ConditionalSagaStep(if_then=self.if_then_conditions, else_then=self.else_condition)
        observed = SagaStep.from_raw(expected)
        self.assertEqual(expected, observed)

    def test_equals(self):
        pass


class TestIfThenAlternative(unittest.TestCase):
    def setUp(self) -> None:
        self.alternative = IfThenAlternative(add_order_condition, ADD_ORDER)

    def test_condition(self):
        self.assertEqual(SagaOperation(add_order_condition), self.alternative.condition)

    def test_saga(self):
        self.assertEqual(ADD_ORDER, self.alternative.saga)

    def test_validate(self):
        with patch("minos.saga.Saga.validate") as mock:
            self.alternative.validate()
            self.assertEqual(1, mock.call_count)

    def test_raw(self):
        expected = {"condition": {"callback": "tests.utils.add_order_condition"}, "saga": ADD_ORDER.raw}

        self.assertEqual(expected, self.alternative.raw)

    def test_from_raw(self):
        observed = IfThenAlternative.from_raw(
            {"condition": {"callback": "tests.utils.add_order_condition"}, "saga": ADD_ORDER.raw}
        )
        self.assertEqual(self.alternative, observed)

    def test_from_raw_already(self):
        observed = IfThenAlternative.from_raw(self.alternative)
        self.assertEqual(self.alternative, observed)

    def test_equals(self):
        pass


class TestElseThenAlternative(unittest.TestCase):
    def setUp(self) -> None:
        self.alternative = ElseThenAlternative(CREATE_PAYMENT)

    def test_saga(self):
        self.assertEqual(CREATE_PAYMENT, self.alternative.saga)

    def test_validate(self):
        with patch("minos.saga.Saga.validate") as mock:
            self.alternative.validate()
            self.assertEqual(1, mock.call_count)

    def test_raw(self):
        expected = {"saga": CREATE_PAYMENT.raw}

        self.assertEqual(expected, self.alternative.raw)

    def test_from_raw(self):
        observed = ElseThenAlternative.from_raw({"saga": CREATE_PAYMENT.raw})
        self.assertEqual(self.alternative, observed)

    def test_from_raw_already(self):
        observed = ElseThenAlternative.from_raw(self.alternative)
        self.assertEqual(self.alternative, observed)

    def test_equals(self):
        pass


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import (
    patch,
)

from minos.saga import (
    ConditionalSagaStep,
    ElseThenAlternative,
    EmptySagaStepException,
    IfThenAlternative,
    MultipleElseThenException,
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
        self.if_then = [
            IfThenAlternative(add_order_condition, ADD_ORDER),
            IfThenAlternative(delete_order_condition, DELETE_ORDER),
        ]
        self.else_then = ElseThenAlternative(CREATE_PAYMENT)

    def test_constructor(self):
        step = ConditionalSagaStep(if_then=self.if_then[0], else_then=self.else_then)
        self.assertEqual([self.if_then[0]], step.if_then_alternatives)
        self.assertEqual(self.else_then, step.else_then_alternative)

    def test_if_then(self):
        observed = (
            ConditionalSagaStep().if_then(add_order_condition, ADD_ORDER).if_then(delete_order_condition, DELETE_ORDER)
        )
        expected = ConditionalSagaStep(if_then=self.if_then)
        self.assertEqual(expected, observed)

    def test_else_then(self):
        observed = ConditionalSagaStep().else_then(CREATE_PAYMENT)
        expected = ConditionalSagaStep(else_then=self.else_then)
        self.assertEqual(expected, observed)

    def test_else_then_raises(self):
        with self.assertRaises(MultipleElseThenException):
            ConditionalSagaStep().else_then(CREATE_PAYMENT).else_then(CREATE_PAYMENT)

    def test_validate(self):
        with patch("minos.saga.IfThenAlternative.validate") as if_mock, patch(
            "minos.saga.ElseThenAlternative.validate"
        ) as else_mock:
            ConditionalSagaStep(if_then=self.if_then, else_then=self.else_then).validate()

            self.assertEqual(2, if_mock.call_count)
            self.assertEqual(1, else_mock.call_count)

    def test_validate_raises(self):
        with self.assertRaises(EmptySagaStepException):
            ConditionalSagaStep().validate()

    def test_raw(self):
        step = ConditionalSagaStep(if_then=self.if_then, else_then=self.else_then)

        expected = {
            "cls": "minos.saga.definitions.steps.conditional.ConditionalSagaStep",
            "else_then": self.else_then.raw,
            "if_then": [self.if_then[0].raw, self.if_then[1].raw],
        }
        self.assertEqual(expected, step.raw)

    def test_from_raw(self):
        raw = {
            "cls": "minos.saga.definitions.steps.conditional.ConditionalSagaStep",
            "else_then": self.else_then.raw,
            "if_then": [self.if_then[0].raw, self.if_then[1].raw],
        }

        expected = ConditionalSagaStep(if_then=self.if_then, else_then=self.else_then)
        observed = SagaStep.from_raw(raw)
        self.assertEqual(expected, observed)

    def test_from_raw_without_else(self):
        raw = {
            "cls": "minos.saga.definitions.steps.conditional.ConditionalSagaStep",
            "else_then": None,
            "if_then": [self.if_then[0].raw, self.if_then[1].raw],
        }

        expected = ConditionalSagaStep(if_then=self.if_then)
        observed = SagaStep.from_raw(raw)
        self.assertEqual(expected, observed)

    def test_from_raw_already(self):
        expected = ConditionalSagaStep(if_then=self.if_then, else_then=self.else_then)
        observed = SagaStep.from_raw(expected)
        self.assertEqual(expected, observed)

    def test_equals(self):
        base = ConditionalSagaStep(if_then=self.if_then, else_then=self.else_then)

        another = ConditionalSagaStep(if_then=self.if_then, else_then=self.else_then)
        self.assertEqual(another, base)

        another = ConditionalSagaStep(else_then=self.else_then)
        self.assertNotEqual(another, base)

        another = ConditionalSagaStep(if_then=self.if_then)
        self.assertNotEqual(another, base)


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
        another = IfThenAlternative(add_order_condition, ADD_ORDER)
        self.assertEqual(another, self.alternative)

        another = IfThenAlternative(delete_order_condition, ADD_ORDER)
        self.assertNotEqual(another, self.alternative)

        another = IfThenAlternative(add_order_condition, DELETE_ORDER)
        self.assertNotEqual(another, self.alternative)


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
        another = ElseThenAlternative(CREATE_PAYMENT)
        self.assertEqual(another, self.alternative)

        another = ElseThenAlternative(ADD_ORDER)
        self.assertNotEqual(another, self.alternative)


if __name__ == "__main__":
    unittest.main()

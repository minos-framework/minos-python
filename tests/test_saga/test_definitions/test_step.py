"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from unittest.mock import (
    MagicMock,
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
    SagaStepOperation,
)
from tests.utils import (
    foo_fn,
)


class TestSagaStepOperation(unittest.TestCase):
    def test_raw(self):
        step = SagaStepOperation("CreateFoo", foo_fn)
        expected = {"callback": "tests.utils.foo_fn", "name": "CreateFoo"}
        self.assertEqual(expected, step.raw)

    def test_from_raw(self):
        raw = {"callback": "tests.utils.foo_fn", "name": "CreateFoo"}

        expected = SagaStepOperation("CreateFoo", foo_fn)
        self.assertEqual(expected, SagaStepOperation.from_raw(raw))

    def test_from_raw_already(self):
        expected = SagaStepOperation("CreateFoo", foo_fn)
        observed = SagaStepOperation.from_raw(expected)
        self.assertEqual(expected, observed)


class TestSagaStep(unittest.TestCase):
    def test_invoke_participant_multiple_raises(self):
        with self.assertRaises(MinosMultipleInvokeParticipantException):
            SagaStep().invoke_participant("foo", foo_fn).invoke_participant("foo", foo_fn)

    def test_with_compensation_multiple_raises(self):
        with self.assertRaises(MinosMultipleWithCompensationException):
            SagaStep().with_compensation("foo", foo_fn).with_compensation("foo", foo_fn)

    def test_on_reply_multiple_raises(self):
        with self.assertRaises(MinosMultipleOnReplyException):
            SagaStep().on_reply("foo").on_reply("foo")

    def test_step_validates(self):
        step = SagaStep(Saga("SagaTest"))
        mock = MagicMock(return_value=True)
        step.validate = mock
        step.step()
        self.assertEqual(1, mock.call_count)

    def test_step_raises_not_saga(self):
        with self.assertRaises(MinosSagaNotDefinedException):
            SagaStep().invoke_participant("FooAdded", foo_fn).step()

    def test_submit(self):
        expected = Saga("SagaTest")
        observed = SagaStep(expected).invoke_participant("FoodAdd", foo_fn).commit()
        self.assertEqual(expected, observed)

    def test_submit_raises(self):
        with self.assertRaises(MinosSagaNotDefinedException):
            SagaStep().invoke_participant("FoodAdd", foo_fn).commit()

    def test_validate_raises_empty(self):
        with self.assertRaises(MinosSagaEmptyStepException):
            SagaStep().validate()

    def test_validate_raises_non_invoke_participant(self):
        with self.assertRaises(MinosUndefinedInvokeParticipantException):
            SagaStep().with_compensation("UserRemove", foo_fn).validate()

    def test_has_reply_true(self):
        step = SagaStep().invoke_participant("FoodAdd", foo_fn).on_reply("foo")
        self.assertTrue(step.has_reply)

    def test_has_reply_false(self):
        step = SagaStep().invoke_participant("FoodAdd", foo_fn)
        self.assertFalse(step.has_reply)

    def test_raw(self):
        step = SagaStep().invoke_participant("FoodAdd", foo_fn).with_compensation("FooDelete", foo_fn).on_reply("foo")
        expected = {
            "invoke_participant": {"callback": "tests.utils.foo_fn", "name": "FoodAdd"},
            "with_compensation": {"callback": "tests.utils.foo_fn", "name": "FooDelete"},
            "on_reply": {"callback": "minos.saga.definitions.step.identity_fn", "name": "foo"},
        }
        self.assertEqual(expected, step.raw)

    def test_from_raw(self):
        raw = {
            "invoke_participant": {"callback": "tests.utils.foo_fn", "name": "FoodAdd"},
            "with_compensation": {"callback": "tests.utils.foo_fn", "name": "FooDelete"},
            "on_reply": {"callback": "minos.saga.definitions.step.identity_fn", "name": "foo"},
        }

        expected = (
            SagaStep().invoke_participant("FoodAdd", foo_fn).with_compensation("FooDelete", foo_fn).on_reply("foo")
        )
        self.assertEqual(expected, SagaStep.from_raw(raw))

    def test_from_raw_already(self):
        expected = (
            SagaStep().invoke_participant("FoodAdd", foo_fn).with_compensation("FooDelete", foo_fn).on_reply("foo")
        )
        observed = SagaStep.from_raw(expected)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

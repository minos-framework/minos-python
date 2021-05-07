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
    Saga,
    SagaStep,
)
from minos.saga.exceptions import (
    MinosUndefinedInvokeParticipantException,
)


def _callback():
    ...


class TestSagaStep(unittest.TestCase):
    def test_invoke_participant_multiple_raises(self):
        with self.assertRaises(MinosMultipleInvokeParticipantException):
            SagaStep().invoke_participant("foo").invoke_participant("foo")

    def test_with_compensation_multiple_raises(self):
        with self.assertRaises(MinosMultipleWithCompensationException):
            SagaStep().with_compensation("foo").with_compensation("foo")

    def test_on_reply_multiple_raises(self):
        with self.assertRaises(MinosMultipleOnReplyException):
            SagaStep().on_reply(_callback).on_reply(_callback)

    def test_step_validates(self):
        step = SagaStep(Saga("SagaTest"))
        mock = MagicMock(return_value=True)
        step.validate = mock
        step.step()
        self.assertEqual(1, mock.call_count)

    def test_step_raises_not_saga(self):
        with self.assertRaises(MinosSagaNotDefinedException):
            SagaStep().invoke_participant("FooAdded").step()

    def test_submit(self):
        expected = Saga("SagaTest")
        observed = SagaStep(expected).commit()
        self.assertEqual(expected, observed)

    def test_submit_raises(self):
        with self.assertRaises(MinosSagaNotDefinedException):
            SagaStep().commit()

    def test_validate_raises_empty(self):
        with self.assertRaises(MinosSagaEmptyStepException):
            SagaStep().validate()

    def test_validate_raises_non_invoke_participant(self):
        with self.assertRaises(MinosUndefinedInvokeParticipantException):
            SagaStep().with_compensation("UserRemove").validate()


if __name__ == "__main__":
    unittest.main()

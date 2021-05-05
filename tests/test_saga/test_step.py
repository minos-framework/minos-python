"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.saga import (
    MinosMultipleInvokeParticipantException,
    MinosMultipleOnReplyException,
    MinosMultipleWithCompensationException,
    SagaStep,
)
from tests.callbacks import (
    a_callback,
)


class TestSomething(unittest.TestCase):
    def test_invoke_participant_multiple_raises(self):
        with self.assertRaises(MinosMultipleInvokeParticipantException):
            SagaStep().invoke_participant("foo").invoke_participant("foo")

    def test_with_compensation_multiple_raises(self):
        with self.assertRaises(MinosMultipleWithCompensationException):
            SagaStep().with_compensation("foo").with_compensation("foo")

    def test_on_reply_multiple_raises(self):
        with self.assertRaises(MinosMultipleOnReplyException):
            SagaStep().on_reply(a_callback).on_reply(a_callback)


if __name__ == "__main__":
    unittest.main()

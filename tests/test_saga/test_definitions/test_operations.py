"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.saga import (
    SagaOperation,
)
from tests.utils import (
    foo_fn,
)


class TestSagaOperation(unittest.TestCase):
    def test_raw(self):
        step = SagaOperation(foo_fn, "CreateFoo")
        expected = {"callback": "tests.utils.foo_fn", "name": "CreateFoo"}
        self.assertEqual(expected, step.raw)

    def test_from_raw(self):
        raw = {"callback": "tests.utils.foo_fn", "name": "CreateFoo"}

        expected = SagaOperation(foo_fn, "CreateFoo")
        self.assertEqual(expected, SagaOperation.from_raw(raw))

    def test_from_raw_already(self):
        expected = SagaOperation(foo_fn, "CreateFoo")
        observed = SagaOperation.from_raw(expected)
        self.assertEqual(expected, observed)

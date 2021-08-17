"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from collections.abc import (
    Mapping,
)

from minos.common import (
    Model,
)


class TestModel(unittest.TestCase):
    def test_base(self):
        self.assertTrue(issubclass(Model, Mapping))


if __name__ == "__main__":
    unittest.main()

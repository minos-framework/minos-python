"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.common import (
    MissingSentinel,
    NoneType,
)


class TestNoneType(unittest.TestCase):
    def test_equal(self):
        self.assertEqual(type(None), NoneType)


class TestMissingSentinel(unittest.TestCase):
    def test_subclass(self):
        self.assertTrue(issubclass(MissingSentinel, object))


if __name__ == "__main__":
    unittest.main()

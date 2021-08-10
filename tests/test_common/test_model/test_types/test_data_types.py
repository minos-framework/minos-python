"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from typing import (
    Generic,
)

from minos.common import (
    MissingSentinel,
)


class TestMissingSentinel(unittest.TestCase):
    def test_subclass(self):
        # noinspection PyTypeHints
        self.assertTrue(issubclass(MissingSentinel, Generic))


if __name__ == "__main__":
    unittest.main()

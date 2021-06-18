"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest

from minos.common import (
    MinosException,
)
from minos.networks import (
    MinosNetworkException,
)


class TestExceptions(unittest.TestCase):
    def test_type(self):
        self.assertTrue(issubclass(MinosNetworkException, MinosException))


if __name__ == "__main__":
    unittest.main()

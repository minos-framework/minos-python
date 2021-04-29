"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.networks import (
    MinosSnapshotDispatcher,
)


class TestMinosSnapshotDispatcher(unittest.TestCase):
    def test_type(self):
        self.assertTrue(issubclass(MinosSnapshotDispatcher, object))


if __name__ == "__main__":
    unittest.main()

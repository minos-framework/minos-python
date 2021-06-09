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
    MinosPreviousVersionSnapshotException,
    MinosSnapshotException,
)
from tests.utils import (
    Bar,
)


class TestExceptions(unittest.TestCase):
    def test_type(self):
        self.assertTrue(issubclass(MinosNetworkException, MinosException))

    def test_snapshot(self):
        self.assertTrue(issubclass(MinosSnapshotException, MinosNetworkException))

    def test_snapshot_previous_version(self):
        self.assertTrue(issubclass(MinosPreviousVersionSnapshotException, MinosSnapshotException))

    def test_snapshot_previous_version_repr(self):
        previous = Bar(1, 2, "blue")
        new = Bar(1, 1, "blue")
        exception = MinosPreviousVersionSnapshotException(previous, new)
        expected = (
            "MinosPreviousVersionSnapshotException(message=\"Version for 'tests.utils.Bar' "
            'aggregate must be greater than 2. Obtained: 1")'
        )
        self.assertEqual(expected, repr(exception))


if __name__ == "__main__":
    unittest.main()

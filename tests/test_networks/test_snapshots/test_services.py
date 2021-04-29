"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from aiomisc.service.periodic import PeriodicService
from minos.networks import MinosSnapshotService


class TestMinosSnapshotService(unittest.TestCase):
    def test_type(self):
        self.assertTrue(issubclass(MinosSnapshotService, PeriodicService))


if __name__ == "__main__":
    unittest.main()

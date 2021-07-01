"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.common import (
    BucketModel,
)


class TestBucketModel(unittest.IsolatedAsyncioTestCase):
    def test_empty(self):
        expected = BucketModel({})
        observed = BucketModel.empty()
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.common import (
    BucketModel,
    Field,
)


class TestBucketModel(unittest.IsolatedAsyncioTestCase):
    def test_get_item(self):
        bucket = BucketModel({Field("doors", int, 5), Field("color", str, "red")})
        self.assertEqual(5, bucket["doors"])
        self.assertEqual("red", bucket["color"])

    def test_set_item(self):
        expected = BucketModel({Field("doors", int, 5), Field("color", str, "red")})

        observed = BucketModel({Field("doors", int, 3), Field("color", str, "blue")})
        observed["doors"] = 5
        observed["color"] = "red"

        self.assertEqual(expected, observed)

    def test_empty(self):
        expected = BucketModel({})
        observed = BucketModel.empty()
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

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

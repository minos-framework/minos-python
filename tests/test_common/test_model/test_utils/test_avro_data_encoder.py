"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.common import (
    AvroDataEncoder,
    MinosMalformedAttributeException,
)


class _Foo:
    """For testing purposes"""


class TestAvroDataEncoder(unittest.TestCase):
    def test_build_float(self):
        encoder = AvroDataEncoder(3.5)
        self.assertEqual(3.5, encoder.build())

    def test_build_raises(self):
        encoder = AvroDataEncoder(_Foo())
        with self.assertRaises(MinosMalformedAttributeException):
            encoder.build()


if __name__ == "__main__":
    unittest.main()

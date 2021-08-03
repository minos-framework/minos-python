"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest

from minos.common import (
    NULL_UUID,
    UUID_REGEX,
)


class TestConstants(unittest.TestCase):
    def test_null_uuid(self):
        self.assertEqual(r"00000000-0000-0000-0000-000000000000", str(NULL_UUID))

    def test_uuid_regex(self):
        self.assertEqual(r"\w{8}-\w{4}-\w{4}-\w{4}-\w{12}", UUID_REGEX.pattern)


if __name__ == "__main__":
    unittest.main()

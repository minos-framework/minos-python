"""tests.test_common.test_datetime module."""
import unittest
from datetime import (
    datetime,
    timezone,
)
from time import (
    time,
)

from minos.common import (
    current_datetime,
)


class TestDatetime(unittest.TestCase):
    def test_current_datetime_type(self):
        observed = current_datetime()
        self.assertIsInstance(observed, datetime)

    def test_current_datetime_timezone(self):
        observed = current_datetime()
        self.assertEqual(observed.tzinfo, timezone.utc)

    def test_current_datetime_now(self):
        observed = current_datetime()
        self.assertAlmostEqual(time(), observed.timestamp(), delta=5)


if __name__ == "__main__":
    unittest.main()

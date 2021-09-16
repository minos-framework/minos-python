import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    NULL_UUID,
    UUID_REGEX,
)


class TestUuids(unittest.TestCase):
    def test_null_uuid(self):
        self.assertEqual(r"00000000-0000-0000-0000-000000000000", str(NULL_UUID))

    def test_uuid_regex(self):
        uuid = str(uuid4())
        self.assertRegex(uuid, UUID_REGEX)


if __name__ == "__main__":
    unittest.main()

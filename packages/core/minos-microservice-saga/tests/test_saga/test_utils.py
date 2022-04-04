import unittest

from minos.common import (
    NotProvidedException,
)
from minos.saga import (
    get_service_name,
)
from tests.utils import (
    MinosSagaTestCase,
)


class TestUtils(MinosSagaTestCase):
    def test_get_service_name(self):
        self.assertEqual("order", get_service_name())

    def test_get_service_name_raises(self):
        with self.assertRaises(NotProvidedException):
            # noinspection PyTypeChecker
            get_service_name(config=None)


if __name__ == "__main__":
    unittest.main()

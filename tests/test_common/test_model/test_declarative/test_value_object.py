from unittest import (
    TestCase,
)

from minos.common.exceptions import (
    MinosImmutableClassException,
)
from tests.value_objects import (
    Address,
)


class TestValueObject(TestCase):
    def setUp(self) -> None:
        self.address = Address(street="street name", number=42, zip_code=42042)

    def test_instantiate(self):
        self.assertEqual("street name", self.address.street)
        self.assertEqual(42, self.address.number)
        self.assertEqual(42042, self.address.zip_code)

    def test_raise_when_accessed(self):
        with self.assertRaises(MinosImmutableClassException):
            self.address.street = "this assignment must raise"

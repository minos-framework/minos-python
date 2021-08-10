from unittest import (
    TestCase,
)

from minos.common import (
    MinosImmutableClassException,
    ValueObject,
    ValueObjectSet,
)
from tests.value_objects import (
    Address,
    Location,
)


class FakeValueObject(ValueObject):
    """Fake Value Object"""


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


class TestValueObjectSet(TestCase):
    def setUp(self) -> None:
        self.location_1 = Location(street="street name")
        self.location_2 = Location(street="another street name")
        self.fake_value_obj = [self.location_1, self.location_2]

    def test_data(self):
        value_objects = ValueObjectSet(self.fake_value_obj)
        self.assertEqual([v for v in self.fake_value_obj], value_objects.data)

    def test_eq_true(self):
        observed = ValueObjectSet(self.fake_value_obj)

        self.assertEqual(self.fake_value_obj, observed.data)
        self.assertEqual([v for v in self.fake_value_obj], observed.data)

    def test_eq_false(self):
        raw = self.fake_value_obj
        observed = ValueObjectSet(raw)
        other = [Location("Test")]
        self.assertNotEqual(ValueObjectSet(other), observed.data)
        self.assertNotEqual(other, observed.data)
        self.assertNotEqual([v for v in other], observed.data)

    def test_len(self):
        value_objects = ValueObjectSet(self.fake_value_obj)
        self.assertEqual(2, len(value_objects))

    def test_iter(self):
        value_objects = ValueObjectSet(self.fake_value_obj)
        self.assertEqual(self.fake_value_obj, value_objects.data)

    def test_contains(self):
        raw = [self.location_1]

        value_objects = ValueObjectSet(raw)

        self.assertIn(raw[0], value_objects.data)
        self.assertNotIn(self.location_2, value_objects.data)
        self.assertNotIn(1234, value_objects.data)

    def test_add(self):

        value_objects = ValueObjectSet()
        value_objects.add(self.location_1)

        self.assertEqual([self.location_1], value_objects.data)

    def test_remove(self):

        value_objects = ValueObjectSet(self.fake_value_obj)
        value_objects.remove(self.location_1)

        self.assertEqual([self.location_2], value_objects.data)

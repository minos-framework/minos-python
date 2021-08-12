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
        self.fake_value_obj = {str(hash(self.location_1)): self.location_1, str(hash(self.location_2)): self.location_2}
        self.fake_value_obj_set = (self.location_1, self.location_2)

    def test_data(self):
        value_objects = ValueObjectSet(self.fake_value_obj)
        self.assertEqual(self.fake_value_obj, value_objects)

    def test_from_set(self):
        value_objects = ValueObjectSet(self.fake_value_obj_set)
        self.assertEqual(self.fake_value_obj, value_objects)

    def test_eq_true(self):
        observed = ValueObjectSet(self.fake_value_obj)

        self.assertEqual(self.fake_value_obj, observed)
        self.assertEqual({k: v for k, v in self.fake_value_obj.items()}, observed)

    def test_eq_false(self):
        raw = self.fake_value_obj
        observed = ValueObjectSet(raw)
        loc = Location("Test")
        other = {str(hash(loc)): Location("Test")}

        self.assertNotEqual(ValueObjectSet(other), set(raw))
        self.assertNotEqual(ValueObjectSet(other), observed)
        self.assertNotEqual(other, observed)
        self.assertNotEqual({k: v for k, v in other.items()}, observed)

    def test_len(self):
        value_objects = ValueObjectSet(self.fake_value_obj)
        self.assertEqual(2, len(value_objects))

    def test_iter(self):
        value_objects = ValueObjectSet(self.fake_value_obj)
        self.assertEqual(self.fake_value_obj, value_objects)

    def test_contains(self):
        str_hash = str(hash(self.location_1))
        raw = {str_hash: self.location_1}

        value_objects = ValueObjectSet(raw)

        self.assertIn(str_hash, value_objects)
        self.assertNotIn(self.location_2, value_objects)
        self.assertNotIn(1234, value_objects)

    def test_add(self):

        value_objects = ValueObjectSet()
        value_objects.add(self.location_1)

        str_hash = str(hash(self.location_1))
        raw = {str_hash: self.location_1}

        self.assertEqual(raw, value_objects)

    def test_remove(self):

        value_objects = ValueObjectSet(self.fake_value_obj)
        value_objects.discard(self.location_1)

        str_hash = str(hash(self.location_2))
        raw = {str_hash: self.location_2}
        self.assertEqual(raw, value_objects)

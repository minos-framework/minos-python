import unittest
from unittest import (
    TestCase,
)

from minos.aggregate import (
    Action,
    IncrementalSetDiff,
    IncrementalSetDiffEntry,
    ValueObject,
    ValueObjectException,
    ValueObjectSet,
)


class _Location(ValueObject):
    street: str


class TestValueObject(TestCase):
    def setUp(self) -> None:
        self.value = _Location(street="street name")

    def test_instantiate(self):
        self.assertEqual("street name", self.value.street)

    def test_raise_when_accessed(self):
        with self.assertRaises(ValueObjectException):
            self.value["street"] = "this assignment must raise"


class TestValueObjectSet(TestCase):
    def setUp(self) -> None:
        self.location_1 = _Location(street="street name")
        self.location_2 = _Location(street="another street name")
        self.fake_value_obj = {self.location_1, self.location_2}
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

    def test_eq_false(self):
        raw = self.fake_value_obj
        observed = ValueObjectSet(raw)
        other = {_Location("Test")}

        self.assertNotEqual(ValueObjectSet(other), set(raw))
        self.assertNotEqual(ValueObjectSet(other), observed)
        self.assertNotEqual(other, observed)

    def test_len(self):
        value_objects = ValueObjectSet(self.fake_value_obj)
        self.assertEqual(2, len(value_objects))

    def test_iter(self):
        value_objects = ValueObjectSet(self.fake_value_obj)
        self.assertEqual(self.fake_value_obj, set(value_objects))

    def test_contains(self):
        raw = {self.location_1}

        value_objects = ValueObjectSet(raw)

        self.assertIn(self.location_1, value_objects)
        self.assertNotIn(self.location_2, value_objects)
        self.assertNotIn(1234, value_objects)

    def test_add(self):
        value_objects = ValueObjectSet()
        value_objects.add(self.location_1)

        raw = {self.location_1}

        self.assertEqual(raw, value_objects)

    def test_remove(self):
        value_objects = ValueObjectSet(self.fake_value_obj)
        value_objects.discard(self.location_1)

        raw = {self.location_2}
        self.assertEqual(raw, value_objects)

    def test_diff(self):
        raw = [_Location(street="street name"), _Location(street="another street name")]
        entities = ValueObjectSet(raw)

        observed = entities.diff(ValueObjectSet([raw[0]]))
        expected = IncrementalSetDiff([IncrementalSetDiffEntry(Action.CREATE, raw[1])])

        self.assertEqual(observed, expected)


class TestValueObjectSetDiff(TestCase):
    def setUp(self) -> None:
        self.raw = [_Location(street="street name"), _Location(street="another street name")]
        self.old = ValueObjectSet(self.raw)

        self.clone = [_Location(street=entity.street) for entity in self.raw]

    def test_from_difference_create(self):
        entities = ValueObjectSet(self.clone)
        new = _Location("San Anton, 23")
        entities.add(new)

        observed = IncrementalSetDiff.from_difference(entities, self.old)
        expected = IncrementalSetDiff([IncrementalSetDiffEntry(Action.CREATE, new)])
        self.assertEqual(expected, observed)

    def test_from_difference_delete(self):
        entities = ValueObjectSet(self.clone)
        removed = self.clone[1]
        entities.remove(removed)

        observed = IncrementalSetDiff.from_difference(entities, self.old)
        expected = IncrementalSetDiff([IncrementalSetDiffEntry(Action.DELETE, removed)])
        self.assertEqual(expected, observed)

    def test_from_difference_combined(self):
        entities = ValueObjectSet(self.clone)
        new = _Location("Europa, 12")
        entities.add(new)

        removed = self.clone[1]
        entities.remove(removed)

        observed = IncrementalSetDiff.from_difference(entities, self.old)

        expected = IncrementalSetDiff(
            [IncrementalSetDiffEntry(Action.CREATE, new), IncrementalSetDiffEntry(Action.DELETE, removed)]
        )
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

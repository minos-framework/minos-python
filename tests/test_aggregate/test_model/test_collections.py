import unittest

from minos.aggregate import (
    Action,
    IncrementalSet,
    IncrementalSetDiff,
    IncrementalSetDiffEntry,
)


class TestIncrementalSet(unittest.TestCase):
    def test_data(self):
        value_objects = IncrementalSet({1, 2})
        self.assertEqual({1, 2}, value_objects)

    def test_from_set(self):
        value_objects = IncrementalSet({1, 2})
        self.assertEqual({1, 2}, value_objects)

    def test_eq_true(self):
        observed = IncrementalSet({1, 2})

        self.assertEqual({1, 2}, observed)

    def test_eq_false(self):
        raw = {1, 2}
        observed = IncrementalSet(raw)
        other = {1}

        self.assertNotEqual(IncrementalSet(other), set(raw))
        self.assertNotEqual(IncrementalSet(other), observed)
        self.assertNotEqual(other, observed)

    def test_len(self):
        value_objects = IncrementalSet({1, 2})
        self.assertEqual(2, len(value_objects))

    def test_iter(self):
        value_objects = IncrementalSet({1, 2})
        self.assertEqual({1, 2}, set(value_objects))

    def test_contains(self):
        raw = {1}

        value_objects = IncrementalSet(raw)

        self.assertIn(1, value_objects)
        self.assertNotIn(2, value_objects)
        self.assertNotIn(1234, value_objects)

    def test_add(self):
        value_objects = IncrementalSet()
        value_objects.add(1)

        raw = {1}

        self.assertEqual(raw, value_objects)

    def test_remove(self):
        value_objects = IncrementalSet({1, 2})
        value_objects.discard(1)

        raw = {2}
        self.assertEqual(raw, value_objects)

    def test_diff(self):
        raw = [1, 2]
        entities = IncrementalSet(raw)

        observed = entities.diff(IncrementalSet([raw[0]]))
        expected = IncrementalSetDiff([IncrementalSetDiffEntry(Action.CREATE, raw[1])])

        self.assertEqual(observed, expected)

    def test_data_cls(self):
        raw = [1, 2]
        entities = IncrementalSet(raw)
        self.assertEqual(int, entities.data_cls)


class TestIncrementalSetDiff(unittest.TestCase):
    def setUp(self) -> None:
        self.raw = [1, 2]
        self.old = IncrementalSet(self.raw)

        self.clone = [v for v in self.raw]

    def test_from_difference_create(self):
        entities = IncrementalSet(self.clone)
        new = 3
        entities.add(new)

        observed = IncrementalSetDiff.from_difference(entities, self.old)
        expected = IncrementalSetDiff([IncrementalSetDiffEntry(Action.CREATE, new)])
        self.assertEqual(expected, observed)

    def test_from_difference_delete(self):
        entities = IncrementalSet(self.clone)
        removed = self.clone[1]
        entities.remove(removed)

        observed = IncrementalSetDiff.from_difference(entities, self.old)
        expected = IncrementalSetDiff([IncrementalSetDiffEntry(Action.DELETE, removed)])
        self.assertEqual(expected, observed)

    def test_from_difference_combined(self):
        entities = IncrementalSet(self.clone)
        new = 3
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

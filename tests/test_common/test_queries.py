import unittest

from minos.common import (
    Condition,
    Ordering,
)


class TestCondition(unittest.TestCase):

    def test_hash(self):
        self.assertIsInstance(hash(Condition.EQUAL("foo", 3)), int)

    def test_eq(self):
        self.assertEqual(Condition.EQUAL("foo", 3), Condition.EQUAL("foo", 3))
        self.assertNotEqual(Condition.EQUAL("foo", 3), Condition.EQUAL("bar", 3))
        self.assertNotEqual(Condition.EQUAL("foo", 3), Condition.EQUAL("foo", 2))
        self.assertNotEqual(Condition.EQUAL("foo", 3), Condition.LOWER_EQUAL("foo", 3))

    def test_iter(self):
        self.assertEqual(("foo", 3), tuple(Condition.EQUAL("foo", 3)))

    def test_condition_true(self):
        self.assertEqual("_TrueCondition()", repr(Condition.TRUE))

    def test_condition_false(self):
        self.assertEqual("_FalseCondition()", repr(Condition.FALSE))

    def test_condition_and(self):
        self.assertEqual(
            "_AndCondition(_TrueCondition(), _FalseCondition())", repr(Condition.AND(Condition.TRUE, Condition.FALSE))
        )

    def test_condition_or(self):
        self.assertEqual(
            "_OrCondition(_TrueCondition(), _FalseCondition())", repr(Condition.OR(Condition.TRUE, Condition.FALSE))
        )

    def test_condition_not(self):
        self.assertEqual("_NotCondition(_TrueCondition())", repr(Condition.NOT(Condition.TRUE)))

    def test_condition_lower(self):
        self.assertEqual("_LowerCondition('foo', 3)", repr(Condition.LOWER("foo", 3)))

    def test_condition_lower_equal(self):
        self.assertEqual("_LowerEqualCondition('foo', 3)", repr(Condition.LOWER_EQUAL("foo", 3)))

    def test_condition_greater(self):
        self.assertEqual("_GreaterCondition('foo', 3)", repr(Condition.GREATER("foo", 3)))

    def test_condition_greater_than(self):
        self.assertEqual("_GreaterEqualCondition('foo', 3)", repr(Condition.GREATER_EQUAL("foo", 3)))

    def test_condition_equal(self):
        self.assertEqual("_EqualCondition('foo', 3)", repr(Condition.EQUAL("foo", 3)))

    def test_condition_not_equal(self):
        self.assertEqual("_NotEqualCondition('foo', 3)", repr(Condition.NOT_EQUAL("foo", 3)))

    def test_condition_in(self):
        self.assertEqual("_InCondition('foo', 3)", repr(Condition.IN("foo", 3)))


class TestOrdering(unittest.TestCase):

    def test_hash(self):
        self.assertIsInstance(hash(Condition.EQUAL("foo", 3)), int)

    def test_eq(self):
        self.assertEqual(Ordering.ASC("foo"), Ordering.ASC("foo"))
        self.assertNotEqual(Ordering.ASC("foo"), Ordering.ASC("bar"))
        self.assertNotEqual(Ordering.ASC("foo"), Ordering.DESC("foo"))

    def test_ordering_asc(self):
        self.assertEqual("_Ordering('foo', False)", repr(Ordering.ASC("foo")))

    def test_ordering_desc(self):
        self.assertEqual("_Ordering('foo', True)", repr(Ordering.DESC("foo")))


if __name__ == '__main__':
    unittest.main()

import unittest

from minos.aggregate import (
    Condition,
    Ordering,
)
from minos.common import (
    DeclarativeModel,
)


class _Number(DeclarativeModel):
    """For testing purposes"""

    value: int


class TestCondition(unittest.TestCase):
    def test_hash(self):
        self.assertIsInstance(hash(Condition.EQUAL("value", 3)), int)

    def test_eq(self):
        self.assertEqual(Condition.EQUAL("value", 3), Condition.EQUAL("value", 3))
        self.assertNotEqual(Condition.EQUAL("value", 3), Condition.EQUAL("bar", 3))
        self.assertNotEqual(Condition.EQUAL("value", 3), Condition.EQUAL("value", 2))
        self.assertNotEqual(Condition.EQUAL("value", 3), Condition.LOWER_EQUAL("value", 3))

    def test_iter(self):
        self.assertEqual(("value", 3), tuple(Condition.EQUAL("value", 3)))

    def test_condition_true(self):
        condition = Condition.TRUE

        self.assertEqual("_TrueCondition()", repr(condition))

        self.assertTrue(condition.evaluate(_Number(42)))
        self.assertTrue(condition.evaluate(_Number(56)))

    def test_condition_false(self):
        condition = Condition.FALSE

        self.assertEqual("_FalseCondition()", repr(condition))

        self.assertFalse(condition.evaluate(_Number(42)))
        self.assertFalse(condition.evaluate(_Number(56)))

    def test_condition_and(self):
        condition = Condition.AND(Condition.GREATER("value", 50), Condition.LOWER("value", 60))
        self.assertEqual("_AndCondition(_GreaterCondition('value', 50), _LowerCondition('value', 60))", repr(condition))

        self.assertFalse(condition.evaluate(_Number(42)))
        self.assertTrue(condition.evaluate(_Number(56)))
        self.assertFalse(condition.evaluate(_Number(60)))

    def test_condition_or(self):
        condition = Condition.OR(Condition.LOWER("value", 50), Condition.GREATER("value", 60))
        self.assertEqual("_OrCondition(_LowerCondition('value', 50), _GreaterCondition('value', 60))", repr(condition))

        self.assertTrue(condition.evaluate(_Number(42)))
        self.assertFalse(condition.evaluate(_Number(56)))
        self.assertTrue(condition.evaluate(_Number(61)))

    def test_condition_not(self):
        condition = Condition.NOT(Condition.LOWER("value", 50))
        self.assertEqual("_NotCondition(_LowerCondition('value', 50))", repr(condition))

        self.assertFalse(condition.evaluate(_Number(42)))
        self.assertTrue(condition.evaluate(_Number(56)))

    def test_condition_lower(self):
        condition = Condition.LOWER("value", 50)
        self.assertEqual("_LowerCondition('value', 50)", repr(condition))

        self.assertTrue(condition.evaluate(_Number(42)))
        self.assertFalse(condition.evaluate(_Number(56)))

    def test_condition_lower_equal(self):
        condition = Condition.LOWER_EQUAL("value", 42)
        self.assertEqual("_LowerEqualCondition('value', 42)", repr(condition))

        self.assertTrue(condition.evaluate(_Number(12)))
        self.assertTrue(condition.evaluate(_Number(42)))
        self.assertFalse(condition.evaluate(_Number(56)))

    def test_condition_greater(self):
        condition = Condition.GREATER("value", 50)
        self.assertEqual("_GreaterCondition('value', 50)", repr(condition))

        self.assertFalse(condition.evaluate(_Number(42)))
        self.assertTrue(condition.evaluate(_Number(56)))

    def test_condition_greater_than(self):
        condition = Condition.GREATER_EQUAL("value", 56)
        self.assertEqual("_GreaterEqualCondition('value', 56)", repr(condition))

        self.assertFalse(condition.evaluate(_Number(42)))
        self.assertTrue(condition.evaluate(_Number(56)))
        self.assertTrue(condition.evaluate(_Number(60)))

    def test_condition_equal(self):
        condition = Condition.EQUAL("value", 56)
        self.assertEqual("_EqualCondition('value', 56)", repr(condition))

        self.assertFalse(condition.evaluate(_Number(42)))
        self.assertTrue(condition.evaluate(_Number(56)))
        self.assertFalse(condition.evaluate(_Number(60)))

    def test_condition_not_equal(self):
        condition = Condition.NOT_EQUAL("value", 42)
        self.assertEqual("_NotEqualCondition('value', 42)", repr(condition))

        self.assertFalse(condition.evaluate(_Number(42)))
        self.assertTrue(condition.evaluate(_Number(56)))
        self.assertTrue(condition.evaluate(_Number(60)))

    def test_condition_in(self):
        condition = Condition.IN("value", [1, 2, 56])
        self.assertEqual("_InCondition('value', [1, 2, 56])", repr(condition))

        self.assertFalse(condition.evaluate(_Number(42)))
        self.assertTrue(condition.evaluate(_Number(56)))


class TestOrdering(unittest.TestCase):
    def test_hash(self):
        self.assertIsInstance(hash(Ordering.ASC("value")), int)

    def test_eq(self):
        self.assertEqual(Ordering.ASC("value"), Ordering.ASC("value"))
        self.assertNotEqual(Ordering.ASC("value"), Ordering.ASC("bar"))
        self.assertNotEqual(Ordering.ASC("value"), Ordering.DESC("value"))

    def test_ordering_asc(self):
        self.assertEqual("_Ordering('value', False)", repr(Ordering.ASC("value")))

    def test_ordering_desc(self):
        self.assertEqual("_Ordering('value', True)", repr(Ordering.DESC("value")))


if __name__ == "__main__":
    unittest.main()

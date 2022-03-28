import unittest

from minos.common import (
    InjectableMixin,
)


class _InjectableMixin(InjectableMixin):
    """For testing purposes."""


class TestInjectableMixin(unittest.TestCase):
    def test_set_get_injectable_name(self):
        with self.assertRaises(AttributeError):
            _InjectableMixin.get_injectable_name()
        _InjectableMixin._set_injectable_name("foo")
        self.assertEqual("foo", _InjectableMixin.get_injectable_name())


if __name__ == "__main__":
    unittest.main()

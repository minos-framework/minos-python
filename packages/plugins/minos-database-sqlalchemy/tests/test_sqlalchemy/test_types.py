import unittest

from sqlalchemy import (
    LargeBinary,
)

from minos.plugins.sqlalchemy import (
    EncodedType,
)


class TestEncodedType(unittest.TestCase):
    def test_cache_ok(self) -> None:
        self.assertEqual(True, EncodedType.cache_ok)

    def test_impl(self) -> None:
        self.assertEqual(LargeBinary, EncodedType.impl)

    def test_process_bind_param(self):
        type_ = EncodedType()
        # noinspection PyTypeChecker
        observed = type_.process_bind_param(56, None)

        self.assertIsInstance(observed, bytes)

    def test_process_result_value(self):
        type_ = EncodedType()

        # noinspection PyTypeChecker
        result = type_.process_bind_param(56, None)
        # noinspection PyTypeChecker
        observed = type_.process_result_value(result, None)

        self.assertEqual(56, observed)


if __name__ == "__main__":
    unittest.main()

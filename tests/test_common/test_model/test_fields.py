import unittest
from typing import (
    Optional,
)
from unittest.mock import (
    call,
    patch,
)

from minos.common import (
    Field,
    MinosAttributeValidationException,
)


class TestField(unittest.IsolatedAsyncioTestCase):
    def test_name(self):
        field = Field("test", int, 3)
        self.assertEqual("test", field.name)

    def test_type(self):
        field = Field("test", int, 3)
        self.assertEqual(int, field.type)

    def test_value_setter(self):
        field = Field("test", int, 3)
        with patch("minos.common.AvroDataDecoder.build", return_value=56) as mock_build:
            field.value = 3

            self.assertEqual(1, mock_build.call_count)
            self.assertEqual(call(3), mock_build.call_args)

        self.assertEqual(56, field.value)

    def test_value(self):
        with patch("minos.common.AvroDataDecoder.build", return_value=56) as mock:
            field = Field("test", int, 3)
            self.assertEqual(1, mock.call_count)
            self.assertEqual(call(3), mock.call_args)

        self.assertEqual(56, field.value)

    def test_value_setter_update(self):
        field = Field("test", Optional[int], 3)
        self.assertEqual(3, field.value)
        field.value = None
        self.assertEqual(None, field.value)
        field.value = 4
        self.assertEqual(4, field.value)

    def test_avro_schema(self):
        field = Field("test", int, 1)
        self.assertEqual({"name": "test", "type": "int"}, field.avro_schema)

    def test_avro_data(self):
        field = Field("test", int, 1)
        self.assertEqual(1, field.avro_data)

    def test_optional_type(self):
        field = Field("test", Optional[int])
        self.assertEqual(Optional[int], field.type)

    def test_empty_field_equality(self):
        self.assertEqual(Field("test", Optional[int], None), Field("test", Optional[int]))

    def test_parser(self):
        field = Field("test", str, "foo", str.title)
        self.assertEqual(str.title, field.parser)

    def test_parser_non_set(self):
        field = Field("test", str, "foo")
        self.assertEqual(None, field.parser)

    def test_parser_value(self):
        field = Field("test", str, "foo", lambda x: x.title())
        self.assertEqual("Foo", field.value)

    def test_parser_with_casting(self):
        field = Field("test", float, "1.234,56", lambda x: x.replace(".", "").replace(",", "."))
        self.assertEqual(1234.56, field.value)

    def test_parser_optional(self):
        field = Field("test", Optional[str], None, lambda x: x if x is None else x.title())
        self.assertEqual(None, field.value)

    def test_validator(self):
        field = Field("test", str, "foo", validator=len)
        self.assertEqual(len, field.validator)

    def test_validator_non_set(self):
        field = Field("test", str, "foo")
        self.assertEqual(None, field.validator)

    def test_validator_ok(self):
        field = Field("test", str, "foo", validator=lambda x: not x.count(" "))
        self.assertEqual("foo", field.value)

    def test_validator_raises(self):
        with self.assertRaises(MinosAttributeValidationException):
            Field("test", str, "foo bar", validator=lambda x: not x.count(" "))

    def test_validator_optional(self):
        field = Field("test", Optional[str], validator=lambda x: not x.count(" "))
        self.assertEqual(None, field.value)

    def test_equal(self):
        self.assertEqual(Field("id", Optional[int], 3), Field("id", Optional[int], 3))
        self.assertNotEqual(Field("id", Optional[int], 3), Field("id", Optional[int], None))
        self.assertNotEqual(Field("id", Optional[int], 3), Field("foo", Optional[int], 3))
        self.assertNotEqual(Field("id", Optional[int], 3), Field("id", int, 3))

    def test_iter(self):
        self.assertEqual(("id", Optional[int], 3, None, None), tuple(Field("id", Optional[int], 3)))

    def test_hash(self):
        self.assertEqual(hash(("id", Optional[int], 3, None, None)), hash(Field("id", Optional[int], 3)))

    def test_repr(self):
        field = Field("test", Optional[int], 1, parser=lambda x: x * 10, validator=lambda x: x > 0)
        self.assertEqual("test=10", repr(field))

    def test_str(self):
        field = Field("test", Optional[int], 1, parser=lambda x: x * 10, validator=lambda x: x > 0)
        self.assertEqual(repr(field), str(field))

    def test_from_avro(self):
        with patch("minos.common.AvroSchemaDecoder.build", return_value=int) as mock:
            observed = Field.from_avro({"name": "id", "type": "int"}, 1234)
            expected = Field("id", int, 1234)
            self.assertEqual(expected, observed)

            self.assertEqual(1, mock.call_count)
            self.assertEqual(call(), mock.call_args)


if __name__ == "__main__":
    unittest.main()

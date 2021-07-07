"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from datetime import (
    date,
    datetime,
    time,
)
from typing import (
    Optional,
    Union,
)
from unittest.mock import (
    call,
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    AvroDataDecoder,
    AvroDataEncoder,
    AvroSchemaEncoder,
    Field,
    MinosAttributeValidationException,
    MinosMalformedAttributeException,
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
        with patch(
            "minos.common.AvroDataDecoder.from_field", side_effect=AvroDataDecoder.from_field
        ) as mock_from_field:
            with patch("minos.common.AvroDataDecoder.build", return_value=56) as mock_build:
                field.value = 3

                self.assertEqual(1, mock_build.call_count)
                self.assertEqual(call(3), mock_build.call_args)

            self.assertEqual(1, mock_from_field.call_count)
            self.assertEqual(call(field), mock_from_field.call_args)

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

        with patch(
            "minos.common.AvroSchemaEncoder.from_field", side_effect=AvroSchemaEncoder.from_field
        ) as mock_from_field:
            with patch("minos.common.AvroSchemaEncoder.build", return_value=56) as mock_build:
                self.assertEqual(56, field.avro_schema)

                self.assertEqual(1, mock_build.call_count)
                self.assertEqual(call(), mock_build.call_args)

            self.assertEqual(1, mock_from_field.call_count)
            self.assertEqual(call(field), mock_from_field.call_args)

    def test_avro_data(self):
        field = Field("test", int, 1)

        with patch(
            "minos.common.AvroDataEncoder.from_field", side_effect=AvroDataEncoder.from_field
        ) as mock_from_field:
            with patch("minos.common.AvroDataEncoder.build", return_value=56) as mock_build:
                self.assertEqual(56, field.avro_data)

                self.assertEqual(1, mock_build.call_count)
                self.assertEqual(call(), mock_build.call_args)

            self.assertEqual(1, mock_from_field.call_count)
            self.assertEqual(call(field), mock_from_field.call_args)

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

    def test_from_avro_int(self):
        obtained = Field.from_avro({"name": "id", "type": "int"}, 1234)
        desired = Field("id", int, 1234)
        self.assertEqual(desired, obtained)

    def test_from_avro_bool(self):
        obtained = Field.from_avro({"name": "id", "type": "boolean"}, True)
        desired = Field("id", bool, True)
        self.assertEqual(desired, obtained)

    def test_from_avro_float(self):
        obtained = Field.from_avro({"name": "id", "type": "float"}, 3.4)
        desired = Field("id", float, 3.4)
        self.assertEqual(desired, obtained)

    def test_from_avro_bytes(self):
        obtained = Field.from_avro({"name": "id", "type": "bytes"}, b"Test")
        desired = Field("id", bytes, b"Test")
        self.assertEqual(desired, obtained)

    def test_from_avro_date(self):
        value = date(2021, 1, 21)
        obtained = Field.from_avro({"name": "id", "type": "int", "logicalType": "date"}, value)
        desired = Field("id", date, value)
        self.assertEqual(desired, obtained)

    def test_from_avro_time(self):
        value = time(20, 45, 21)
        obtained = Field.from_avro({"name": "id", "type": "int", "logicalType": "time-micros"}, value)
        desired = Field("id", time, value)
        self.assertEqual(desired, obtained)

    def test_from_avro_datetime(self):
        value = datetime(2021, 3, 12, 21, 32, 21)
        obtained = Field.from_avro({"name": "id", "type": "long", "logicalType": "timestamp-micros"}, value)
        desired = Field("id", datetime, value)
        self.assertEqual(desired, obtained)

    def test_from_avro_uuid(self):
        uuid = uuid4()
        obtained = Field.from_avro({"name": "id", "type": "string", "logicalType": "uuid"}, uuid)
        desired = Field("id", UUID, uuid)
        self.assertEqual(desired, obtained)

    def test_from_avro_plain_array(self):
        obtained = Field.from_avro({"name": "example", "type": "array", "items": "string"}, ["a", "b", "c"])
        desired = Field("example", list[str], ["a", "b", "c"])
        self.assertEqual(desired, obtained)

    def test_from_avro_plain_map(self):
        obtained = Field.from_avro({"name": "example", "type": "map", "values": "int"}, {"a": 1, "b": 2})
        desired = Field("example", dict[str, int], {"a": 1, "b": 2})
        self.assertEqual(desired, obtained)

    def test_from_avro_nested_arrays(self):
        obtained = Field.from_avro(
            {"name": "example", "type": "array", "items": {"type": {"type": "array", "items": "string"}}},
            [["a", "b", "c"]],
        )
        desired = Field("example", list[list[str]], [["a", "b", "c"]])
        self.assertEqual(desired, obtained)

    def test_from_avro_none(self):
        obtained = Field.from_avro({"name": "example", "type": "null"}, None)
        desired = Field("example", type(None), None)
        self.assertEqual(desired, obtained)

    def test_from_avro_union(self):
        obtained = Field.from_avro({"name": "example", "type": "array", "items": ["int", "string"]}, [1, "a"])
        desired = Field("example", list[Union[int, str]], [1, "a"])
        self.assertEqual(desired, obtained)

    def test_from_avro_raises(self):
        with self.assertRaises(MinosMalformedAttributeException):
            Field.from_avro({"name": "id", "type": "foo"}, None)
        with self.assertRaises(MinosMalformedAttributeException):
            Field.from_avro({"name": "id", "type": "string", "logicalType": "foo"}, None)


if __name__ == "__main__":
    unittest.main()

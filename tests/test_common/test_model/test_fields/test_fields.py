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
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    InMemorySnapshot,
    MinosAttributeValidationException,
    MinosMalformedAttributeException,
    MinosReqAttributeException,
    MinosTypeAttributeException,
    ModelField,
    ModelRef,
)
from tests.aggregate_classes import (
    Owner,
)
from tests.model_classes import (
    User,
)
from tests.utils import (
    FakeBroker,
    FakeRepository,
)


class TestModelField(unittest.IsolatedAsyncioTestCase):
    def test_name(self):
        field = ModelField("test", int, 3)
        self.assertEqual("test", field.name)

    def test_type(self):
        field = ModelField("test", int, 3)
        self.assertEqual(int, field.type)

    def test_value_int(self):
        field = ModelField("test", int, 3)
        self.assertEqual(3, field.value)

    def test_value_float(self):
        field = ModelField("test", float, 3.14)
        self.assertEqual(3.14, field.value)

    def test_value_bytes(self):
        field = ModelField("test", bytes, bytes("foo", "utf-8"))
        self.assertEqual(bytes("foo", "utf-8"), field.value)

    def test_value_date(self):
        value = date(2021, 1, 21)
        field = ModelField("test", date, value)
        self.assertEqual(value, field.value)

    def test_value_date_int(self):
        field = ModelField("test", date, 18648)
        self.assertEqual(date(2021, 1, 21), field.value)

    def test_value_date_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            ModelField("test", date, "2342342")

    def test_value_time(self):
        value = time(20, 45, 21)
        field = ModelField("test", time, value)
        self.assertEqual(value, field.value)

    def test_value_time_int(self):
        field = ModelField("test", time, 74721000)
        self.assertEqual(time(20, 45, 21), field.value)

    def test_value_time_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            ModelField("test", time, "2342342")

    def test_value_datetime(self):
        value = datetime.now()
        field = ModelField("test", datetime, value)
        self.assertEqual(value, field.value)

    def test_value_datetime_int(self):
        field = ModelField("test", datetime, 1615584741000)
        self.assertEqual(datetime(2021, 3, 12, 21, 32, 21), field.value)

    def test_value_datetime_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            ModelField("test", datetime, "2342342")

    def test_value_float_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            ModelField("test", float, [3])
        with self.assertRaises(MinosTypeAttributeException):
            ModelField("test", float, "foo")

    def test_value_bytes_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            ModelField("test", bytes, 3)

    def test_value_list_int(self):
        field = ModelField("test", list[int], [1, 2, 3])
        self.assertEqual([1, 2, 3], field.value)

    def test_value_list_str(self):
        field = ModelField("test", list[str], ["foo", "bar", "foobar"])
        self.assertEqual(["foo", "bar", "foobar"], field.value)

    def test_value_list_model(self):
        field = ModelField("test", list[User], [User(123), User(456)])
        self.assertEqual([User(123), User(456)], field.value)

    async def test_value_list_model_ref(self):
        async with FakeBroker() as broker, FakeRepository() as repository, InMemorySnapshot() as snapshot:
            field = ModelField(
                "test",
                list[ModelRef[Owner]],
                [1, 2, Owner("Foo", "Bar", 56, _broker=broker, _repository=repository, _snapshot=snapshot)],
            )
            self.assertEqual(
                [1, 2, Owner("Foo", "Bar", 56, _broker=broker, _repository=repository, _snapshot=snapshot)],
                field.value,
            )

    def test_value_uuid(self):
        value = uuid4()
        field = ModelField("test", UUID, value)
        self.assertEqual(value, field.value)

    def test_value_uuid_str(self):
        value = uuid4()
        field = ModelField("test", UUID, str(value))
        self.assertEqual(value, field.value)

    def test_value_uuid_bytes(self):
        value = uuid4()
        field = ModelField("test", UUID, value.bytes)
        self.assertEqual(value, field.value)

    def test_value_uuid_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            ModelField("test", UUID, "foo")
        with self.assertRaises(MinosTypeAttributeException):
            ModelField("test", UUID, bytes())

    def test_avro_schema_int(self):
        field = ModelField("test", int, 1)
        expected = {"name": "test", "type": "int"}
        self.assertEqual(expected, field.avro_schema)

    def test_avro_schema_bool(self):
        field = ModelField("test", bool, True)
        expected = {"name": "test", "type": "boolean"}
        self.assertEqual(expected, field.avro_schema)

    def test_avro_schema_float(self):
        field = ModelField("test", float, 3.4)
        expected = {"name": "test", "type": "float"}
        self.assertEqual(expected, field.avro_schema)

    def test_avro_schema_string(self):
        field = ModelField("test", str, "foo")
        expected = {"name": "test", "type": "string"}
        self.assertEqual(expected, field.avro_schema)

    def test_avro_schema_bytes(self):
        field = ModelField("test", bytes, bytes("foo", "utf-8"))
        expected = {"name": "test", "type": "bytes"}
        self.assertEqual(expected, field.avro_schema)

    def test_avro_schema_date(self):
        field = ModelField("test", date, date(2021, 1, 21))
        expected = {"name": "test", "type": {"type": "int", "logicalType": "date"}}
        self.assertEqual(expected, field.avro_schema)

    def test_avro_schema_time(self):
        field = ModelField("test", time, time(20, 32, 12))
        expected = {"name": "test", "type": {"type": "int", "logicalType": "time-millis"}}
        self.assertEqual(expected, field.avro_schema)

    def test_avro_schema_datetime(self):
        field = ModelField("test", datetime, datetime.now())
        expected = {"name": "test", "type": {"type": "long", "logicalType": "timestamp-millis"}}
        self.assertEqual(expected, field.avro_schema)

    def test_avro_schema_dict(self):
        field = ModelField("test", dict[str, int], {"foo": 1, "bar": 2})
        expected = {"name": "test", "type": {"type": "map", "values": "int"}}
        self.assertEqual(expected, field.avro_schema)

    def test_avro_schema_model_ref(self):
        field = ModelField("test", Optional[ModelRef[Owner]], 1)
        expected = {
            "name": "test",
            "type": [
                {
                    "fields": [
                        {"name": "id", "type": "int"},
                        {"name": "version", "type": "int"},
                        {"name": "name", "type": "string"},
                        {"name": "surname", "type": "string"},
                        {"name": "age", "type": ["int", "null"]},
                    ],
                    "name": "Owner",
                    "namespace": "tests.aggregate_classes.test",
                    "type": "record",
                },
                "int",
                "null",
            ],
        }
        self.assertEqual(expected, field.avro_schema)

    def test_avro_schema_list_model(self):
        field = ModelField("test", list[Optional[User]], [User(123), User(456)])
        expected = {
            "name": "test",
            "type": {
                "items": [
                    {
                        "fields": [{"name": "id", "type": "int"}, {"name": "username", "type": ["string", "null"]}],
                        "name": "User",
                        "namespace": "tests.model_classes.test",
                        "type": "record",
                    },
                    "null",
                ],
                "type": "array",
            },
        }
        self.assertEqual(expected, field.avro_schema)

    def test_avro_schema_uuid(self):
        field = ModelField("test", UUID, uuid4())
        self.assertEqual({"name": "test", "type": {"type": "string", "logicalType": "uuid"}}, field.avro_schema)

    def test_avro_data_list_model(self):
        field = ModelField("test", list[Optional[User]], [User(123), User(456)])
        expected = [{"id": 123, "username": None}, {"id": 456, "username": None}]
        self.assertEqual(expected, field.avro_data)

    def test_avro_data_dict(self):
        field = ModelField("test", dict[str, int], {"foo": 1, "bar": 2})
        self.assertEqual({"bar": 2, "foo": 1}, field.avro_data)

    def test_avro_data_bytes(self):
        field = ModelField("test", bytes, bytes("foo", "utf-8"))
        self.assertEqual(b"foo", field.avro_data)

    def test_avro_data_date(self):
        field = ModelField("test", date, date(2021, 1, 21))
        self.assertEqual(18648, field.avro_data)

    def test_avro_data_time(self):
        field = ModelField("test", time, time(20, 45, 21))
        self.assertEqual(74721000, field.avro_data)

    def test_avro_data_datetime(self):
        value = datetime(2021, 3, 12, 21, 32, 21)
        field = ModelField("test", datetime, value)
        self.assertEqual(1615584741000, field.avro_data)

    def test_avro_data_uuid(self):
        value = uuid4()
        field = ModelField("test", UUID, value)
        self.assertEqual(str(value), field.avro_data)

    def test_value_list_optional(self):
        field = ModelField("test", list[Optional[int]], [1, None, 3, 4])
        self.assertEqual([1, None, 3, 4], field.value)

    def test_value_list_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            ModelField("test", list[int], 3)

    def test_value_dict(self):
        field = ModelField("test", dict[str, bool], {"foo": True, "bar": False})
        self.assertEqual({"foo": True, "bar": False}, field.value)

    def test_value_dict_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            ModelField("test", dict[str, int], 3)

    def test_dict_keys_raises(self):
        with self.assertRaises(MinosMalformedAttributeException):
            ModelField("test", dict[int, int], {1: 2, 3: 4})

    def test_value_model(self):
        user = User(1234)
        field = ModelField("test", User, user)
        self.assertEqual(user, field.value)

    async def test_value_model_ref_value(self):
        async with FakeBroker() as broker, FakeRepository() as repository, InMemorySnapshot() as snapshot:
            user = Owner("Foo", "Bar", _broker=broker, _repository=repository, _snapshot=snapshot)
            field = ModelField("test", ModelRef[Owner], user)
            self.assertEqual(user, field.value)

    def test_value_model_ref_reference(self):
        field = ModelField("test", ModelRef[Owner], 1234)
        self.assertEqual(1234, field.value)

    def test_value_model_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            ModelField("test", User, "foo")

    def test_value_model_ref_raises(self):
        with self.assertRaises(MinosMalformedAttributeException):
            ModelField("test", ModelRef[User], User(1234))

    def test_value_model_optional(self):
        field = ModelField("test", Optional[User], None)
        self.assertIsNone(field.value)

        user = User(1234)
        field.value = user
        self.assertEqual(user, field.value)

    def test_value_unsupported(self):
        with self.assertRaises(MinosTypeAttributeException):
            ModelField("test", set[int], {3})

    def test_value_setter(self):
        field = ModelField("test", int, 3)
        field.value = 3
        self.assertEqual(3, field.value)

    def test_value_setter_raises(self):
        field = ModelField("test", int, 3)
        with self.assertRaises(MinosReqAttributeException):
            field.value = None

    def test_value_setter_list_raises_required(self):
        with self.assertRaises(MinosReqAttributeException):
            ModelField("test", list[int])

    def test_value_setter_union_raises_required(self):
        with self.assertRaises(MinosReqAttributeException):
            ModelField("test", Union[int, str])

    def test_value_setter_dict(self):
        field = ModelField("test", dict[str, bool], {})
        field.value = {"foo": True, "bar": False}
        self.assertEqual({"foo": True, "bar": False}, field.value)

    def test_value_setter_dict_raises(self):
        field = ModelField("test", dict[str, bool], {})
        with self.assertRaises(MinosTypeAttributeException):
            field.value = "foo"
        with self.assertRaises(MinosTypeAttributeException):
            field.value = {"foo": 1, "bar": 2}
        with self.assertRaises(MinosTypeAttributeException):
            field.value = {1: True, 2: False}

    def test_empty_value_raises(self):
        with self.assertRaises(MinosReqAttributeException):
            ModelField("id", int)

    def test_union_type(self):
        with self.assertRaises(MinosReqAttributeException):
            ModelField("test", Union[int, list[int]], None)

    def test_optional_type(self):
        field = ModelField("test", Optional[int])
        self.assertEqual(Optional[int], field.type)

    def test_empty_optional_value(self):
        field = ModelField("test", Optional[int])
        self.assertEqual(None, field.value)

    def test_empty_field_equality(self):
        self.assertEqual(ModelField("test", Optional[int], None), ModelField("test", Optional[int]))

    def test_value_setter_optional_int(self):
        field = ModelField("test", Optional[int], 3)
        self.assertEqual(3, field.value)
        field.value = None
        self.assertEqual(None, field.value)
        field.value = 4
        self.assertEqual(4, field.value)

    def test_value_setter_optional_list(self):
        field = ModelField("test", Optional[list[int]], [1, 2, 3])
        self.assertEqual([1, 2, 3], field.value)
        field.value = None
        self.assertEqual(None, field.value)
        field.value = [4]
        self.assertEqual([4], field.value)

    def test_parser(self):
        field = ModelField("test", str, "foo", str.title)
        self.assertEqual(str.title, field.parser)

    def test_parser_non_set(self):
        field = ModelField("test", str, "foo")
        self.assertEqual(None, field.parser)

    def test_parser_value(self):
        field = ModelField("test", str, "foo", lambda x: x.title())
        self.assertEqual("Foo", field.value)

    def test_parser_with_casting(self):
        field = ModelField("test", float, "1.234,56", lambda x: x.replace(".", "").replace(",", "."))
        self.assertEqual(1234.56, field.value)

    def test_parser_optional(self):
        field = ModelField("test", Optional[str], None, lambda x: x if x is None else x.title())
        self.assertEqual(None, field.value)

    def test_validator(self):
        field = ModelField("test", str, "foo", validator=len)
        self.assertEqual(len, field.validator)

    def test_validator_non_set(self):
        field = ModelField("test", str, "foo")
        self.assertEqual(None, field.validator)

    def test_validator_ok(self):
        field = ModelField("test", str, "foo", validator=lambda x: not x.count(" "))
        self.assertEqual("foo", field.value)

    def test_validator_raises(self):
        with self.assertRaises(MinosAttributeValidationException):
            ModelField("test", str, "foo bar", validator=lambda x: not x.count(" "))

    def test_validator_optional(self):
        field = ModelField("test", Optional[str], validator=lambda x: not x.count(" "))
        self.assertEqual(None, field.value)

    def test_equal(self):
        self.assertEqual(ModelField("id", Optional[int], 3), ModelField("id", Optional[int], 3))
        self.assertNotEqual(ModelField("id", Optional[int], 3), ModelField("id", Optional[int], None))
        self.assertNotEqual(ModelField("id", Optional[int], 3), ModelField("foo", Optional[int], 3))
        self.assertNotEqual(ModelField("id", Optional[int], 3), ModelField("id", int, 3))

    def test_iter(self):
        self.assertEqual(("id", Optional[int], 3, None, None), tuple(ModelField("id", Optional[int], 3)))

    def test_hash(self):
        self.assertEqual(hash(("id", Optional[int], 3, None, None)), hash(ModelField("id", Optional[int], 3)))

    def test_repr(self):
        field = ModelField("test", Optional[int], 1, parser=lambda x: x * 10, validator=lambda x: x > 0)
        expected = "ModelField(name='test', type=typing.Optional[int], value=10, parser=<lambda>, validator=<lambda>)"
        self.assertEqual(expected, repr(field))

    def test_repr_empty_parser(self):
        field = ModelField("test", Optional[int], 1)
        self.assertEqual(
            "ModelField(name='test', type=typing.Optional[int], value=1, parser=None, validator=None)", repr(field),
        )

    def test_str(self):
        field = ModelField("test", Optional[int], 1, parser=lambda x: x * 10, validator=lambda x: x > 0)
        expected = "test=10"
        self.assertEqual(expected, str(field))

    def test_from_avro_int(self):
        obtained = ModelField.from_avro({"name": "id", "type": "int"}, 1234)
        desired = ModelField("id", int, 1234)
        self.assertEqual(desired, obtained)

    def test_from_avro_bool(self):
        obtained = ModelField.from_avro({"name": "id", "type": "boolean"}, True)
        desired = ModelField("id", bool, True)
        self.assertEqual(desired, obtained)

    def test_from_avro_float(self):
        obtained = ModelField.from_avro({"name": "id", "type": "float"}, 3.4)
        desired = ModelField("id", float, 3.4)
        self.assertEqual(desired, obtained)

    def test_from_avro_bytes(self):
        obtained = ModelField.from_avro({"name": "id", "type": "bytes"}, b"Test")
        desired = ModelField("id", bytes, b"Test")
        self.assertEqual(desired, obtained)

    def test_from_avro_date(self):
        value = date(2021, 1, 21)
        obtained = ModelField.from_avro({"name": "id", "type": "int", "logicalType": "date"}, value)
        desired = ModelField("id", date, value)
        self.assertEqual(desired, obtained)

    def test_from_avro_time(self):
        value = time(20, 45, 21)
        obtained = ModelField.from_avro({"name": "id", "type": "int", "logicalType": "time-millis"}, value)
        desired = ModelField("id", time, value)
        self.assertEqual(desired, obtained)

    def test_from_avro_datetime(self):
        value = datetime(2021, 3, 12, 21, 32, 21)
        obtained = ModelField.from_avro({"name": "id", "type": "long", "logicalType": "timestamp-millis"}, value)
        desired = ModelField("id", datetime, value)
        self.assertEqual(desired, obtained)

    def test_from_avro_uuid(self):
        uuid = uuid4()
        obtained = ModelField.from_avro({"name": "id", "type": "string", "logicalType": "uuid"}, uuid)
        desired = ModelField("id", UUID, uuid)
        self.assertEqual(desired, obtained)

    def test_from_avro_plain_array(self):
        obtained = ModelField.from_avro({"name": "example", "type": "array", "items": "string"}, ["a", "b", "c"])
        desired = ModelField("example", list[str], ["a", "b", "c"])
        self.assertEqual(desired, obtained)

    def test_from_avro_plain_map(self):
        obtained = ModelField.from_avro({"name": "example", "type": "map", "values": "int"}, {"a": 1, "b": 2})
        desired = ModelField("example", dict[str, int], {"a": 1, "b": 2})
        self.assertEqual(desired, obtained)

    def test_from_avro_nested_arrays(self):
        obtained = ModelField.from_avro(
            {"name": "example", "type": "array", "items": {"type": {"type": "array", "items": "string"}}},
            [["a", "b", "c"]],
        )
        desired = ModelField("example", list[list[str]], [["a", "b", "c"]])
        self.assertEqual(desired, obtained)

    def test_from_avro_none(self):
        obtained = ModelField.from_avro({"name": "example", "type": "null"}, None)
        desired = ModelField("example", type(None), None)
        self.assertEqual(desired, obtained)

    def test_from_avro_union(self):
        obtained = ModelField.from_avro({"name": "example", "type": "array", "items": ["int", "string"]}, [1, "a"])
        desired = ModelField("example", list[Union[int, str]], [1, "a"])
        self.assertEqual(desired, obtained)

    def test_from_avro_raises(self):
        with self.assertRaises(MinosMalformedAttributeException):
            ModelField.from_avro({"name": "id", "type": "foo"}, None)
        with self.assertRaises(MinosMalformedAttributeException):
            ModelField.from_avro({"name": "id", "type": "string", "logicalType": "foo"}, None)


if __name__ == "__main__":
    unittest.main()

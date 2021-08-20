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
    Any,
    Optional,
    Union,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    AvroDataDecoder,
    DataTransferObject,
    EntitySet,
    Field,
    InMemorySnapshot,
    MinosMalformedAttributeException,
    MinosReqAttributeException,
    MinosTypeAttributeException,
    MissingSentinel,
    ModelRef,
    ModelType,
    current_datetime,
)
from tests.aggregate_classes import (
    Owner,
)
from tests.model_classes import (
    Base,
    GenericUser,
    User,
)
from tests.subaggregate_classes import (
    CartItem,
)
from tests.utils import (
    FakeBroker,
    FakeEntity,
    FakeRepository,
)


class TestAvroDataDecoder(unittest.IsolatedAsyncioTestCase):
    def test_model_type(self):
        observed = AvroDataDecoder("test", ModelType.build("Foo", {"bar": str})).build({"bar": "foobar"})

        self.assertIsInstance(observed, DataTransferObject)
        self.assertEqual({"bar": "foobar"}, observed.avro_data)

    def test_model_type_with_inheritance(self):
        # noinspection PyPep8Naming
        Foo = ModelType.build("Foo", {"bar": Base})
        decoder = AvroDataDecoder("test", Foo)
        value = Foo(User(1234))
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_model_type_already_casted(self):
        value = DataTransferObject("Foo", {"bar": Field("bar", str, "foobar")})
        observed = AvroDataDecoder("test", ModelType.build("Foo", {"bar": str})).build(value)
        self.assertEqual(value, observed)

    def test_model_type_raises(self):
        with self.assertRaises(MinosTypeAttributeException):
            AvroDataDecoder("test", ModelType.build("Foo", {"bar": str})).build(3)

    def test_any(self):
        decoder = AvroDataDecoder("test", Any)
        self.assertEqual(3, decoder.build(3))

    def test_any_raises(self):
        decoder = AvroDataDecoder("test", Any)
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build({"one", "two"})

    def test_none(self):
        decoder = AvroDataDecoder("test", type(None))
        observed = decoder.build(None)
        self.assertEqual(None, observed)

    def test_none_raises(self):
        decoder = AvroDataDecoder("test", type(None))
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build("hello")

    def test_int(self):
        decoder = AvroDataDecoder("test", int)
        observed = decoder.build(3)
        self.assertEqual(3, observed)

    def test_int_raises(self):
        decoder = AvroDataDecoder("test", int)
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build("hello")

    def test_float(self):
        decoder = AvroDataDecoder("test", float)
        observed = decoder.build(3.14)
        self.assertEqual(3.14, observed)

    def test_bytes(self):
        decoder = AvroDataDecoder("test", bytes)
        observed = decoder.build(bytes("foo", "utf-8"))
        self.assertEqual(bytes("foo", "utf-8"), observed)

    def test_date(self):
        decoder = AvroDataDecoder("test", date)
        value = date(2021, 1, 21)
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_date_int(self):
        decoder = AvroDataDecoder("test", date)
        observed = decoder.build(18648)
        self.assertEqual(date(2021, 1, 21), observed)

    def test_date_raises(self):
        decoder = AvroDataDecoder("test", date)
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build("2342342")

    def test_time(self):
        decoder = AvroDataDecoder("test", time)
        value = time(20, 45, 21)
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_time_int(self):
        decoder = AvroDataDecoder("test", time)
        observed = decoder.build(74721000000)
        self.assertEqual(time(20, 45, 21), observed)

    def test_time_raises(self):
        decoder = AvroDataDecoder("test", time)
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build("2342342")

    def test_datetime(self):
        decoder = AvroDataDecoder("test", datetime)
        value = current_datetime()
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_datetime_int(self):
        decoder = AvroDataDecoder("test", datetime)
        observed = decoder.build(1615584741000000)
        self.assertEqual(datetime(2021, 3, 12, 21, 32, 21), observed)

    def test_datetime_raises(self):
        decoder = AvroDataDecoder("test", datetime)
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build("2342342")

    def test_float_raises(self):
        decoder = AvroDataDecoder("test", float)
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build([3])
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build("foo")

    def test_bytes_raises(self):
        decoder = AvroDataDecoder("test", bytes)
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build(3)

    def test_uuid(self):
        decoder = AvroDataDecoder("test", UUID)
        value = uuid4()
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_uuid_str(self):
        decoder = AvroDataDecoder("test", UUID)
        value = uuid4()
        observed = decoder.build(str(value))
        self.assertEqual(value, observed)

    def test_uuid_bytes(self):
        decoder = AvroDataDecoder("test", UUID)
        value = uuid4()
        observed = decoder.build(value.bytes)
        self.assertEqual(value, observed)

    def test_uuid_raises(self):
        decoder = AvroDataDecoder("test", UUID)
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build("foo")

        decoder = AvroDataDecoder("test", UUID)
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build(bytes())

    def test_list_int(self):
        decoder = AvroDataDecoder("test", list[int])
        observed = decoder.build([1, 2, 3])
        self.assertEqual([1, 2, 3], observed)

    def test_list_str(self):
        decoder = AvroDataDecoder("test", list[str])
        observed = decoder.build(["foo", "bar", "foobar"])
        self.assertEqual(["foo", "bar", "foobar"], observed)

    def test_list_model(self):
        decoder = AvroDataDecoder("test", list[User])
        value = [User(123), User(456)]
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    async def test_list_model_ref(self):
        decoder = AvroDataDecoder("test", list[ModelRef[Owner]])
        async with FakeBroker() as broker, FakeRepository() as repository, InMemorySnapshot() as snapshot:
            value = [uuid4(), Owner("Foo", "Bar", 56, _broker=broker, _repository=repository, _snapshot=snapshot)]
            observed = decoder.build(value)
            self.assertEqual(value, observed)

    async def test_list_model_subaggregate_ref(self):
        decoder = AvroDataDecoder("test", list[ModelRef[CartItem]])
        async with FakeBroker():
            value = [uuid4(), CartItem(uuid4(), 3, "Foo", 56)]
            observed = decoder.build(value)
            self.assertEqual(value, observed)

    def test_list_raises(self):
        decoder = AvroDataDecoder("test", list)
        with self.assertRaises(MinosMalformedAttributeException):
            decoder.build([1, 2, 3])

        decoder = AvroDataDecoder("test", list[int])
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build(3)
        with self.assertRaises(MinosReqAttributeException):
            decoder.build(None)

    def test_list_optional(self):
        decoder = AvroDataDecoder("test", list[Optional[int]])
        value = [1, None, 3, 4]
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_list_any(self):
        decoder = AvroDataDecoder("test", list[Any])
        self.assertEqual([1, "hola", True], decoder.build([1, "hola", True]))

    def test_dict(self):
        decoder = AvroDataDecoder("test", dict[str, bool])
        value = {"foo": True, "bar": False}
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_dict_raises(self):
        decoder = AvroDataDecoder("test", dict[str, bool])
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build("foo")
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build({"foo": 1, "bar": 2})
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build({1: True, 2: False})

    def test_dict_keys_raises(self):
        decoder = AvroDataDecoder("test", dict[int, int])
        with self.assertRaises(MinosMalformedAttributeException):
            decoder.build({1: 2, 3: 4})

    def test_model(self):
        decoder = AvroDataDecoder("test", User)
        value = User(1234)
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_model_with_inheritance(self):
        decoder = AvroDataDecoder("test", Base)
        value = User(1234)
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_model_from_dict(self):
        decoder = AvroDataDecoder("test", User)
        value = User(1234)
        observed = decoder.build({"id": 1234})
        self.assertEqual(value, observed)

    async def test_model_ref_value(self):
        decoder = AvroDataDecoder("test", ModelRef[Owner])
        async with FakeBroker() as broker, FakeRepository() as repository, InMemorySnapshot() as snapshot:
            value = Owner("Foo", "Bar", _broker=broker, _repository=repository, _snapshot=snapshot)
            observed = decoder.build(value)
            self.assertEqual(value, observed)

    def test_model_ref_reference(self):
        decoder = AvroDataDecoder("test", ModelRef[Owner])
        value = uuid4()
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_model_raises(self):
        decoder = AvroDataDecoder("test", User)
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build("foo")

    def test_model_ref_raises(self):
        decoder = AvroDataDecoder("test", ModelRef[User])
        with self.assertRaises(MinosMalformedAttributeException):
            decoder.build(User(1234))

    def test_model_optional(self):
        decoder = AvroDataDecoder("test", Optional[User])
        observed = decoder.build(None)
        self.assertIsNone(observed)

    def test_unsupported(self):
        decoder = AvroDataDecoder("test", set[int])
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build({3})

    def test_empty_raises(self):
        decoder = AvroDataDecoder("test", date)
        with self.assertRaises(MinosReqAttributeException):
            decoder.build(None)
        with self.assertRaises(MinosReqAttributeException):
            decoder.build(MissingSentinel)

    def test_union_raises(self):
        decoder = AvroDataDecoder("test", Union[int, list[int]])
        with self.assertRaises(MinosReqAttributeException):
            decoder.build(None)
        with self.assertRaises(MinosReqAttributeException):
            decoder.build(MissingSentinel)
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build("hello")

    def test_optional_type(self):
        decoder = AvroDataDecoder("test", Optional[int])
        observed = decoder.build(None)
        self.assertIsNone(observed)

    def test_model_generic(self):
        value = GenericUser("foo")
        decoder = AvroDataDecoder("test", GenericUser[str])
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_model_generic_fail(self):
        value = GenericUser("foo")
        decoder = AvroDataDecoder("test", GenericUser[int])
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build(value)

    def test_entity_set(self):
        raw = {FakeEntity("John"), FakeEntity("Michael")}
        entities = EntitySet(raw)
        decoder = AvroDataDecoder("test", EntitySet[FakeEntity])
        observed = decoder.build(entities)

        self.assertEqual(entities, observed)

    def test_entity_set_empty(self):
        entities = EntitySet()
        decoder = AvroDataDecoder("test", EntitySet[FakeEntity])
        observed = decoder.build(entities)

        self.assertEqual(entities, observed)

    def test_entity_set_raises(self):
        raw = {FakeEntity("John"), FakeEntity("Michael")}
        entities = EntitySet(raw)
        decoder = AvroDataDecoder("test", EntitySet[Base])
        with self.assertRaises(MinosTypeAttributeException):
            decoder.build(entities)


if __name__ == "__main__":
    unittest.main()

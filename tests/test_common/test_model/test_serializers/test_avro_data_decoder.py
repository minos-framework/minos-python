import unittest
from datetime import (
    date,
    datetime,
    time,
    timezone,
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
    DataDecoderMalformedTypeException,
    DataDecoderRequiredValueException,
    DataDecoderTypeException,
    DataTransferObject,
    EntitySet,
    Field,
    InMemorySnapshot,
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
        observed = AvroDataDecoder(ModelType.build("Foo", {"bar": str})).build({"bar": "foobar"})

        self.assertIsInstance(observed, DataTransferObject)
        self.assertEqual({"bar": "foobar"}, observed.avro_data)

    def test_model_type_with_inheritance(self):
        # noinspection PyPep8Naming
        Foo = ModelType.build("Foo", {"bar": Base})
        decoder = AvroDataDecoder(Foo)
        value = Foo(User(1234))
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_model_type_already_casted(self):
        value = DataTransferObject("Foo", {"bar": Field("bar", str, "foobar")})
        observed = AvroDataDecoder(ModelType.build("Foo", {"bar": str})).build(value)
        self.assertEqual(value, observed)

    def test_model_type_raises(self):
        with self.assertRaises(DataDecoderTypeException):
            AvroDataDecoder(ModelType.build("Foo", {"bar": str})).build(3)

    def test_any(self):
        decoder = AvroDataDecoder(Any)
        self.assertEqual(3, decoder.build(3))

    def test_any_raises(self):
        decoder = AvroDataDecoder(Any)
        with self.assertRaises(DataDecoderTypeException):
            decoder.build({"one", "two"})

    def test_none(self):
        decoder = AvroDataDecoder(type(None))
        observed = decoder.build(None)
        self.assertEqual(None, observed)

    def test_none_raises(self):
        decoder = AvroDataDecoder(type(None))
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("hello")

    def test_int(self):
        decoder = AvroDataDecoder(int)
        observed = decoder.build(3)
        self.assertEqual(3, observed)

    def test_int_raises(self):
        decoder = AvroDataDecoder(int)
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("hello")

    def test_float(self):
        decoder = AvroDataDecoder(float)
        observed = decoder.build(3.14)
        self.assertEqual(3.14, observed)

    def test_bytes(self):
        decoder = AvroDataDecoder(bytes)
        observed = decoder.build(bytes("foo", "utf-8"))
        self.assertEqual(bytes("foo", "utf-8"), observed)

    def test_date(self):
        decoder = AvroDataDecoder(date)
        value = date(2021, 1, 21)
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_date_int(self):
        decoder = AvroDataDecoder(date)
        observed = decoder.build(18648)
        self.assertEqual(date(2021, 1, 21), observed)

    def test_date_raises(self):
        decoder = AvroDataDecoder(date)
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("2342342")

    def test_time(self):
        decoder = AvroDataDecoder(time)
        value = time(20, 45, 21)
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_time_int(self):
        decoder = AvroDataDecoder(time)
        observed = decoder.build(74721000000)
        self.assertEqual(time(20, 45, 21), observed)

    def test_time_raises(self):
        decoder = AvroDataDecoder(time)
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("2342342")

    def test_datetime(self):
        decoder = AvroDataDecoder(datetime)
        value = current_datetime()
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_datetime_int(self):
        decoder = AvroDataDecoder(datetime)
        observed = decoder.build(1615584741000000)
        self.assertEqual(datetime(2021, 3, 12, 21, 32, 21, tzinfo=timezone.utc), observed)

    def test_datetime_raises(self):
        decoder = AvroDataDecoder(datetime)
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("2342342")

    def test_float_raises(self):
        decoder = AvroDataDecoder(float)
        with self.assertRaises(DataDecoderTypeException):
            decoder.build([3])
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("foo")

    def test_bytes_raises(self):
        decoder = AvroDataDecoder(bytes)
        with self.assertRaises(DataDecoderTypeException):
            decoder.build(3)

    def test_uuid(self):
        decoder = AvroDataDecoder(UUID)
        value = uuid4()
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_uuid_str(self):
        decoder = AvroDataDecoder(UUID)
        value = uuid4()
        observed = decoder.build(str(value))
        self.assertEqual(value, observed)

    def test_uuid_bytes(self):
        decoder = AvroDataDecoder(UUID)
        value = uuid4()
        observed = decoder.build(value.bytes)
        self.assertEqual(value, observed)

    def test_uuid_raises(self):
        decoder = AvroDataDecoder(UUID)
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("foo")

        decoder = AvroDataDecoder(UUID)
        with self.assertRaises(DataDecoderTypeException):
            decoder.build(bytes())

    def test_list_int(self):
        decoder = AvroDataDecoder(list[int])
        observed = decoder.build([1, 2, 3])
        self.assertEqual([1, 2, 3], observed)

    def test_list_str(self):
        decoder = AvroDataDecoder(list[str])
        observed = decoder.build(["foo", "bar", "foobar"])
        self.assertEqual(["foo", "bar", "foobar"], observed)

    def test_list_model(self):
        decoder = AvroDataDecoder(list[User])
        value = [User(123), User(456)]
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    async def test_list_model_ref(self):
        decoder = AvroDataDecoder(list[ModelRef[Owner]])
        async with FakeBroker() as broker, FakeRepository() as repository, InMemorySnapshot() as snapshot:
            value = [uuid4(), Owner("Foo", "Bar", 56, _broker=broker, _repository=repository, _snapshot=snapshot)]
            observed = decoder.build(value)
            self.assertEqual(value, observed)

    async def test_list_model_subaggregate_ref(self):
        decoder = AvroDataDecoder(list[ModelRef[CartItem]])
        async with FakeBroker():
            value = [uuid4(), CartItem(uuid4(), 3, "Foo", 56)]
            observed = decoder.build(value)
            self.assertEqual(value, observed)

    def test_list_raises(self):
        decoder = AvroDataDecoder(list)
        with self.assertRaises(DataDecoderMalformedTypeException):
            decoder.build([1, 2, 3])

        decoder = AvroDataDecoder(list[int])
        with self.assertRaises(DataDecoderTypeException):
            decoder.build(3)
        with self.assertRaises(DataDecoderRequiredValueException):
            decoder.build(None)

    def test_list_optional(self):
        decoder = AvroDataDecoder(list[Optional[int]])
        value = [1, None, 3, 4]
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_list_any(self):
        decoder = AvroDataDecoder(list[Any])
        self.assertEqual([1, "hola", True], decoder.build([1, "hola", True]))

    def test_dict(self):
        decoder = AvroDataDecoder(dict[str, bool])
        value = {"foo": True, "bar": False}
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_dict_raises(self):
        decoder = AvroDataDecoder(dict[str, bool])
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("foo")
        with self.assertRaises(DataDecoderTypeException):
            decoder.build({"foo": 1, "bar": 2})
        with self.assertRaises(DataDecoderTypeException):
            decoder.build({1: True, 2: False})

    def test_dict_keys_raises(self):
        decoder = AvroDataDecoder(dict[int, int])
        with self.assertRaises(DataDecoderMalformedTypeException):
            decoder.build({1: 2, 3: 4})

    def test_model(self):
        decoder = AvroDataDecoder(User)
        value = User(1234)
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_model_with_inheritance(self):
        decoder = AvroDataDecoder(Base)
        value = User(1234)
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_model_from_dict(self):
        decoder = AvroDataDecoder(User)
        value = User(1234)
        observed = decoder.build({"id": 1234})
        self.assertEqual(value, observed)

    async def test_model_ref_value(self):
        decoder = AvroDataDecoder(ModelRef[Owner])
        async with FakeBroker() as broker, FakeRepository() as repository, InMemorySnapshot() as snapshot:
            value = Owner("Foo", "Bar", _broker=broker, _repository=repository, _snapshot=snapshot)
            observed = decoder.build(value)
            self.assertEqual(value, observed)

    def test_model_ref_reference(self):
        decoder = AvroDataDecoder(ModelRef[Owner])
        value = uuid4()
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_model_raises(self):
        decoder = AvroDataDecoder(User)
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("foo")

    def test_model_ref_raises(self):
        decoder = AvroDataDecoder(ModelRef[User])
        with self.assertRaises(DataDecoderMalformedTypeException):
            decoder.build(User(1234))

    def test_model_optional(self):
        decoder = AvroDataDecoder(Optional[User])
        observed = decoder.build(None)
        self.assertIsNone(observed)

    def test_unsupported(self):
        decoder = AvroDataDecoder(set[int])
        with self.assertRaises(DataDecoderTypeException):
            decoder.build({3})

    def test_empty_raises(self):
        decoder = AvroDataDecoder(date)
        with self.assertRaises(DataDecoderRequiredValueException):
            decoder.build(None)
        with self.assertRaises(DataDecoderRequiredValueException):
            decoder.build(MissingSentinel)

    def test_union_raises(self):
        decoder = AvroDataDecoder(Union[int, list[int]])
        with self.assertRaises(DataDecoderRequiredValueException):
            decoder.build(None)
        with self.assertRaises(DataDecoderRequiredValueException):
            decoder.build(MissingSentinel)
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("hello")

    def test_optional_type(self):
        decoder = AvroDataDecoder(Optional[int])
        observed = decoder.build(None)
        self.assertIsNone(observed)

    def test_model_generic(self):
        value = GenericUser("foo")
        decoder = AvroDataDecoder(GenericUser[str])
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_model_generic_fail(self):
        value = GenericUser("foo")
        decoder = AvroDataDecoder(GenericUser[int])
        with self.assertRaises(DataDecoderTypeException):
            decoder.build(value)

    def test_entity_set(self):
        raw = {FakeEntity("John"), FakeEntity("Michael")}
        entities = EntitySet(raw)
        decoder = AvroDataDecoder(EntitySet[FakeEntity])
        observed = decoder.build(entities)

        self.assertEqual(entities, observed)

    def test_entity_set_empty(self):
        entities = EntitySet()
        decoder = AvroDataDecoder(EntitySet[FakeEntity])
        observed = decoder.build(entities)

        self.assertEqual(entities, observed)

    def test_entity_set_raises(self):
        raw = {FakeEntity("John"), FakeEntity("Michael")}
        entities = EntitySet(raw)
        decoder = AvroDataDecoder(EntitySet[Base])
        with self.assertRaises(DataDecoderTypeException):
            decoder.build(entities)


if __name__ == "__main__":
    unittest.main()

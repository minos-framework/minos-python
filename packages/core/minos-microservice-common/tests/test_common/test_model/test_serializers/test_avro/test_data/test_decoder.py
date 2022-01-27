import unittest
from datetime import (
    date,
    datetime,
    time,
    timedelta,
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
    DataDecoderException,
    DataDecoderMalformedTypeException,
    DataDecoderRequiredValueException,
    DataDecoderTypeException,
    DataTransferObject,
    Field,
    MissingSentinel,
    ModelType,
    current_datetime,
)
from tests.model_classes import (
    Analytics,
    Base,
    GenericUser,
    User,
)
from tests.utils import (
    MinosTestCase,
)


class TestAvroDataDecoder(MinosTestCase):
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
        with self.assertRaises(DataDecoderException):
            decoder.build(AvroDataDecoder)

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

    def test_timedelta(self):
        decoder = AvroDataDecoder(timedelta)
        value = timedelta(days=23, hours=12, seconds=1, microseconds=23)
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_timedelta_int(self):
        decoder = AvroDataDecoder(timedelta)
        observed = decoder.build(2030401000023)
        self.assertEqual(timedelta(days=23, hours=12, seconds=1, microseconds=23), observed)

    def test_timedelta_raises(self):
        decoder = AvroDataDecoder(timedelta)
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

    def test_list_empty(self):
        decoder = AvroDataDecoder(list[int])
        observed = decoder.build([])
        self.assertEqual([], observed)

    def test_list_raises(self):
        decoder = AvroDataDecoder(list)
        with self.assertRaises(DataDecoderMalformedTypeException):
            decoder.build([1, 2, 3])

        decoder = AvroDataDecoder(list[int])
        with self.assertRaises(DataDecoderTypeException):
            decoder.build(3)
        with self.assertRaises(DataDecoderRequiredValueException):
            decoder.build(None)
        decoder = AvroDataDecoder(list[str])
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("foo")

    def test_list_optional(self):
        decoder = AvroDataDecoder(list[Optional[int]])
        value = [1, None, 3, 4]
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_list_any(self):
        decoder = AvroDataDecoder(list[Any])
        self.assertEqual([1, "hola", True], decoder.build([1, "hola", True]))

    def test_set(self):
        decoder = AvroDataDecoder(set[int])
        self.assertEqual({1, 2, 3}, decoder.build([1, 2, 3]))

    def test_set_raises(self):
        decoder = AvroDataDecoder(set)
        with self.assertRaises(DataDecoderMalformedTypeException):
            decoder.build({1, 2, 3})

        decoder = AvroDataDecoder(set[int])
        with self.assertRaises(DataDecoderTypeException):
            decoder.build(3)
        with self.assertRaises(DataDecoderRequiredValueException):
            decoder.build(None)
        decoder = AvroDataDecoder(set[str])
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("foo")

    def test_dict(self):
        decoder = AvroDataDecoder(dict[str, bool])
        value = {"foo": True, "bar": False}
        observed = decoder.build(value)
        self.assertEqual(value, observed)

    def test_dict_empty(self):
        decoder = AvroDataDecoder(dict[str, bool])
        observed = decoder.build(dict())
        self.assertEqual(dict(), observed)

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

    def test_model_raises(self):
        decoder = AvroDataDecoder(User)
        with self.assertRaises(DataDecoderTypeException):
            decoder.build("foo")

    def test_model_optional(self):
        decoder = AvroDataDecoder(Optional[User])
        observed = decoder.build(None)
        self.assertIsNone(observed)

    def test_unsupported(self):
        # noinspection PyUnresolvedReferences
        decoder = AvroDataDecoder(type[Any])
        with self.assertRaises(DataDecoderTypeException):
            decoder.build(AvroDataDecoder)

    def test_empty_raises(self):
        decoder = AvroDataDecoder(date)
        with self.assertRaises(DataDecoderRequiredValueException):
            decoder.build(None)
        with self.assertRaises(DataDecoderRequiredValueException):
            decoder.build(MissingSentinel)

    def test_union(self):
        decoder = AvroDataDecoder(Union[list[str], str])
        self.assertEqual(["foo", "bar"], decoder.build(["foo", "bar"]))
        self.assertEqual("foo", decoder.build("foo"))

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

    def test_container_inheritance(self):
        # noinspection PyPep8Naming
        Container = ModelType.build("Container", {"data": list[Base]})
        raw = Container([User(1, "John"), Analytics(2, dict()), User(3, "John"), Analytics(4, dict())])
        decoder = AvroDataDecoder(Container)
        observed = decoder.build(raw)

        self.assertEqual(raw, observed)


if __name__ == "__main__":
    unittest.main()

import unittest
from datetime import (
    date,
    datetime,
    time,
    timezone,
)
from decimal import (
    Decimal,
)
from uuid import (
    uuid4,
)

from minos.common import (
    AvroDataEncoder,
    MinosMalformedAttributeException,
)
from tests.model_classes import (
    User,
)


class _Foo:
    """For testing purposes"""


class TestAvroDataEncoder(unittest.TestCase):
    def test_build_float(self):
        encoder = AvroDataEncoder(3.5)
        self.assertEqual(3.5, encoder.build())

    def test_build_decimal(self):
        encoder = AvroDataEncoder(Decimal("3.5"))
        self.assertEqual(3.5, encoder.build())

    def test_build_bytes(self):
        encoder = AvroDataEncoder(b"test")
        self.assertEqual(b"test", encoder.build())

    # noinspection SpellCheckingInspection
    def test_build_memoryview(self):
        encoder = AvroDataEncoder(memoryview(b"test"))
        self.assertEqual(b"test", encoder.build())

    def test_build_raises(self):
        encoder = AvroDataEncoder(_Foo())
        with self.assertRaises(MinosMalformedAttributeException):
            encoder.build()

    def test_avro_data_float(self):
        observed = AvroDataEncoder(3.14159265359).build()
        self.assertEqual(3.14159265359, observed)

    def test_avro_data_list_model(self):
        observed = AvroDataEncoder([User(123), User(456)]).build()
        expected = [{"id": 123, "username": None}, {"id": 456, "username": None}]
        self.assertEqual(expected, observed)

    def test_avro_data_dict(self):
        observed = AvroDataEncoder({"foo": 1, "bar": 2}).build()
        self.assertEqual({"bar": 2, "foo": 1}, observed)

    def test_avro_data_bytes(self):
        observed = AvroDataEncoder(bytes("foo", "utf-8")).build()
        self.assertEqual(b"foo", observed)

    def test_avro_data_date(self):
        observed = AvroDataEncoder(date(2021, 1, 21)).build()
        self.assertEqual(18648, observed)

    def test_avro_data_time(self):
        observed = AvroDataEncoder(time(20, 45, 21)).build()
        self.assertEqual(74721000000, observed)

    def test_avro_data_datetime(self):
        observed = AvroDataEncoder(datetime(2021, 3, 12, 21, 32, 21, tzinfo=timezone.utc)).build()
        self.assertEqual(1615584741000000, observed)

    def test_avro_data_uuid(self):
        value = uuid4()
        observed = AvroDataEncoder(value).build()
        self.assertEqual(str(value), observed)


if __name__ == "__main__":
    unittest.main()

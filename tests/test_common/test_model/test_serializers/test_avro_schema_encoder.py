import unittest
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from typing import (
    Any,
    Optional,
)
from unittest.mock import (
    MagicMock,
)
from uuid import (
    UUID,
)

from minos.common import (
    AvroSchemaEncoder,
    ModelType,
    classname,
)
from tests.model_classes import (
    Status,
    User,
)


class TestAvroSchemaEncoder(unittest.TestCase):
    def test_model_type(self):
        expected = {
            "fields": [{"name": "username", "type": "string"}],
            "name": "User",
            "namespace": "path.to.hello",
            "type": "record",
        }
        encoder = AvroSchemaEncoder(ModelType.build("User", {"username": str}, namespace_="path.to"))
        encoder.generate_random_str = MagicMock(return_value="hello")
        observed = encoder.build()
        self.assertEqual(expected, observed)

    def test_int(self):
        observed = AvroSchemaEncoder(int).build()
        expected = "int"
        self.assertEqual(expected, observed)

    def test_bool(self):
        expected = "boolean"
        observed = AvroSchemaEncoder(bool).build()
        self.assertEqual(expected, observed)

    def test_float(self):
        expected = "double"
        observed = AvroSchemaEncoder(float).build()
        self.assertEqual(expected, observed)

    def test_string(self):
        expected = "string"
        observed = AvroSchemaEncoder(str).build()
        self.assertEqual(expected, observed)

    def test_bytes(self):
        expected = "bytes"
        observed = AvroSchemaEncoder(bytes).build()
        self.assertEqual(expected, observed)

    def test_date(self):
        expected = {"type": "int", "logicalType": "date"}
        observed = AvroSchemaEncoder(date).build()
        self.assertEqual(expected, observed)

    def test_time(self):
        expected = {"type": "int", "logicalType": "time-micros"}
        observed = AvroSchemaEncoder(time).build()
        self.assertEqual(expected, observed)

    def test_datetime(self):
        expected = {"type": "long", "logicalType": "timestamp-micros"}
        observed = AvroSchemaEncoder(datetime).build()
        self.assertEqual(expected, observed)

    def test_timedelta(self):
        expected = {"type": "long", "logicalType": "timedelta-micros"}
        observed = AvroSchemaEncoder(timedelta).build()
        self.assertEqual(expected, observed)

    def test_set(self):
        expected = {"type": "array", "items": "string", "logicalType": "set"}
        observed = AvroSchemaEncoder(set[str]).build()
        self.assertEqual(expected, observed)

    def test_dict(self):
        expected = {"type": "map", "values": "int"}
        observed = AvroSchemaEncoder(dict[str, int]).build()
        self.assertEqual(expected, observed)

    def test_list_model(self):
        expected = {
            "items": [
                {
                    "fields": [{"name": "id", "type": "int"}, {"name": "username", "type": ["string", "null"]}],
                    "name": "User",
                    "namespace": "tests.model_classes.hello",
                    "type": "record",
                },
                "null",
            ],
            "type": "array",
        }
        encoder = AvroSchemaEncoder(list[Optional[User]])
        encoder.generate_random_str = MagicMock(return_value="hello")

        observed = encoder.build()
        self.assertEqual(expected, observed)

    def test_uuid(self):
        expected = {"type": "string", "logicalType": "uuid"}
        observed = AvroSchemaEncoder(UUID).build()
        self.assertEqual(expected, observed)

    def test_any(self):
        expected = "null"
        observed = AvroSchemaEncoder(Any).build()
        self.assertEqual(expected, observed)

    def test_enum(self):
        expected = {"type": "string", "logicalType": classname(Status)}
        observed = AvroSchemaEncoder(Status).build()
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

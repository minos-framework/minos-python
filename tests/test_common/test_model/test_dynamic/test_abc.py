import unittest

from minos.common import (
    DynamicModel,
    Field,
)
from tests.model_classes import (
    Foo,
)


class TestDynamicModel(unittest.TestCase):
    def test_from_avro_bytes(self):
        original = Foo("hello")
        model = DynamicModel.from_avro_bytes(original.avro_bytes)
        self.assertEqual("hello", model.text)

    def test_from_avro_bytes_multiple(self):
        encoded = Foo.to_avro_bytes([Foo("hello"), Foo("bye")])
        decoded = DynamicModel.from_avro_bytes(encoded)
        self.assertEqual("hello", decoded[0].text)
        self.assertEqual("bye", decoded[1].text)

    def test_from_avro(self):
        data = {"text": "test"}
        schema = {
            "fields": [{"name": "text", "type": "string"}],
            "name": "TestModel",
            "type": "record",
        }

        model = DynamicModel.from_avro(schema, data)
        self.assertEqual({"text": Field("text", str, "test")}, model.fields)

    def test_type_hints(self):
        data = {"text": "test"}
        schema = {
            "fields": [{"name": "text", "type": "string"}],
            "name": "TestModel",
            "type": "record",
        }

        model = DynamicModel.from_avro(schema, data)
        self.assertEqual({"text": str}, model.type_hints)

    def test_from_avro_list_schema(self):
        data = {"text": "test"}
        schema = [{"fields": [{"name": "text", "type": "string"}], "name": "TestModel", "type": "record"}]
        model = DynamicModel.from_avro(schema, data)
        self.assertEqual({"text": Field("text", str, "test")}, model.fields)


if __name__ == "__main__":
    unittest.main()

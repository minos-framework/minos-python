import dataclasses
import typing as t

import orjson

from minos.common.model.avro import schema, utils
from minos.common.model.avro.fields import FieldType
from minos.common.model.avro.schema import SchemaMetadata

AVRO = "avro"

class MinosAvroModel(object):

    @classmethod
    def generate_dataclass(cls: t.Any) -> t.Any:
        if dataclasses.is_dataclass(cls):
            return cls
        return dataclasses.dataclass(cls)

    @classmethod
    def generate_metadata(cls: t.Any) -> SchemaMetadata:
        meta = getattr(cls.klass, "Meta", None)

        if meta is None:
            return SchemaMetadata()
        return SchemaMetadata.create(meta)

    @classmethod
    def generate_schema(cls: t.Any, schema_type: str = "avro") -> t.Dict:
        if cls.schema_def is None:
            # Generate metaclass and metadata
            cls.klass = cls.generate_dataclass()
            cls.metadata = cls.generate_metadata()

            # let's live open the possibility to define different
            # schema definitions like json
            if schema_type == "avro":
                # cache the schema
                cls.schema_def = cls._generate_avro_schema()
            else:
                raise ValueError("Invalid type. Expected avro schema type.")

        return cls.schema_def

    @classmethod
    def _generate_avro_schema(cls: t.Any) -> schema.AvroSchemaDefinition:
        return schema.AvroSchemaDefinition("record", cls.klass, metadata=cls.metadata)

    @classmethod
    def avro_schema(cls: t.Any) -> str:
        return orjson.dumps(cls.generate_schema(schema_type=AVRO).render())

    @classmethod
    def avro_schema_to_python(cls: t.Any) -> t.Dict[str, t.Any]:
        return orjson.loads(cls.avro_schema())

    @classmethod
    def get_fields(cls: t.Any) -> t.List[FieldType]:
        if cls.schema_def is None:
            return cls.generate_schema().fields
        return cls.schema_def.fields

    @staticmethod
    def standardize_custom_type(value: t.Any) -> t.Any:
        if utils.is_custom_type(value):
            return value["default"]
        return value

    def asdict(self) -> t.Dict:
        data = dataclasses.asdict(self)

        # te standardize called can be replaced if we have a custom implementation of asdict
        # for now I think is better to use the native implementation
        return {key: self.standardize_custom_type(value) for key, value in data.items()}

    def serialize(self, serialization_type: str = AVRO) -> bytes:
        schema = self.avro_schema_to_python()

        return serialization.serialize(self.asdict(), schema, serialization_type=serialization_type)


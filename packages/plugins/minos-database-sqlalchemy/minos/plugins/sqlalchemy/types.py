from typing import (
    Any,
)

from sqlalchemy import (
    LargeBinary,
    TypeDecorator,
)
from sqlalchemy.engine import (
    Dialect,
)

from minos.common import (
    AvroDataDecoder,
    AvroDataEncoder,
    AvroSchemaDecoder,
    AvroSchemaEncoder,
    MinosAvroProtocol,
    TypeHintBuilder,
)


class EncodedType(TypeDecorator):
    """TODO"""

    impl = LargeBinary

    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Dialect) -> bytes:
        """TODO"""
        data = AvroDataEncoder().build(value)
        type_ = TypeHintBuilder(value).build()
        schema = AvroSchemaEncoder().build(type_)
        return MinosAvroProtocol.encode(data, schema)

    def process_result_value(self, value: bytes, dialect: Dialect) -> Any:
        """TODO"""
        schema = MinosAvroProtocol.decode_schema(value)
        type_ = AvroSchemaDecoder().build(schema)

        data = MinosAvroProtocol.decode(value)

        value = AvroDataDecoder().build(data, type_)

        return value

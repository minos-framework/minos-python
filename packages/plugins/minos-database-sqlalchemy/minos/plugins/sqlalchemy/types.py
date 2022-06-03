from typing import (
    Any,
)

from sqlalchemy import (
    LargeBinary,
    TypeDecorator,
)

from minos.common import (
    AvroDataDecoder,
    AvroDataEncoder,
    AvroSchemaDecoder,
    AvroSchemaEncoder,
    MinosAvroProtocol,
)


class EncodedType(TypeDecorator):
    """TODO"""

    impl = LargeBinary

    cache_ok = True

    def process_bind_param(self, value: Any, dialect) -> bytes:
        data = AvroDataEncoder().build(value)
        schema = AvroSchemaEncoder().build(value)
        return MinosAvroProtocol.encode(data, schema)

    def process_result_value(self, value: bytes, dialect) -> Any:
        schema = MinosAvroProtocol.decode_schema(value)
        type_ = AvroSchemaDecoder().build(schema)

        data = MinosAvroProtocol.decode(value)

        value = AvroDataDecoder().build(data, type_)

        return value

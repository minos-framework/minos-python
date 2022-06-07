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
    """Encoded Type class."""

    impl = LargeBinary

    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Dialect) -> bytes:
        """Process bind param.

        :param value: The value to be encoded.
        :param dialect: The database's dialect.
        :return: A bytes encoding of the value.
        """
        data = AvroDataEncoder().build(value)
        type_ = TypeHintBuilder(value).build()
        schema = AvroSchemaEncoder().build(type_)
        return MinosAvroProtocol.encode(data, schema)

    def process_result_value(self, value: bytes, dialect: Dialect) -> Any:
        """Process result value.

        :param value: The value in bytes to be decoded.
        :param dialect: The database's dialect.
        :return: The decoded representation of the value.
        """
        schema = MinosAvroProtocol.decode_schema(value)
        type_ = AvroSchemaDecoder().build(schema)

        data = MinosAvroProtocol.decode(value)

        value = AvroDataDecoder().build(data, type_)

        return value

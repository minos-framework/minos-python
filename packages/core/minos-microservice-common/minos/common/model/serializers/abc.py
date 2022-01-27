from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Union,
)

if TYPE_CHECKING:
    from ..abc import (
        Model,
    )
    from ..fields import (
        Field,
    )


class SchemaEncoder(ABC):
    """Schema Encoder base class."""

    @abstractmethod
    def build(self, type_: Union[type, Model, Field], **kwargs) -> Any:
        """Build the avro schema for the given field.

        :param type_: The type to be encoded as a schema.
        :return: A dictionary object.
        """


class SchemaDecoder(ABC):
    """Schema Decoder base class."""

    @abstractmethod
    def build(self, schema: Any, **kwargs) -> type:
        """Build type from given avro schema item.

        :param schema: The schema to be decoded as a type.
        :return: A type object.
        """


class DataEncoder(ABC):
    """Data Encoder base class."""

    @abstractmethod
    def build(self, value: Any, **kwargs) -> Any:
        """Build an avro data representation based on the content of the given field.

        :param value: The value to be encoded.
        :return: A `avro`-compatible data.
        """


class DataDecoder(ABC):
    """Data Decoder base class."""

    @abstractmethod
    def build(self, data: Any, type_: Any, **kwargs) -> Any:
        """Cast data type according to the field definition.

        :param data: Data to be casted.
        :param type_: The type of the decoded data.
        :return: The casted object.
        """

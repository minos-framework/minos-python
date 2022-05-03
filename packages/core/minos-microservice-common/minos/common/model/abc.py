from __future__ import (
    annotations,
)

import logging
from abc import (
    abstractmethod,
)
from base64 import (
    b64decode,
    b64encode,
)
from collections.abc import (
    Mapping,
)
from typing import (
    Any,
    Iterable,
    Iterator,
    Type,
    TypedDict,
    TypeVar,
    Union,
)

from ..exceptions import (
    EmptyMinosModelSequenceException,
    MultiTypeMinosModelSequenceException,
)
from ..importlib import (
    classname,
)
from ..meta import (
    classproperty,
    property_or_classproperty,
    self_or_classmethod,
)
from ..protocol import (
    MinosAvroProtocol,
)
from .fields import (
    Field,
)
from .serializers import (
    AvroDataDecoder,
    AvroDataEncoder,
    AvroSchemaDecoder,
    AvroSchemaEncoder,
    DataDecoder,
    DataEncoder,
    SchemaDecoder,
    SchemaEncoder,
)
from .types import (
    MissingSentinel,
    ModelType,
)

logger = logging.getLogger(__name__)


class Model(Mapping):
    """Base class for ``minos`` model entities."""

    _field_cls: Type[Field] = Field

    _fields: dict[str, Field]
    __eq_reversing: bool

    def __init__(self, fields: Union[Iterable[Field], dict[str, Field]] = None, **kwargs):
        """Class constructor.

        :param fields: Dictionary that contains the ``Field`` instances of the model indexed by name.
        """
        if fields is None:
            fields = dict()
        if not isinstance(fields, dict):
            fields = {field.name: field for field in fields}
        self._fields = fields
        self.__eq_reversing = False

    @classmethod
    def from_typed_dict(cls: Type[T], typed_dict: TypedDict, *args, **kwargs) -> T:
        """Build a ``Model`` from a ``TypeDict`` and ``data``.

        :param typed_dict: ``TypeDict`` object containing the DTO's structure
        :param args: Positional arguments to be passed to the model constructor.
        :param kwargs: Named arguments to be passed to the model constructor.
        :return: A new ``DataTransferObject`` instance.
        """
        return cls.from_model_type(ModelType.from_typed_dict(typed_dict), *args, **kwargs)

    @classmethod
    @abstractmethod
    def from_model_type(cls: Type[T], model_type: ModelType, *args, **kwargs) -> T:
        """Build a ``Model`` from a ``ModelType``.

        :param model_type: ``ModelType`` object containing the DTO's structure
        :param args: Positional arguments to be passed to the model constructor.
        :param kwargs: Named arguments to be passed to the model constructor.
        :return: A new ``DeclarativeModel`` instance.
        """

    @classmethod
    def from_avro_str(cls: Type[T], raw: str, **kwargs) -> Union[T, list[T]]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A ``str`` representation of the model.
        :return: A single instance or a sequence of instances.
        """
        raw = b64decode(raw.encode())
        return cls.from_avro_bytes(raw, **kwargs)

    # noinspection PyUnusedLocal
    @classmethod
    def from_avro_bytes(cls: Type[T], raw: bytes, batch_mode: bool = False, **kwargs) -> Union[T, list[T]]:
        """Build a single instance or a sequence of instances from bytes.

        :param raw: A ``bytes`` representation of the model.
        :param batch_mode: If ``True`` the data is processed as a list of models, otherwise the data is processed as a
            single model.
        :param kwargs: Additional named arguments.
        :return: A single instance or a sequence of instances.
        """
        schema = MinosAvroProtocol.decode_schema(raw)
        data = MinosAvroProtocol.decode(raw, batch_mode=batch_mode)

        if batch_mode:
            return [cls.from_avro(schema, entry) for entry in data]

        return cls.from_avro(schema, data)

    @classmethod
    def from_avro(cls: Type[T], schema: Any, data: Any) -> T:
        """Build a new instance from the ``avro`` schema and data.

        :param schema: The avro schema of the model.
        :param data: The avro data of the model.
        :return: A new ``DynamicModel`` instance.
        """
        schema_decoder = AvroSchemaDecoder()
        type_ = schema_decoder.build(schema)

        data_decoder = AvroDataDecoder()
        instance = data_decoder.build(data, type_)

        return instance

    @classmethod
    def to_avro_str(cls: Type[T], models: list[T]) -> str:
        """Build the avro string representation of the given object instances.

        :param models: A sequence of minos models.
        :return: A bytes object.
        """
        return b64encode(cls.to_avro_bytes(models)).decode()

    @classmethod
    def to_avro_bytes(cls: Type[T], models: list[T]) -> bytes:
        """Create a ``bytes`` representation of the given object instances.

        :param models: A sequence of minos models.
        :return: A bytes object.
        """
        if len(models) == 0:
            raise EmptyMinosModelSequenceException("'models' parameter cannot be empty.")

        model_type = type(models[0])
        if not all(model_type == type(model) for model in models):
            raise MultiTypeMinosModelSequenceException(
                f"Every model must have type {model_type} to be valid. Found types: {[type(model) for model in models]}"
            )

        avro_schema = models[0].avro_schema
        # noinspection PyTypeChecker
        return MinosAvroProtocol.encode([model.avro_data for model in models], avro_schema, batch_mode=True)

    # noinspection PyMethodParameters
    @property_or_classproperty
    def model_type(self_or_cls) -> ModelType:
        """Get the model type of the instance.

        :return: A ``ModelType`` instance.
        """
        # noinspection PyTypeChecker
        return ModelType.from_model(self_or_cls)

    # noinspection PyMethodParameters
    @classproperty
    def classname(cls) -> str:
        """Compute the current class namespace.
        :return: An string object.
        """
        # noinspection PyTypeChecker
        return classname(cls)

    # noinspection PyMethodParameters
    @property_or_classproperty
    def type_hints(self_or_cls) -> dict[str, type]:
        """Get the type hinting of the instance or class.

        :return: A dictionary in which the keys are the field names and the values are the types.
        """
        return dict(self_or_cls._type_hints())

    # noinspection PyMethodParameters
    @self_or_classmethod
    def _type_hints(self_or_cls) -> Iterator[tuple[str, Any]]:
        if not isinstance(self_or_cls, type):
            yield from ((field.name, field.real_type) for field in self_or_cls.fields.values())

    # noinspection PyMethodParameters
    @property_or_classproperty
    def type_hints_parameters(self_or_cls) -> tuple[TypeVar, ...]:
        """Get the sequence of generic type hints parameters..

        :return: A tuple of `TypeVar` instances.
        """
        return getattr(self_or_cls, "__parameters__", tuple())

    @property
    def fields(self) -> dict[str, Field]:
        """Fields getter"""
        return self._fields

    def __setitem__(self, key: str, value: Any) -> None:
        try:
            self._fields[key].value = value
        except KeyError:
            raise KeyError(f"{type(self).__name__!r} does not contain the {key!r} field")

    def __getitem__(self, item: str) -> Any:
        try:
            return self._fields[item].value
        except KeyError:
            raise KeyError(f"{type(self).__name__!r} does not contain the {item!r} field.")

    def __setattr__(self, key: str, value: Any) -> None:
        if key.startswith("_"):
            object.__setattr__(self, key, value)
            return

        if key not in self._fields:
            raise AttributeError(f"{type(self).__name__!r} does not contain the {key!r} attribute.")

        self[key] = value

    def __getattr__(self, item: str) -> Any:
        if item.startswith("_") or item not in self._fields:
            raise AttributeError(f"{type(self).__name__!r} does not contain the {item!r} attribute.")

        return self[item]

    # noinspection PyMethodParameters
    @property_or_classproperty
    def avro_schema(self_or_cls) -> list[dict[str, Any]]:
        """Compute the avro schema of the model.

        :return: A dictionary object.
        """
        # noinspection PyTypeChecker
        encoder = AvroSchemaEncoder()
        return encoder.build(self_or_cls)

    @property
    def avro_data(self) -> dict[str, Any]:
        """Compute the avro data of the model.

        :return: A dictionary object.
        """
        encoder = AvroDataEncoder()
        return encoder.build(self)

    @property
    def avro_str(self) -> str:
        """Generate bytes representation of the current instance.

        :return: A bytes object.
        """
        # noinspection PyTypeChecker
        return b64encode(self.avro_bytes).decode()

    @property
    def avro_bytes(self) -> bytes:
        """Generate bytes representation of the current instance.

        :return: A bytes object.
        """
        # noinspection PyTypeChecker
        return MinosAvroProtocol.encode(self.avro_data, self.avro_schema)

    # noinspection PyUnusedLocal
    @staticmethod
    def encode_schema(encoder: SchemaEncoder, target: Any, **kwargs) -> Any:
        """Encode schema with the given encoder.

        :param encoder: The encoder instance.
        :param target: An optional pre-encoded schema.
        :param kwargs: Additional named arguments.
        :return: The encoded schema of the instance.
        """
        return MissingSentinel

    # noinspection PyUnusedLocal
    @staticmethod
    def decode_schema(decoder: SchemaDecoder, target: Any, **kwargs) -> Any:
        """Decode schema with the given encoder.

        :param decoder: The decoder instance.
        :param target: The schema to be decoded.
        :param kwargs: Additional named arguments.
        :return: The decoded schema as a type.
        """
        return MissingSentinel

    # noinspection PyUnusedLocal
    @staticmethod
    def encode_data(encoder: DataEncoder, target: Any, **kwargs) -> Any:
        """Encode data with the given encoder.

        :param encoder: The encoder instance.
        :param target: An optional pre-encoded data.
        :param kwargs: Additional named arguments.
        :return: The encoded data of the instance.
        """
        return MissingSentinel

    # noinspection PyUnusedLocal
    @staticmethod
    def decode_data(decoder: DataDecoder, target: Any, type_: ModelType, **kwargs) -> Any:
        """Decode data with the given decoder.

        :param decoder: The decoder instance.
        :param target: The data to be decoded.
        :param type_: The data type.
        :param kwargs: Additional named arguments.
        :return: A decoded instance.
        """
        return MissingSentinel

    def __eq__(self: T, other: T) -> bool:
        if type(self) == type(other) and self.fields == other.fields:
            return True

        if not self.__eq_reversing:
            try:
                self.__eq_reversing = True
                return other == self
            finally:
                self.__eq_reversing = False

        return False

    def __hash__(self) -> int:
        return hash(tuple(self.fields.values()))

    def __iter__(self) -> Iterable[str]:
        yield from self.fields.keys()

    def __len__(self):
        return len(self.fields)

    def __repr__(self) -> str:
        fields_repr = ", ".join(repr(field) for field in self.fields.values())
        return f"{type(self).__name__}({fields_repr})"


T = TypeVar("T", bound=Model)

"""minos.common.model.abc module."""

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
    NoReturn,
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
    AvroSchemaDecoder,
    AvroSchemaEncoder,
)
from .types import (
    ModelType,
)

logger = logging.getLogger(__name__)


class Model(Mapping):
    """Base class for ``minos`` model entities."""

    _fields: dict[str, Field]

    def __init__(self, fields: Union[Iterable[Field], dict[str, Field]] = None):
        """Class constructor.

        :param fields: Dictionary that contains the ``Field`` instances of the model indexed by name.
        """
        if fields is None:
            fields = dict()
        if not isinstance(fields, dict):
            fields = {field.name: field for field in fields}
        self._fields = fields

    @classmethod
    def from_avro_str(cls: Type[T], raw: str, **kwargs) -> Union[T, list[T]]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """
        raw = b64decode(raw.encode())
        return cls.from_avro_bytes(raw, **kwargs)

    @classmethod
    def from_avro_bytes(cls: Type[T], raw: bytes, **kwargs) -> Union[T, list[T]]:
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """
        schema = MinosAvroProtocol.decode_schema(raw)
        decoded = MinosAvroProtocol.decode(raw)

        # FIXME: Extend implementation of the `AvroDataDecoder` to avoid this fix.
        schema["name"] = "{}.fake.{}".format(*schema["name"].rsplit(".", 1))

        if isinstance(decoded, list):
            return [cls.from_avro(schema, d | kwargs) for d in decoded]
        return cls.from_avro(schema, decoded | kwargs)

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
    def from_avro(cls: Type[T], schema: Union[dict[str, Any], list[dict[str, Any]]], data: dict[str, Any]) -> T:
        """Build a new instance from the ``avro`` schema and data.

        :param schema: The avro schema of the model.
        :param data: The avro data of the model.
        :return: A new ``DynamicModel`` instance.
        """
        if isinstance(schema, list):
            schema = schema[-1]
        model_type = AvroSchemaDecoder(schema).build()
        return AvroDataDecoder("", model_type).build(data)

    @classmethod
    def to_avro_str(cls: Type[T], models: list[T]) -> str:
        """Create a bytes representation of the given object instances.

        :param models: A sequence of minos models.
        :return: A bytes object.
        """
        return b64encode(cls.to_avro_bytes(models)).decode()

    @classmethod
    def to_avro_bytes(cls: Type[T], models: list[T]) -> bytes:
        """Create a bytes representation of the given object instances.

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
        return MinosAvroProtocol().encode([model.avro_data for model in models], avro_schema)

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

    def __setitem__(self, key: str, value: Any) -> NoReturn:
        try:
            setattr(self, key, value)
        except AttributeError as exc:
            raise KeyError(exc)

    def __getitem__(self, item: str) -> Any:
        try:
            return getattr(self, item)
        except AttributeError as exc:
            raise KeyError(exc)

    def __setattr__(self, key: str, value: Any) -> NoReturn:
        if key.startswith("_"):
            super().__setattr__(key, value)
        elif key in self._fields:
            self._fields[key].value = value
        else:
            raise AttributeError(f"{type(self).__name__!r} does not contain the {key!r} field")

    def __getattr__(self, item: str) -> Any:
        if item in self._fields:
            return self._fields[item].value
        else:
            raise AttributeError(f"{type(self).__name__!r} does not contain the {item!r} field.")

    # noinspection PyMethodParameters
    @property_or_classproperty
    def avro_schema(self_or_cls) -> list[dict[str, Any]]:
        """Compute the avro schema of the model.

        :return: A dictionary object.
        """
        # noinspection PyTypeChecker
        return [AvroSchemaEncoder("", ModelType.from_model(self_or_cls)).build()["type"]]

    @property
    def avro_data(self) -> dict[str, Any]:
        """Compute the avro data of the model.

        :return: A dictionary object.
        """
        return {name: field.avro_data for name, field in self.fields.items()}

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
        return MinosAvroProtocol().encode(self.avro_data, self.avro_schema)

    def __eq__(self: T, other: T) -> bool:
        return type(self) == type(other) and self.fields == other.fields

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

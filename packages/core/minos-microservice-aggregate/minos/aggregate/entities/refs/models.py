from __future__ import (
    annotations,
)

import logging
from typing import (
    Any,
    Generic,
    Optional,
    TypeVar,
    Union,
    get_args,
)
from uuid import (
    UUID,
    SafeUUID,
)

from minos.common import (
    DataDecoder,
    DataEncoder,
    DeclarativeModel,
    Model,
    ModelType,
    SchemaDecoder,
    SchemaEncoder,
)

from ...contextvars import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
)

logger = logging.getLogger(__name__)
MT = TypeVar("MT", bound=Model)


class Ref(DeclarativeModel, UUID, Generic[MT]):
    """Model Reference."""

    data: Union[MT, UUID]

    def __init__(self, data: Union[MT, UUID], *args, **kwargs):
        if not isinstance(data, UUID) and not hasattr(data, "uuid"):
            raise ValueError(f"data must be an {UUID!r} instance or have 'uuid' as one of its fields")
        DeclarativeModel.__init__(self, data, *args, **kwargs)

    def __setitem__(self, key: str, value: Any) -> None:
        try:
            return super().__setitem__(key, value)
        except KeyError as exc:
            if key == "uuid":
                self.data = value
                return

            try:
                self.data[key] = value
            except Exception:
                raise exc

    def __getitem__(self, item: str) -> Any:
        try:
            return super().__getitem__(item)
        except KeyError as exc:
            if item == "uuid":
                return self.uuid

            if not self.resolved:
                raise KeyError(f"The reference is not resolved: {self!r}")

            try:
                return self.data[item]
            except Exception:
                raise exc

    def __setattr__(self, key: str, value: Any) -> None:
        try:
            return super().__setattr__(key, value)
        except AttributeError as exc:
            if key == "uuid":
                self.data = value
                return

            try:
                setattr(self.data, key, value)
            except Exception:
                raise exc

    def __getattr__(self, item: str) -> Any:
        try:
            return super().__getattr__(item)
        except AttributeError as exc:
            if item == "data":
                raise exc

            if not self.resolved:
                raise AttributeError(f"The reference is not resolved: {self!r}")

            try:
                return getattr(self.data, item)
            except Exception:
                raise exc

    @property
    def int(self) -> int:
        """Get the UUID as a 128-bit integer.

        :return: An integer value.
        """
        return self.uuid.int

    @property
    def is_safe(self) -> SafeUUID:
        """Get an enum indicating whether the UUID has been generated in a way that is safe.

        :return: A ``SafeUUID`` value.
        """
        return self.uuid.is_safe

    # noinspection PyMethodParameters
    @classmethod
    def encode_schema(cls, encoder: SchemaEncoder, target: Any, **kwargs) -> Any:
        """Encode schema with the given encoder.

        :param encoder: The encoder instance.
        :param target: An optional pre-encoded schema.
        :return: The encoded schema of the instance.
        """
        schema = encoder.build(target.type_hints["data"], **kwargs)
        return [(sub | {"logicalType": cls.classname}) for sub in schema]

    @classmethod
    def decode_schema(cls, decoder: SchemaDecoder, target: Any, **kwargs) -> ModelType:
        """Decode schema with the given encoder.

        :param decoder: The decoder instance.
        :param target: The schema to be decoded.
        :return: The decoded schema as a type.
        """
        decoded = decoder.build(target, **kwargs)
        if not isinstance(decoded, ModelType):
            raise ValueError(f"The decoded type is not valid: {decoded}")
        return ModelType.from_model(cls[decoded])

    @staticmethod
    def encode_data(encoder: DataEncoder, target: Any, **kwargs) -> Any:
        """Encode data with the given encoder.

        :param encoder: The encoder instance.
        :param target: An optional pre-encoded data.
        :return: The encoded data of the instance.
        """
        target = target["data"]
        if IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get() and isinstance(target, dict):
            target = target["uuid"]
        return encoder.build(target, **kwargs)

    @classmethod
    def decode_data(cls, decoder: DataDecoder, target: Any, type_: ModelType, **kwargs) -> Ref:
        """Decode data with the given decoder.

        :param decoder: The decoder instance.
        :param target: The data to be decoded.
        :param type_: The data type.
        :return: A decoded instance.
        """
        decoded = decoder.build(target, type_.type_hints["data"], **kwargs)
        return cls(decoded, additional_type_hints=type_.type_hints)

    def __eq__(self, other):
        return super().__eq__(other) or self.uuid == other or self.data == other

    def __hash__(self):
        return hash(self.uuid)

    @property
    def uuid(self) -> UUID:
        """Get the UUID that identifies the ``Model``.

        :return:
        """
        if not self.resolved:
            return self.data
        return self.data.uuid

    @uuid.setter
    def uuid(self, value: UUID) -> None:
        """Set the uuid that identifies the ``Model``.

        :return: This method does not return anything.
        """
        raise RuntimeError("The 'uuid' must be set through the '__setattr__' method.")  # pragma: no cover

    @property
    def data_cls(self) -> Optional[type]:
        """Get data class if available.

        :return: A model type.
        """
        args = get_args(self.type_hints["data"])
        if args[0] != MT:
            return args[0]
        return None

    # noinspection PyUnusedLocal
    async def resolve(self, force: bool = False, **kwargs) -> None:
        """Resolve the instance.

        :param force: If ``True``, the resolution will be performed also if it is not necessary.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        if not force and self.resolved:
            return

        from .resolvers import (
            RefResolver,
        )

        self.data = await RefResolver(**kwargs).resolve(self)

    @property
    def resolved(self) -> bool:
        """Check if the instance is already resolved.

        :return: ``True`` if resolved or ``False`` otherwise.
        """
        try:
            return not isinstance(self.data, UUID)
        except AttributeError:
            return False

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.data!r})"

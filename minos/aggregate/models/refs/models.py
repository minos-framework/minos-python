from __future__ import (
    annotations,
)

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

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    DataDecoder,
    DataEncoder,
    DeclarativeModel,
    Model,
    ModelType,
    SchemaDecoder,
    SchemaEncoder,
    self_or_classmethod,
)
from minos.networks import (
    DynamicBroker,
    DynamicBrokerPool,
)

from ...contextvars import (
    IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR,
)
from ..entities import (
    Entity,
)

MT = TypeVar("MT", bound=Model)


class AggregateRef(Entity):
    """Aggregate Ref class."""

    version: int

    def __init__(self, uuid: UUID, *args, **kwargs):
        super().__init__(uuid=uuid, *args, **kwargs)


class ModelRef(DeclarativeModel, UUID, Generic[MT]):
    """Model Reference."""

    data: Union[MT, UUID]

    @inject
    def __init__(
        self, data: Union[MT, UUID], *args, broker_pool: DynamicBrokerPool = Provide["broker_pool"], **kwargs,
    ):
        if not isinstance(data, UUID) and not hasattr(data, "uuid"):
            raise ValueError(f"data must be an {UUID!r} instance or have 'uuid' as one of its fields")
        DeclarativeModel.__init__(self, data, *args, **kwargs)

        self._broker_pool = broker_pool

    def __getattr__(self, item: str) -> Any:
        try:
            return super().__getattr__(item)
        except AttributeError as exc:
            if item != "data":
                return getattr(self.data, item)
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
    @self_or_classmethod
    def encode_schema(self_or_cls, encoder: SchemaEncoder, target: Any, **kwargs) -> Any:
        """Encode schema with the given encoder.

        :param encoder: The encoder instance.
        :param target: An optional pre-encoded schema.
        :return: The encoded schema of the instance.
        """
        schema = encoder.build(target.type_hints["data"], **kwargs)
        return [(sub | {"logicalType": self_or_cls.classname}) for sub in schema]

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

    def encode_data(self, encoder: DataEncoder, target: Any, **kwargs) -> Any:
        """Encode data with the given encoder.

        :param encoder: The encoder instance.
        :param target: An optional pre-encoded data.
        :return: The encoded data of the instance.
        """
        target = self.data
        if IS_REPOSITORY_SERIALIZATION_CONTEXT_VAR.get() and not isinstance(target, UUID):
            target = target.uuid
        return encoder.build(target, **kwargs)

    @classmethod
    def decode_data(cls, decoder: DataDecoder, target: Any, type_: ModelType, **kwargs) -> ModelRef:
        """Decode data with the given decoder.

        :param decoder: The decoder instance.
        :param target: The data to be decoded.
        :param type_: The data type.
        :return: A decoded instance.
        """
        decoded = decoder.build(target, type_.type_hints["data"], **kwargs)
        return ModelRef(decoded, additional_type_hints=type_.type_hints)

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

        name = self.data_cls.__name__

        async with self._broker_pool.acquire() as broker:
            await broker.send(data={"uuid": self.uuid}, topic=f"Get{name}")
            self.data = await self._get_response(broker)

    @staticmethod
    async def _get_response(handler: DynamicBroker, **kwargs) -> MT:
        handler_entry = await handler.get_one(**kwargs)
        response = handler_entry.data
        return response.data

    @property
    def resolved(self) -> bool:
        """Check if the instance is already resolved.

        :return: ``True`` if resolved or ``False`` otherwise.
        """
        return not isinstance(self.data, UUID)

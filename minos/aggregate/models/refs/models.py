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
    AvroDataEncoder,
    DeclarativeModel,
    Field,
)
from minos.networks import (
    DynamicBroker,
    DynamicBrokerPool,
)

from ...events import (
    SUBMITTING_EVENT_CONTEXT_VAR,
)
from ..entities import (
    Entity,
)

MT = TypeVar("MT")


class AggregateRef(Entity):
    """Aggregate Ref class."""

    version: int

    def __init__(self, uuid: UUID, *args, **kwargs):
        super().__init__(uuid=uuid, *args, **kwargs)


class FieldRef(Field):
    """Ref Field class."""

    @property
    def avro_data(self) -> Any:
        """Compute the avro data of the model.

        If submitting is active then simply the identifier is used, otherwise the complete value is used.

        :return: A dictionary object.
        """
        if not SUBMITTING_EVENT_CONTEXT_VAR.get():
            return super().avro_data

        value = self.value
        if not isinstance(value, UUID):
            value = value.uuid

        return AvroDataEncoder(value).build()


class ModelRef(DeclarativeModel, UUID, Generic[MT]):
    """Model Reference."""

    _field_cls = FieldRef
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
        if args:
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

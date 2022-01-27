from __future__ import (
    annotations,
)

from enum import (
    IntEnum,
)
from typing import (
    Any,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from minos.networks import (
    BrokerMessage,
)


class SagaRequest:
    """Saga Request class."""

    __slots__ = (
        "_target",
        "_content",
    )

    def __init__(self, target: str, content: Any = None):
        self._target = target
        self._content = content

    @property
    def target(self) -> str:
        """Get the target of the request.

        :return: A ``str`` instance.
        """
        return self._target

    # noinspection PyUnusedLocal
    async def content(self, **kwargs) -> Any:
        """Get the content of the request.

        :param kwargs: Additional named parameters.
        :return: The content of the request.
        """
        return self._content

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self._target == other._target and self._content == other._content

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._target!r}, {self._content!r})"

    def __hash__(self):
        return hash((self._target, self._content))


class SagaResponse:
    """Saga Response class."""

    __slots__ = (
        "_content",
        "_status",
        "_related_services",
        "_uuid",
    )

    def __init__(
        self,
        content: Any = None,
        related_services: Optional[set[str]] = None,
        status: Optional[Union[int, SagaResponseStatus]] = None,
        uuid: Optional[UUID] = None,
        *args,
        **kwargs,
    ):
        if status is None:
            status = SagaResponseStatus.SUCCESS
        if not isinstance(status, SagaResponseStatus):
            status = SagaResponseStatus.from_raw(status)
        if related_services is None:
            related_services = set()

        self._content = content
        self._status = status
        self._related_services = related_services
        self._uuid = uuid

    @classmethod
    def from_message(cls, message: BrokerMessage) -> SagaResponse:
        """Build a new ``SagaResponse`` from a ``BrokerMessage``.

        :param message: The ``BrokerMessage`` instance.
        :return: A ``SagaResponse``.
        """

        uuid = UUID(message.headers["saga"])

        if raw_related_services := message.headers.get("related_services"):
            related_services = set(raw_related_services.split(","))
        else:
            related_services = set()

        return SagaResponse(message.content, related_services, message.status, uuid)

    # noinspection PyUnusedLocal
    async def content(self, **kwargs) -> Any:
        """Get the response content.

        :param kwargs: Additional named parameters.
        :return: The content of the response.
        """
        return self._content

    @property
    def ok(self) -> bool:
        """Check if the response is okay.

        :return: ``True`` if the response is okay
        """
        return self._status == SagaResponseStatus.SUCCESS

    @property
    def status(self) -> SagaResponseStatus:
        """Get the status code of the response.

        :return: A ``ResponseStatus`` instance.
        """
        return self._status

    @property
    def related_services(self) -> set[str]:
        """Get the microservice name that generated the response.

        :return: An string value containing the microservice name.
        """
        return self._related_services

    @property
    def uuid(self) -> UUID:
        """Get the identifier of the saga execution that must receive the response.

        :return: An ``UUID`` value.
        """
        return self._uuid

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, type(self))
            and self._content == other._content
            and self._status == other._status
            and self._related_services == other._related_services
            and self._uuid == other._uuid
        )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"{self._content!r}, {self._status!r}, {self._related_services!r}, {self._uuid!r}"
            f")"
        )

    def __hash__(self):
        return hash((self._content, self._status, tuple(sorted(self._related_services)), self._uuid))


class SagaResponseStatus(IntEnum):
    """Saga Response Status class."""

    SUCCESS = 200
    ERROR = 400
    SYSTEM_ERROR = 500

    @classmethod
    def from_raw(cls, raw: int) -> SagaResponseStatus:
        """Build a new instance from raw.

        :param raw: The raw representation of the instance.
        :return: A ``SagaResponseStatus`` instance.
        """
        return next(obj for obj in cls if obj.value == raw)

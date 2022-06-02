from collections.abc import (
    Iterable,
)
from typing import (
    Any,
    Union,
)
from uuid import (
    UUID,
)

from ...messages import (
    BrokerMessage,
)


class BrokerPublisherTransactionEntry:
    """Broker Publisher Transaction Entry class."""

    def __init__(self, message: Union[memoryview, bytes, BrokerMessage], transaction_uuid: UUID):
        if isinstance(message, memoryview):
            message = message.tobytes()
        if isinstance(message, bytes):
            message = BrokerMessage.from_avro_bytes(message)
        self._message = message
        self._transaction_uuid = transaction_uuid

    @property
    def message(self) -> BrokerMessage:
        """Get the message.

        :return: A ``BrokerMessage`` instance.
        """
        return self._message

    @property
    def transaction_uuid(self) -> UUID:
        """Get the transaction identifier.

        :return: An ``UUID`` instance.
        """
        return self._transaction_uuid

    def as_raw(self) -> dict[str, Any]:
        """Get a raw representation of the instance.

        :return: A dictionary in which the keys are attribute names and values the attribute contents.
        """
        return {
            "message": self._message.avro_bytes,
            "transaction_uuid": self._transaction_uuid,
        }

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        yield from (
            self._message,
            self._transaction_uuid,
        )

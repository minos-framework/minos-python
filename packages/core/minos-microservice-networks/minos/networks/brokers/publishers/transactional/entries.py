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
    """TODO"""

    def __init__(self, message: Union[bytes, BrokerMessage], transaction_uuid: UUID):
        if isinstance(message, bytes):
            message = BrokerMessage.from_avro_bytes(message)
        self._message = message
        self._transaction_uuid = transaction_uuid

    @property
    def message(self) -> BrokerMessage:
        """TODO"""
        return self._message

    @property
    def transaction_uuid(self) -> UUID:
        """TODO"""
        return self._transaction_uuid

    def as_raw(self) -> dict[str, Any]:
        """TODO"""
        return {
            "message": self._message.avro_bytes,
            "transaction_uuid": self._transaction_uuid,
        }

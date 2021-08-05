"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from datetime import (
    datetime,
)
from typing import (
    Callable,
    Generic,
    Optional,
    Type,
    TypeVar,
)

from minos.common import (
    Model,
)

T = TypeVar("T")


class HandlerEntry(Generic[T]):
    """Handler Entry class."""

    def __init__(
        self,
        id: int,
        topic: str,
        callback: Optional[Callable],
        partition_id: int,
        data: T,
        retry: int,
        created_at: datetime,
    ):
        self.id = id
        self.topic = topic
        self.callback = callback
        self.partition_id = partition_id
        self.data = data
        self.retry = retry
        self.created_at = created_at

    @classmethod
    def from_raw(
        cls,
        raw: tuple[int, str, int, bytes, int, datetime],
        callback_lookup: Optional[Callable[[str], Callable]] = None,
        data_cls: Type[Model] = Model,
    ) -> HandlerEntry:
        """Build a new instance from raw

        :param raw: The raw entry.
        :param callback_lookup: The lookup function for callbacks.
        :param data_cls: The model class to be used for deserialization.
        :return: A new ``HandlerEntry``.
        """
        id = raw[0]
        topic = raw[1]
        callback = None if callback_lookup is None else callback_lookup(raw[1])
        partition_id = raw[2]
        data = data_cls.from_avro_bytes(raw[3])
        retry = raw[4]
        created_at = raw[5]

        entry = cls(id, topic, callback, partition_id, data, retry, created_at)
        return entry

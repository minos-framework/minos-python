"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
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

from cached_property import (
    cached_property,
)

from minos.common import (
    MinosException,
    Model,
)

logger = logging.getLogger(__name__)
T = TypeVar("T")


class HandlerEntry(Generic[T]):
    """Handler Entry class."""

    def __init__(
        self,
        id: int,
        topic: str,
        partition_id: int,
        data_bytes: bytes,
        retry: int = 0,
        created_at: Optional[datetime] = None,
        data_cls: Type[Model] = Model,
        callback_lookup: Optional[Callable] = None,
        exception: Optional[Exception] = None,
    ):
        if created_at is None:
            created_at = datetime.now()

        self.id = id
        self.topic = topic
        self.partition_id = partition_id
        self.data_bytes = data_bytes
        self.data_cls = data_cls
        self.retry = retry
        self.created_at = created_at
        self.callback_lookup = callback_lookup
        self.exception = exception

    @property
    def success(self) -> bool:
        """Check if the entry is in success state or not

        :return: A boolean value.
        """
        return self.exception is None

    @cached_property
    def callback(self) -> Optional[Callable]:
        """Get the callback if lookup is provided.

        :return: A Callable object or None.
        """
        if self.callback_lookup is None:
            return None
        return self.callback_lookup(self.topic)

    @cached_property
    def data(self) -> T:
        """Get the data.

        :return: A ``Model`` inherited instance.
        """
        return self.data_cls.from_avro_bytes(self.data_bytes)

    def __repr__(self):
        try:
            return f"{type(self).__name__}(id={self.id!r}, data={self.data!r})"
        except MinosException:
            return f"{type(self).__name__}(id={self.id!r})"

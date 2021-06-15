"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from __future__ import (
    annotations,
)

from typing import (
    Type,
    Union,
)

from ...meta import (
    self_or_classmethod,
)
from ...protocol import (
    MinosAvroProtocol,
)
from ..abc import (
    Model,
)
from ..fields import (
    AvroSchemaDecoder,
)
from .abc import (
    DeclarativeModel,
)


class Event(DeclarativeModel):
    """Base Event class."""

    topic: str
    items: list[Model]

    def __init__(self, topic: str, items: list[Model], *args, _items_type=None, **kwargs):
        if _items_type is None:
            items_type = {
                item.classname: type(item) if isinstance(item, DeclarativeModel) else item.typed_dict for item in items
            }
            items_type = Union[tuple(items_type.values())]
            _items_type = list[items_type]
        self._items_type = _items_type
        super().__init__(topic, items, *args, **kwargs)

    @property
    def items_type(self) -> Type[list[Model]]:
        """TODO

        :return: TODO
        """
        return self._items_type

    @classmethod
    def from_avro_bytes(cls, raw: bytes, **kwargs):
        """Build a single instance or a sequence of instances from bytes

        :param raw: A bytes data.
        :return: A single instance or a sequence of instances.
        """
        schema = MinosAvroProtocol.decode_schema(raw)
        items_schema = next(raw for raw in schema["fields"] if raw["name"] == "items")["type"]
        _items_type = AvroSchemaDecoder(items_schema).build()
        return super().from_avro_bytes(raw, _items_type=_items_type, **kwargs)

    # noinspection PyMethodParameters
    @self_or_classmethod
    def _type_hints(self_or_cls) -> dict[str, Type]:
        for k, v in super()._type_hints():
            if k == "items" and not isinstance(self_or_cls, type):
                v = self_or_cls._items_type
            yield k, v
        return

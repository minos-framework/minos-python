"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import abc
from typing import (
    Any,
    NoReturn,
    Type,
)

from .exceptions import (
    MultiTypeMinosModelSequenceException,
)
from .importlib import (
    import_module,
)
from .meta import (
    self_or_classmethod,
)
from .model import (
    Aggregate,
    MinosModel,
)


class MinosBaseBroker(abc.ABC):
    """Base Broker class."""

    def __init__(self, topic: str):
        self.topic = topic

    @abc.abstractmethod
    async def send(self, items: list[Aggregate]) -> NoReturn:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :return: This method does not return anything.
        """
        raise NotImplementedError

    async def send_one(self, item: Aggregate) -> NoReturn:
        """Send one ``Aggregate`` instance.

        :param item: The instance to be send.
        :return: This method does not return anything.
        """
        return await self.send([item])


class Event(MinosModel):
    """Base Event class."""

    topic: str
    model: str
    items: list[Aggregate]

    def __init__(self, topic: str, items: list[Aggregate], model: str = None, *args, **kwargs):
        if model is None:
            model_cls = type(items[0])
            model = model_cls.classname
        else:
            model_cls = import_module(model)

        if not all(model_cls == type(item) for item in items):
            raise MultiTypeMinosModelSequenceException(
                f"Every model must have type {model_cls} to be valid. Found types: {[type(model) for model in items]}"
            )

        super().__init__(topic, model, list(items), *args, **kwargs)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> MinosModel:
        """Build a new instance from a dictionary.

        :param d: A dictionary object.
        :return: A new ``MinosModel`` instance.
        """
        if "model" in d and "items" in d:
            model_cls = import_module(d["model"])
            # noinspection PyUnresolvedReferences
            d["items"] = [model_cls.from_dict(item) for item in d["items"]]
        return super().from_dict(d)

    # noinspection PyMethodParameters
    @self_or_classmethod
    def _type_hints(self_or_cls) -> dict[str, Type]:
        for k, v in super()._type_hints():
            if k == "items" and not isinstance(self_or_cls, type):
                v = list[self_or_cls.model_cls]
            yield k, v
        return

    @property
    def model_cls(self) -> Type[Aggregate]:
        """Get the model class.

        :return: A type object.
        """
        # noinspection PyTypeChecker
        return import_module(self.model)


class Command(Event):
    """Base Command class."""

    reply_on: str

    def __init__(self, topic: str, items: list[Aggregate], reply_on: str, *args, **kwargs):
        super().__init__(topic, items, *args, reply_on=reply_on, **kwargs)

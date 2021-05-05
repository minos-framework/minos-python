"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from __future__ import (
    annotations,
)

from typing import (
    Any,
    Type,
)

from minos.common.exceptions import (
    MultiTypeMinosModelSequenceException,
)
from minos.common.importlib import (
    import_module,
)
from minos.common.meta import (
    self_or_classmethod,
)
from minos.common.model import (
    Aggregate,
    MinosModel,
)


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

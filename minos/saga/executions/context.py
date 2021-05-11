"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    NoReturn,
    Type,
)

from minos.common import (
    MinosModel,
    MinosModelException,
    ModelField,
    classname,
    import_module,
)
from minos.common.meta import (
    self_or_classmethod,
)


class SagaContext(MinosModel):
    """TODO"""

    types_: dict[str, str]

    def __init__(self, **kwargs):
        types_ = kwargs.pop("types_", None)
        if types_ is None:
            types_ = {k: classname(type(v)) for k, v in kwargs.items()}
        super().__init__(types_=types_, **kwargs)

    def update(self, key: str, value: MinosModel) -> NoReturn:
        """TODO

        :param key: TODO
        :param value: TODO
        :return: TODO
        """
        setattr(self, key, value)

    def __setattr__(self, key, value):
        try:
            super().__setattr__(key, value)
        except MinosModelException:
            self.types_[key] = classname(type(value))
            self._fields[key] = ModelField(key, type(value), value)

    # noinspection PyMethodParameters
    @self_or_classmethod
    def _type_hints(self_or_cls) -> dict[str, Type]:
        yield from super()._type_hints()
        for k, v in self_or_cls.types_.items():
            yield k, import_module(v)

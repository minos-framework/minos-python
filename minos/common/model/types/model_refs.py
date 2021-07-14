"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from collections import (
    defaultdict,
)
from typing import (
    Any,
    Iterable,
    NoReturn,
    Optional,
    Type,
    get_args,
    get_origin,
)
from uuid import (
    UUID,
)

from .builders import (
    TypeHintBuilder,
)
from .data_types import (
    ModelRef,
)


class ModelRefExtractor:
    """TODO"""

    def __init__(self, value: Any, kind: Optional[Type] = None):
        if kind is None:
            kind = TypeHintBuilder(value).build()
        self.value = value
        self.kind = kind

    def build(self) -> dict[str, set[UUID]]:
        """TODO

        :return: TODO
        """
        ans = defaultdict(set)
        self._build(self.value, self.kind, ans)
        return ans

    def _build(self, value: Any, kind: Type, ans: dict[str, set[UUID]]) -> NoReturn:
        from ..abc import (
            Model,
        )

        if isinstance(value, (tuple, list, set)):
            self._build_iterable(value, get_args(kind)[0], ans)

        elif isinstance(value, dict):
            self._build_iterable(value.keys(), get_args(kind)[0], ans)
            self._build_iterable(value.values(), get_args(kind)[1], ans)

        elif isinstance(value, Model):
            for field in value.fields.values():
                self._build(field.value, field.type, ans)

        elif get_origin(kind) is ModelRef and isinstance(value, UUID):
            cls = get_args(kind)[0]
            name = cls.__name__
            ans[name].add(value)

    def _build_iterable(self, value: Iterable, kind: Type, ans: dict[str, set[UUID]]) -> NoReturn:
        for sub_value in value:
            self._build(sub_value, kind, ans)

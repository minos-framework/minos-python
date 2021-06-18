"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Any,
    Generic,
    NoReturn,
    TypeVar,
)

from .abc import (
    DynamicModel,
)

T = TypeVar("T")


class BucketModel(DynamicModel, Generic[T]):
    """Bucket Model class."""

    def __getitem__(self, item: str) -> Any:
        return getattr(self, item)

    def __setitem__(self, key: str, value: Any) -> NoReturn:
        setattr(self, key, value)

    @classmethod
    def empty(cls) -> T:
        """Build an empty ``BucketModel`` instance.

        :return: A ``BucketModel`` instance.
        """
        return cls(dict())

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
    TypeVar,
)

from .abc import (
    DynamicModel,
)


class BucketModel(DynamicModel):
    """Bucket Model class."""

    @classmethod
    def empty(cls: Type[T]) -> T:
        """Build an empty ``BucketModel`` instance.

        :return: A ``BucketModel`` instance.
        """
        return cls(dict())


T = TypeVar("T", bound=BucketModel)

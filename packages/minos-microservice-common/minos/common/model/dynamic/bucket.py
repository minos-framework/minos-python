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
        return cls(fields=dict())


T = TypeVar("T", bound=BucketModel)

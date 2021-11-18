from __future__ import (
    annotations,
)

import logging
from typing import (
    TypeVar,
)

from minos.aggregate import (
    AggregateDiff,
    ModelRefResolver,
)

logger = logging.getLogger(__name__)


class PreEventHandler:
    """Pre Event Handler class."""

    @classmethod
    async def handle(cls, diff: T, resolve_references: bool = True, **kwargs) -> T:
        """TODO

        :param diff: TODO
        :param resolve_references: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if not isinstance(diff, AggregateDiff) or not resolve_references:
            return diff

        try:
            return await ModelRefResolver(**kwargs).resolve(diff)
        except Exception as exc:
            logger.warning(f"An exception was raised while trying to resolve model references: {exc!r}")
            return diff


T = TypeVar("T")

from __future__ import (
    annotations,
)

import logging
from typing import (
    TypeVar,
)

from minos.aggregate import (
    Event,
    RefResolver,
)

logger = logging.getLogger(__name__)


class PreEventHandler:
    """Pre Event Handler class."""

    @classmethod
    async def handle(cls, diff: T, resolve_references: bool = False, **kwargs) -> T:
        """Handle Ref resolution for Events.

        :param diff: The instance containing ``Ref`` instances.
        :param resolve_references: If ``True`` the resolution is performed, otherwise it is skipped.
        :param kwargs: Additional named arguments.
        :return: The original instance with the ``Ref`` references already resolved.
        """
        if not isinstance(diff, Event) or not resolve_references:
            return diff

        try:
            return await RefResolver(**kwargs).resolve(diff)
        except Exception as exc:
            logger.exception(f"An exception was raised while trying to resolve model references: {exc!r}")
            return diff


T = TypeVar("T")

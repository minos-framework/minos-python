"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import annotations

import logging
from typing import (
    Generic,
    TypeVar,
)
from uuid import UUID
from ....constants import NULL_UUID
from ..abc import DeclarativeModel

T = TypeVar("T")
logger = logging.getLogger(__name__)


class SubAggregate(DeclarativeModel, Generic[T]):
    """Base aggregate class."""

    uuid: UUID
    version: int

    def __init__(
        self, *args, uuid: UUID = NULL_UUID, version: int = 0, **kwargs,
    ):

        super().__init__(uuid, version, *args, **kwargs)

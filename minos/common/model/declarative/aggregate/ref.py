"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from uuid import (
    UUID,
)

from ..abc import (
    DeclarativeModel,
)

logger = logging.getLogger(__name__)


class AggregateRef(DeclarativeModel):
    """Sub Aggregate class."""

    uuid: UUID
    version: int

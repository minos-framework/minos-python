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

from ..entities import (
    Entity,
)

logger = logging.getLogger(__name__)


class AggregateRef(Entity):
    """Sub Aggregate class."""

    version: int

    def __init__(self, uuid: UUID, *args, **kwargs):
        super().__init__(uuid=uuid, *args, **kwargs)

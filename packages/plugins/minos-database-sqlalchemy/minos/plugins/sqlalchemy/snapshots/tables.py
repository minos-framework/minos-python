from __future__ import (
    annotations,
)

import logging

from sqlalchemy import (
    Table,
)

from minos.aggregate import (
    Entity,
)

logger = logging.getLogger(__name__)


class SqlAlchemySnapshotTableBuilder:
    """TODO"""

    def build(self, type_: Entity) -> Table:
        """TODO"""

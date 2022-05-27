from __future__ import (
    annotations,
)

import logging
from datetime import (
    date,
    datetime,
    time,
)
from typing import (
    Any,
)

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Time,
)
from sqlalchemy.sql.type_api import (
    TypeEngine,
)

from minos.aggregate import (
    Entity,
)
from minos.common import (
    ModelType,
)

logger = logging.getLogger(__name__)


class SqlAlchemySnapshotTableBuilder:
    """TODO"""

    def build(self, *types: type[Entity]) -> dict[type[Entity], Table]:
        """TODO"""
        ans = dict()

        metadata = MetaData()

        for type_ in types:
            ans[type_] = self._build_one(type_, metadata)
        return ans

    def _build_one(self, type_: type[Entity], metadata: MetaData) -> Table:
        mt = ModelType.from_model(type_)

        columns = self._build_columns(mt.type_hints)

        table = Table(mt.name, metadata, *columns)

        return table

    def _build_columns(self, type_hints: dict[str, Any]) -> list[Column]:
        columns = list()
        for name, type_ in type_hints.items():
            column_type = self._build_column_type(type_)
            column = Column(name, column_type)
            columns.append(column)

        return columns

    def _build_column_type(self, type_: type) -> TypeEngine:
        if type_ is str:
            return String()
        if type_ is int:
            return Integer()
        if type_ is bool:
            return Boolean()
        if type_ is datetime:
            return DateTime()
        if type_ is float:
            return Float()
        if type_ is date:
            return Date()
        if type_ is time:
            return Time()

        raise ValueError("TODO")

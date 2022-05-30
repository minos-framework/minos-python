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
from uuid import (
    UUID,
)

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Time,
)

from minos.aggregate import (
    Entity,
    Ref,
)
from minos.common import (
    NULL_UUID,
    ModelType,
)

logger = logging.getLogger(__name__)


class SqlAlchemySnapshotTableBuilder:
    """TODO"""

    @classmethod
    def build(cls, *types: type[Entity]) -> MetaData:
        """TODO"""
        metadata = MetaData()

        for type_ in types:
            cls._build_one(type_, metadata)
        return metadata

    @classmethod
    def _build_one(cls, type_: type[Entity], metadata: MetaData) -> Table:
        if not isinstance(type_, type) or not issubclass(type_, Entity):
            raise ValueError("TODO")

        mt = ModelType.from_model(type_)

        columns = cls._build_columns(mt.type_hints)

        table = Table(mt.name, metadata, *columns, PrimaryKeyConstraint("uuid", "transaction_uuid"))

        return table

    @classmethod
    def _build_columns(cls, type_hints: dict[str, Any]) -> list[Column]:
        columns = list()
        for name, type_ in type_hints.items():
            column = cls._build_column(name, type_)
            columns.append(column)

        column = Column("transaction_uuid", String, nullable=False, default=str(NULL_UUID))
        columns.append(column)
        return columns

    @staticmethod
    def _build_column(name: str, type_: type) -> Column:
        foreign_key = None
        if type_ is UUID:
            column_type = String()
        elif type_ is str:
            column_type = String()
        elif type_ is int:
            column_type = Integer()
        elif type_ is bool:
            column_type = Boolean()
        elif type_ is datetime:
            column_type = DateTime()
        elif type_ is float:
            column_type = Float()
        elif type_ is date:
            column_type = Date()
        elif type_ is time:
            column_type = Time()
        elif type_ is Ref:
            column_type, foreign_key = String(), ForeignKey(f"{Ref.data_cls.__name__}.uuid")
        else:
            column_type = LargeBinary()

        nullable = False

        return Column(name, column_type, foreign_key, nullable=nullable)

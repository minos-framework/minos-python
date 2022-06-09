from __future__ import (
    annotations,
)

import logging
from datetime import (
    date,
    datetime,
    time,
    timedelta,
    timezone,
)
from enum import (
    Enum,
)
from typing import (
    Any,
    get_args,
)
from uuid import (
    UUID,
)

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
)
from sqlalchemy import Enum as EnumType
from sqlalchemy import (
    Float,
    Integer,
    Interval,
    LargeBinary,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Time,
)
from sqlalchemy_utils import (
    UUIDType,
)

from minos.aggregate import (
    Entity,
    Ref,
)
from minos.common import (
    NULL_UUID,
    ModelType,
    is_optional,
)

from ....types import (
    EncodedType,
)

logger = logging.getLogger(__name__)


class SqlAlchemySnapshotTableFactory:
    """SqlAlchemy Snapshot Table Factory class."""

    @classmethod
    def build(cls, *types: type[Entity]) -> MetaData:
        """Build a metadata instance from a sequence of types.

        :param types: The sequence of ``Entity`` types.
        :return: A ``MetaData`` instance.
        """
        metadata = MetaData()

        for type_ in types:
            cls._build_one(type_, metadata)
        return metadata

    @classmethod
    def _build_one(cls, type_: type[Entity], metadata: MetaData) -> Table:
        if not isinstance(type_, type) or not issubclass(type_, Entity):
            raise ValueError(f"The `type_` param must be a subclass of {Entity!r}. Obtained: {type_!r}")

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

        column = Column("transaction_uuid", UUIDType, nullable=False, default=str(NULL_UUID))
        columns.append(column)
        return columns

    @staticmethod
    def _build_column(name: str, type_: type) -> Column:
        nullable = False

        if is_optional(type_, strict=True):
            nullable = True
            type_ = get_args(type_)[0]

        if issubclass(type_, Enum):
            column_type = EnumType(type_)
        elif issubclass(type_, bool):
            column_type = Boolean()
        elif issubclass(type_, int):
            column_type = Integer()
        elif issubclass(type_, float):
            column_type = Float()
        elif issubclass(type_, str):
            column_type = String()
        elif issubclass(type_, bytes):
            column_type = LargeBinary()
        elif issubclass(type_, datetime):
            column_type = DateTime(timezone=timezone.utc)
        elif issubclass(type_, timedelta):
            column_type = Interval()
        elif issubclass(type_, date):
            column_type = Date()
        elif issubclass(type_, time):
            column_type = Time()
        elif issubclass(type_, Ref) or (isinstance(type_, ModelType) and issubclass(type_.model_cls, Ref)):
            column_type = UUIDType(binary=False)
        elif issubclass(type_, UUID):
            column_type = UUIDType(binary=False)
        else:
            column_type = EncodedType()

        return Column(name, column_type, nullable=nullable)

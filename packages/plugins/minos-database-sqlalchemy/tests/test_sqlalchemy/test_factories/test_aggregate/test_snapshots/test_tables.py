import unittest
from datetime import (
    date,
    datetime,
    time,
    timedelta,
)
from enum import (
    Enum,
)
from uuid import (
    UUID,
)

from sqlalchemy import (
    Boolean,
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
    String,
    Time,
    inspect,
)
from sqlalchemy_utils import (
    UUIDType,
)

from minos.aggregate import (
    Entity,
    Ref,
)
from minos.common import (
    DeclarativeModel,
)
from minos.plugins.sqlalchemy import (
    EncodedType,
    SqlAlchemyDatabaseClient,
    SqlAlchemyDatabaseOperation,
    SqlAlchemySnapshotTableFactory,
)
from tests.utils import (
    SqlAlchemyTestCase,
)


class TestSqlAlchemySnapshotTableFactory(SqlAlchemyTestCase):
    def test_build(self):
        class _Foo(Entity):
            bar: str

        class _Bar(Entity):
            foo: Ref[_Foo]

        observed = SqlAlchemySnapshotTableFactory.build(_Foo, _Bar)

        self.assertIsInstance(observed, MetaData)

        self.assertEqual({_Foo.__name__, _Bar.__name__}, observed.tables.keys())
        self.assertEqual(
            {"uuid", "version", "created_at", "updated_at", "bar", "_transaction_uuid", "_deleted"},
            set(observed.tables[_Foo.__name__].columns.keys()),
        )
        self.assertEqual(
            {"uuid", "version", "created_at", "updated_at", "foo", "_transaction_uuid", "_deleted"},
            set(observed.tables[_Bar.__name__].columns.keys()),
        )

    def test_build_uuid(self):
        class _Foo(Entity):
            ...

        metadata = SqlAlchemySnapshotTableFactory.build(_Foo)
        observed = metadata.tables[_Foo.__name__].columns["uuid"]

        self.assertIsInstance(observed.type, UUIDType)
        self.assertTrue(observed.primary_key)

    def test_build_version(self):
        class _Foo(Entity):
            ...

        metadata = SqlAlchemySnapshotTableFactory.build(_Foo)
        observed = metadata.tables[_Foo.__name__].columns["version"]

        self.assertIsInstance(observed.type, Integer)
        self.assertFalse(observed.primary_key)

    def test_build_created_at(self):
        class _Foo(Entity):
            ...

        metadata = SqlAlchemySnapshotTableFactory.build(_Foo)
        observed = metadata.tables[_Foo.__name__].columns["created_at"]

        self.assertIsInstance(observed.type, DateTime)
        self.assertFalse(observed.primary_key)

    def test_build_updated_at(self):
        class _Foo(Entity):
            ...

        metadata = SqlAlchemySnapshotTableFactory.build(_Foo)
        observed = metadata.tables[_Foo.__name__].columns["updated_at"]

        self.assertIsInstance(observed.type, DateTime)
        self.assertFalse(observed.primary_key)

    def test_build_transaction_uuid(self):
        class _Foo(Entity):
            ...

        metadata = SqlAlchemySnapshotTableFactory.build(_Foo)
        observed = metadata.tables[_Foo.__name__].columns["_transaction_uuid"]

        self.assertIsInstance(observed.type, UUIDType)
        self.assertTrue(observed.primary_key)

    def test_build_deleted(self):
        class _Foo(Entity):
            ...

        metadata = SqlAlchemySnapshotTableFactory.build(_Foo)
        observed = metadata.tables[_Foo.__name__].columns["_deleted"]

        self.assertIsInstance(observed.type, Boolean)
        self.assertTrue(observed.default)

    def test_build_column_types(self):
        class _Enum(Enum):
            one = 1
            two = 2

        class _Bar(Entity):
            ...

        class _Foo(Entity):
            enum_: _Enum
            bool_: bool
            int_: int
            float_: float
            str_: str
            bytes_: bytes
            datetime_: datetime
            timedelta_: timedelta
            date_: date
            time_: time
            ref_: Ref[_Bar]
            uuid_: UUID
            encoded_: _Bar

        metadata = SqlAlchemySnapshotTableFactory.build(_Foo, _Bar)

        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["enum_"].type, EnumType)
        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["bool_"].type, Boolean)
        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["int_"].type, Integer)
        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["float_"].type, Float)
        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["str_"].type, String)
        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["bytes_"].type, LargeBinary)
        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["datetime_"].type, DateTime)
        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["timedelta_"].type, Interval)
        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["date_"].type, Date)
        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["time_"].type, Time)
        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["ref_"].type, UUIDType)
        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["uuid_"].type, UUIDType)
        self.assertIsInstance(metadata.tables[_Foo.__name__].columns["encoded_"].type, EncodedType)

    async def test_build_create_tables(self):
        class _Foo(Entity):
            bar: str

        class _Bar(Entity):
            foo: Ref[_Foo]

        metadata = SqlAlchemySnapshotTableFactory.build(_Foo, _Bar)

        create_operation = SqlAlchemyDatabaseOperation(metadata.create_all)
        drop_operation = SqlAlchemyDatabaseOperation(metadata.drop_all)
        get_tables_operation = SqlAlchemyDatabaseOperation(lambda connectable: inspect(connectable).get_table_names())

        async with SqlAlchemyDatabaseClient.from_config(self.config) as client:
            observed = set(await client.connection.run_sync(get_tables_operation.statement))
            self.assertEqual(set(), observed)

            await client.execute(create_operation)

            observed = set(await client.connection.run_sync(get_tables_operation.statement))
            self.assertEqual({_Bar.__name__, _Foo.__name__}, observed)

            await client.execute(drop_operation)

            observed = set(await client.connection.run_sync(get_tables_operation.statement))
            self.assertEqual(set(), observed)

    def test_build_raises(self):
        class _Foo(DeclarativeModel):
            ...

        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            SqlAlchemySnapshotTableFactory.build(_Foo)


if __name__ == "__main__":
    unittest.main()

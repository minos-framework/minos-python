import unittest

from sqlalchemy import (
    MetaData,
)

from minos.aggregate import (
    Entity,
    Ref,
)
from minos.plugins.sqlalchemy import (
    SqlAlchemySnapshotTableFactory,
)


class TestSqlAlchemySnapshotTableFactory(unittest.IsolatedAsyncioTestCase):
    def test_entity(self):
        class _Foo(Entity):
            bar: str

        class _Bar(Entity):
            foo: Ref[_Foo]

        observed = SqlAlchemySnapshotTableFactory.build(_Foo, _Bar)

        self.assertIsInstance(observed, MetaData)

        self.assertEqual({_Foo.__name__, _Bar.__name__}, observed.tables.keys())


if __name__ == "__main__":
    unittest.main()

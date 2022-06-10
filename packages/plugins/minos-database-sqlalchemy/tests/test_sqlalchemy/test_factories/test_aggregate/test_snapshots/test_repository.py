import unittest

from minos.aggregate import (
    DatabaseSnapshotRepository,
)
from minos.aggregate.testing import (
    SnapshotRepositoryTestCase,
)
from minos.common import (
    ProgrammingException,
)
from tests.utils import (
    SqlAlchemyTestCase,
)


# noinspection SqlNoDataSourceInspection
class TestDatabaseSnapshotRepository(SqlAlchemyTestCase, SnapshotRepositoryTestCase):
    __test__ = True

    def build_snapshot_repository(self):
        return DatabaseSnapshotRepository.from_config(self.config)

    async def test_find_contains(self):
        # The contains condition does not apply to standard SQL databases.
        with self.assertRaises(ProgrammingException):
            await super().test_find_contains()


if __name__ == "__main__":
    unittest.main()

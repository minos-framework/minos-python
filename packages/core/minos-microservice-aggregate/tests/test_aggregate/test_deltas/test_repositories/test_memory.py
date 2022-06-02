import unittest

from minos.aggregate import (
    DeltaRepository,
    InMemoryDeltaRepository,
)
from minos.aggregate.testing import (
    DeltaRepositoryTestCase,
)
from tests.utils import (
    AggregateTestCase,
)


class TestInMemoryDeltaRepositorySubmit(AggregateTestCase, DeltaRepositoryTestCase):
    __test__ = True

    def build_delta_repository(self) -> DeltaRepository:
        """For testing purposes."""
        return InMemoryDeltaRepository()


if __name__ == "__main__":
    unittest.main()

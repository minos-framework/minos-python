import unittest

from minos.aggregate import (
    EventRepository,
    InMemoryEventRepository,
)
from minos.aggregate.testing import (
    EventRepositoryTestCase,
)
from tests.utils import (
    AggregateTestCase,
)


class TestInMemoryEventRepositorySubmit(AggregateTestCase, EventRepositoryTestCase):
    __test__ = True

    def build_event_repository(self) -> EventRepository:
        """For testing purposes."""
        return InMemoryEventRepository()


if __name__ == "__main__":
    unittest.main()

import unittest

from minos.aggregate import (
    EventRepository,
    InMemoryEventRepository,
)
from minos.aggregate.testing import (
    EventRepositorySelectTestCase,
    EventRepositorySubmitTestCase,
)
from tests.utils import (
    AggregateTestCase,
)


class TestInMemoryEventRepositorySubmit(AggregateTestCase, EventRepositorySubmitTestCase):
    __test__ = True

    def build_event_repository(self) -> EventRepository:
        """For testing purposes."""
        return InMemoryEventRepository()


class TestInMemoryEventRepositorySelect(AggregateTestCase, EventRepositorySelectTestCase):
    __test__ = True

    def build_event_repository(self) -> EventRepository:
        """For testing purposes."""
        return InMemoryEventRepository()


if __name__ == "__main__":
    unittest.main()

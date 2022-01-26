import unittest

from minos.aggregate import (
    EventRepository,
    InMemoryEventRepository,
)
from tests.testcases import (
    EventRepositorySelectTestCase,
    EventRepositorySubmitTestCase,
)


class TestInMemoryEventRepositorySubmit(EventRepositorySubmitTestCase):
    __test__ = True

    def build_event_repository(self) -> EventRepository:
        """For testing purposes."""
        return InMemoryEventRepository()


class TestInMemoryEventRepositorySelect(EventRepositorySelectTestCase):
    __test__ = True

    def build_event_repository(self) -> EventRepository:
        """For testing purposes."""
        return InMemoryEventRepository()


if __name__ == "__main__":
    unittest.main()

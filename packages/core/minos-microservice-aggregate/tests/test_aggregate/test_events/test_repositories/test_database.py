import unittest
from datetime import (
    datetime,
    timezone,
)
from unittest.mock import (
    patch,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    Action,
    DatabaseEventRepository,
    EventRepository,
)
from minos.aggregate.testing import (
    EventRepositorySelectTestCase,
    EventRepositorySubmitTestCase,
)
from minos.common import (
    DatabaseClient,
    IntegrityException,
    current_datetime,
)
from tests.utils import (
    AggregateTestCase,
    FakeAsyncIterator,
)


class TestDatabaseEventRepositorySubmit(AggregateTestCase, EventRepositorySubmitTestCase):
    __test__ = True

    def build_event_repository(self) -> EventRepository:
        """For testing purposes."""
        return DatabaseEventRepository.from_config(self.config)

    async def test_generate_uuid(self):
        fetch_one = [
            (1, self.uuid, 1, current_datetime()),
        ]
        fetch_all = [(self.uuid, "example.Car", 1, bytes(), 1, Action.CREATE, current_datetime())]
        with patch.object(DatabaseClient, "fetch_one", side_effect=fetch_one):
            with patch.object(DatabaseClient, "fetch_all", return_value=FakeAsyncIterator(fetch_all)):
                await super().test_generate_uuid()

    async def test_submit(self):
        fetch_one = [
            (1, self.uuid, 1, current_datetime()),
        ]
        fetch_all = [(self.uuid, "example.Car", 1, bytes(), 1, Action.CREATE, current_datetime())]
        with patch.object(DatabaseClient, "fetch_one", side_effect=fetch_one):
            with patch.object(DatabaseClient, "fetch_all", return_value=FakeAsyncIterator(fetch_all)):
                await super().test_submit()

    async def test_submit_with_version(self):
        fetch_one = [
            (1, self.uuid, 3, current_datetime()),
        ]
        fetch_all = [(self.uuid, "example.Car", 3, bytes(), 1, Action.CREATE, current_datetime())]
        with patch.object(DatabaseClient, "fetch_one", side_effect=fetch_one):
            with patch.object(DatabaseClient, "fetch_all", return_value=FakeAsyncIterator(fetch_all)):
                await super().test_submit_with_version()

    async def test_submit_with_created_at(self):
        created_at = datetime(2021, 10, 25, 8, 30, tzinfo=timezone.utc)
        fetch_one = [
            (1, self.uuid, 1, created_at),
        ]
        fetch_all = [(self.uuid, "example.Car", 1, bytes(), 1, Action.CREATE, created_at)]
        with patch.object(DatabaseClient, "fetch_one", side_effect=fetch_one):
            with patch.object(DatabaseClient, "fetch_all", return_value=FakeAsyncIterator(fetch_all)):
                await super().test_submit_with_created_at()

    async def test_submit_raises_duplicate(self):
        fetch_one = [
            (1, uuid4(), 1, current_datetime()),
            IntegrityException(""),
            (1,),
        ]
        with patch.object(DatabaseClient, "fetch_one", side_effect=fetch_one):
            await super().test_submit_raises_duplicate()

    async def test_offset(self):
        fetch_one = [
            (0,),
            (1, uuid4(), 1, current_datetime()),
            (1,),
        ]
        with patch.object(DatabaseClient, "fetch_one", side_effect=fetch_one):
            await super().test_offset()


@unittest.skip
class TestDatabaseEventRepositorySelect(AggregateTestCase, EventRepositorySelectTestCase):
    __test__ = True

    def build_event_repository(self) -> EventRepository:
        """For testing purposes."""
        return DatabaseEventRepository.from_config(self.config)


if __name__ == "__main__":
    unittest.main()

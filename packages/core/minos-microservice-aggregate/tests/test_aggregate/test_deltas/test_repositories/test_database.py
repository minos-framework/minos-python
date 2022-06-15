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
    DatabaseDeltaRepository,
    DeltaRepository,
)
from minos.aggregate.testing import (
    DeltaRepositoryTestCase,
)
from minos.common import (
    DatabaseClient,
    IntegrityException,
    classname,
    current_datetime,
)
from tests.utils import (
    AggregateTestCase,
    FakeAsyncIterator,
)


class TestDatabaseDeltaRepository(AggregateTestCase, DeltaRepositoryTestCase):
    __test__ = True

    def build_delta_repository(self) -> DeltaRepository:
        """For testing purposes."""
        return DatabaseDeltaRepository.from_config(self.config)

    async def test_generate_uuid(self):
        fetch_one = [
            (1, self.uuid, 1, current_datetime()),
        ]
        fetch_all = [(self.uuid, classname(self.Car), 1, bytes(), 1, Action.CREATE, current_datetime())]
        with patch.object(DatabaseClient, "fetch_one", side_effect=fetch_one):
            with patch.object(DatabaseClient, "fetch_all", return_value=FakeAsyncIterator(fetch_all)):
                await super().test_generate_uuid()

    async def test_submit(self):
        fetch_one = [
            (1, self.uuid, 1, current_datetime()),
        ]
        fetch_all = [(self.uuid, classname(self.Car), 1, bytes(), 1, Action.CREATE, current_datetime())]
        with patch.object(DatabaseClient, "fetch_one", side_effect=fetch_one):
            with patch.object(DatabaseClient, "fetch_all", return_value=FakeAsyncIterator(fetch_all)):
                await super().test_submit()

    async def test_submit_with_version(self):
        fetch_one = [
            (1, self.uuid, 3, current_datetime()),
        ]
        fetch_all = [(self.uuid, classname(self.Car), 3, bytes(), 1, Action.CREATE, current_datetime())]
        with patch.object(DatabaseClient, "fetch_one", side_effect=fetch_one):
            with patch.object(DatabaseClient, "fetch_all", return_value=FakeAsyncIterator(fetch_all)):
                await super().test_submit_with_version()

    async def test_submit_with_created_at(self):
        created_at = datetime(2021, 10, 25, 8, 30, tzinfo=timezone.utc)
        fetch_one = [
            (1, self.uuid, 1, created_at),
        ]
        fetch_all = [(self.uuid, classname(self.Car), 1, bytes(), 1, Action.CREATE, created_at)]
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

    async def populate(self) -> None:
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                (1, uuid4(), 1, current_datetime()),
                (2, uuid4(), 2, current_datetime()),
                (3, uuid4(), 1, current_datetime()),
                (4, uuid4(), 3, current_datetime()),
                (5, uuid4(), 4, current_datetime()),
                (6, uuid4(), 2, current_datetime()),
                (7, uuid4(), 1, current_datetime()),
                (8, uuid4(), 3, current_datetime()),
                (9, uuid4(), 3, current_datetime()),
                (10, uuid4(), 4, current_datetime()),
            ],
        ):
            await super().populate()

    async def test_select(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[0].as_raw().values()),
                    tuple(self.entries[1].as_raw().values()),
                    tuple(self.entries[2].as_raw().values()),
                    tuple(self.entries[3].as_raw().values()),
                    tuple(self.entries[4].as_raw().values()),
                    tuple(self.entries[5].as_raw().values()),
                    tuple(self.entries[6].as_raw().values()),
                    tuple(self.entries[7].as_raw().values()),
                    tuple(self.entries[8].as_raw().values()),
                    tuple(self.entries[9].as_raw().values()),
                ]
            ),
        ):
            await super().test_select()

    async def test_select_id(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[1].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_id()

    async def test_select_id_lt(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[0].as_raw().values()),
                    tuple(self.entries[1].as_raw().values()),
                    tuple(self.entries[2].as_raw().values()),
                    tuple(self.entries[3].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_id_lt()

    async def test_select_id_gt(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[4].as_raw().values()),
                    tuple(self.entries[5].as_raw().values()),
                    tuple(self.entries[6].as_raw().values()),
                    tuple(self.entries[7].as_raw().values()),
                    tuple(self.entries[8].as_raw().values()),
                    tuple(self.entries[9].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_id_gt()

    async def test_select_id_le(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[0].as_raw().values()),
                    tuple(self.entries[1].as_raw().values()),
                    tuple(self.entries[2].as_raw().values()),
                    tuple(self.entries[3].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_id_le()

    async def test_select_id_ge(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[4].as_raw().values()),
                    tuple(self.entries[5].as_raw().values()),
                    tuple(self.entries[6].as_raw().values()),
                    tuple(self.entries[7].as_raw().values()),
                    tuple(self.entries[8].as_raw().values()),
                    tuple(self.entries[9].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_id_ge()

    async def test_select_uuid(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[2].as_raw().values()),
                    tuple(self.entries[5].as_raw().values()),
                    tuple(self.entries[7].as_raw().values()),
                    tuple(self.entries[8].as_raw().values()),
                    tuple(self.entries[9].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_uuid()

    async def test_select_name(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[6].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_name()

    async def test_select_version(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[4].as_raw().values()),
                    tuple(self.entries[9].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_version()

    async def test_select_version_lt(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[0].as_raw().values()),
                    tuple(self.entries[2].as_raw().values()),
                    tuple(self.entries[6].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_version_lt()

    async def test_select_version_gt(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[1].as_raw().values()),
                    tuple(self.entries[3].as_raw().values()),
                    tuple(self.entries[4].as_raw().values()),
                    tuple(self.entries[5].as_raw().values()),
                    tuple(self.entries[7].as_raw().values()),
                    tuple(self.entries[8].as_raw().values()),
                    tuple(self.entries[9].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_version_gt()

    async def test_select_version_le(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[0].as_raw().values()),
                    tuple(self.entries[2].as_raw().values()),
                    tuple(self.entries[6].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_version_le()

    async def test_select_version_ge(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[1].as_raw().values()),
                    tuple(self.entries[3].as_raw().values()),
                    tuple(self.entries[4].as_raw().values()),
                    tuple(self.entries[5].as_raw().values()),
                    tuple(self.entries[7].as_raw().values()),
                    tuple(self.entries[8].as_raw().values()),
                    tuple(self.entries[9].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_version_ge()

    async def test_select_transaction_uuid_null(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[0].as_raw().values()),
                    tuple(self.entries[1].as_raw().values()),
                    tuple(self.entries[2].as_raw().values()),
                    tuple(self.entries[3].as_raw().values()),
                    tuple(self.entries[4].as_raw().values()),
                    tuple(self.entries[5].as_raw().values()),
                    tuple(self.entries[6].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_transaction_uuid_null()

    async def test_select_transaction_uuid(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[7].as_raw().values()),
                    tuple(self.entries[9].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_transaction_uuid()

    async def test_select_transaction_uuid_ne(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[7].as_raw().values()),
                    tuple(self.entries[8].as_raw().values()),
                    tuple(self.entries[9].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_transaction_uuid_ne()

    async def test_select_transaction_uuid_in(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[7].as_raw().values()),
                    tuple(self.entries[8].as_raw().values()),
                    tuple(self.entries[9].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_transaction_uuid_in()

    async def test_select_combined(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(self.entries[2].as_raw().values()),
                    tuple(self.entries[5].as_raw().values()),
                    tuple(self.entries[7].as_raw().values()),
                    tuple(self.entries[8].as_raw().values()),
                    tuple(self.entries[9].as_raw().values()),
                ]
            ),
        ):
            await super().test_select_combined()


if __name__ == "__main__":
    unittest.main()

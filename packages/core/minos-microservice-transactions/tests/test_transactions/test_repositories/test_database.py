import unittest
from unittest.mock import (
    patch,
)

from minos.common import (
    DatabaseClient,
    ProgrammingException,
    current_datetime,
)
from minos.transactions import (
    DatabaseTransactionRepository,
    TransactionRepository,
    TransactionStatus,
)
from minos.transactions.testing import (
    TransactionRepositoryTestCase,
)
from tests.utils import (
    FakeAsyncIterator,
    TransactionsTestCase,
)


# noinspection SqlNoDataSourceInspection
class TestDatabaseTransactionRepository(TransactionsTestCase, TransactionRepositoryTestCase):
    __test__ = True

    def build_transaction_repository(self) -> TransactionRepository:
        return DatabaseTransactionRepository.from_config(self.config)

    async def test_submit(self):
        with patch.object(
            DatabaseClient,
            "fetch_one",
            return_value=[current_datetime()],
        ):
            with patch.object(
                DatabaseClient,
                "fetch_all",
                return_value=FakeAsyncIterator([(self.uuid, TransactionStatus.PENDING)]),
            ):
                await super().test_submit()

    async def test_submit_pending_raises(self):
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                (current_datetime(),),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
            ],
        ):
            await super().test_submit_pending_raises()

    async def test_submit_reserving_raises(self):
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                (current_datetime(),),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
            ],
        ):
            await super().test_submit_reserving_raises()

    async def test_submit_reserved_raises(self):
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                (current_datetime(),),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
            ],
        ):
            await super().test_submit_reserved_raises()

    async def test_submit_committing_raises(self):
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                (current_datetime(),),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
            ],
        ):
            await super().test_submit_committing_raises()

    async def test_submit_committed_raises(self):
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                (current_datetime(),),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
            ],
        ):
            await super().test_submit_committed_raises()

    async def test_submit_rejected_raises(self):
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                (current_datetime(),),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
                ProgrammingException(""),
            ],
        ):
            await super().test_submit_rejected_raises()

    async def populate(self) -> None:
        with patch.object(
            DatabaseClient,
            "fetch_one",
            side_effect=[
                (current_datetime(),),
                (current_datetime(),),
                (current_datetime(),),
                (current_datetime(),),
                (current_datetime(),),
                (current_datetime(),),
            ],
        ):
            await super().populate()

    async def test_select(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator([tuple(entry.as_raw().values()) for entry in self.entries]),
        ):
            await super().test_select()

    async def test_select_uuid(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator([tuple(entry.as_raw().values()) for entry in [self.entries[1]]]),
        ):
            await super().test_select_uuid()

    async def test_select_uuid_ne(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [
                    tuple(entry.as_raw().values())
                    for entry in [self.entries[0], self.entries[2], self.entries[3], self.entries[4]]
                ]
            ),
        ):
            await super().test_select_uuid_ne()

    async def test_select_uuid_in(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [tuple(entry.as_raw().values()) for entry in [self.entries[1], self.entries[2]]]
            ),
        ):
            await super().test_select_uuid_in()

    async def test_select_destination_uuid(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator([tuple(entry.as_raw().values()) for entry in [self.entries[4]]]),
        ):
            await super().test_select_destination_uuid()

    async def test_select_status(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [tuple(entry.as_raw().values()) for entry in [self.entries[0], self.entries[1], self.entries[4]]]
            ),
        ):
            await super().test_select_status()

    async def test_select_status_in(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            return_value=FakeAsyncIterator(
                [tuple(entry.as_raw().values()) for entry in [self.entries[2], self.entries[3]]]
            ),
        ):
            await super().test_select_status_in()

    async def test_select_updated_at(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            side_effect=[
                FakeAsyncIterator([tuple(self.entries[2].as_raw().values())]),
                FakeAsyncIterator([tuple(entry.as_raw().values()) for entry in [self.entries[2]]]),
            ],
        ):
            await super().test_select_updated_at()

    async def test_select_updated_at_lt(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            side_effect=[
                FakeAsyncIterator([tuple(self.entries[2].as_raw().values())]),
                FakeAsyncIterator([tuple(entry.as_raw().values()) for entry in [self.entries[0], self.entries[1]]]),
            ],
        ):
            await super().test_select_updated_at_lt()

    async def test_select_updated_at_gt(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            side_effect=[
                FakeAsyncIterator([tuple(self.entries[2].as_raw().values())]),
                FakeAsyncIterator([tuple(entry.as_raw().values()) for entry in [self.entries[3], self.entries[4]]]),
            ],
        ):
            await super().test_select_updated_at_gt()

    async def test_select_updated_at_le(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            side_effect=[
                FakeAsyncIterator([tuple(self.entries[2].as_raw().values())]),
                FakeAsyncIterator(
                    [tuple(entry.as_raw().values()) for entry in [self.entries[0], self.entries[1], self.entries[2]]]
                ),
            ],
        ):
            await super().test_select_updated_at_le()

    async def test_select_updated_at_ge(self):
        with patch.object(
            DatabaseClient,
            "fetch_all",
            side_effect=[
                FakeAsyncIterator([tuple(self.entries[2].as_raw().values())]),
                FakeAsyncIterator(
                    [tuple(entry.as_raw().values()) for entry in [self.entries[2], self.entries[3], self.entries[4]]]
                ),
            ],
        ):
            await super().test_select_updated_at_ge()


if __name__ == "__main__":
    unittest.main()

import unittest
from datetime import (
    timedelta,
)
from unittest.mock import (
    call,
    patch,
)
from uuid import (
    uuid4,
)

from minos.aggregate import (
    EventRepositoryConflictException,
    TransactionEntry,
    TransactionNotFoundException,
    TransactionRepositoryConflictException,
    TransactionService,
    TransactionStatus,
)
from minos.common import (
    current_datetime,
)
from minos.common.testing import (
    DatabaseMinosTestCase,
)
from minos.networks import (
    BrokerCommandEnrouteDecorator,
    InMemoryRequest,
    PeriodicEventEnrouteDecorator,
    ResponseException,
)
from tests.utils import (
    AggregateTestCase,
    FakeAsyncIterator,
)


class TestSnapshotService(AggregateTestCase, DatabaseMinosTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.service = TransactionService(config=self.config)

    def test_get_enroute(self):
        expected = {
            "__reserve__": {BrokerCommandEnrouteDecorator("_ReserveOrderTransaction")},
            "__reject__": {BrokerCommandEnrouteDecorator("_RejectOrderTransaction")},
            "__commit__": {BrokerCommandEnrouteDecorator("_CommitOrderTransaction")},
            "__reject_blocked__": {PeriodicEventEnrouteDecorator("* * * * *")},
        }
        observed = TransactionService.__get_enroute__(self.config)
        self.assertEqual(expected, observed)

    async def test_reserve(self):
        uuid = uuid4()
        with patch("minos.aggregate.TransactionEntry.reserve") as reserve_mock:
            with patch("minos.aggregate.TransactionRepository.get", return_value=TransactionEntry(uuid)) as get_mock:
                response = await self.service.__reserve__(InMemoryRequest(uuid))
        self.assertEqual([call(uuid)], get_mock.call_args_list)
        self.assertEqual([call()], reserve_mock.call_args_list)
        self.assertEqual(None, response)

    async def test_reserve_raises(self):
        with patch("minos.aggregate.TransactionRepository.get", side_effect=TransactionNotFoundException("")):
            with self.assertRaises(ResponseException):
                await self.service.__reserve__(InMemoryRequest(None))

        with patch("minos.aggregate.TransactionRepository.get", return_value=TransactionEntry()):
            with patch("minos.aggregate.TransactionEntry.reserve", side_effect=EventRepositoryConflictException("", 0)):
                with self.assertRaises(ResponseException):
                    await self.service.__reserve__(InMemoryRequest(None))

    async def test_reject(self):
        uuid = uuid4()
        with patch("minos.aggregate.TransactionEntry.reject") as reject_mock:
            with patch("minos.aggregate.TransactionRepository.get", return_value=TransactionEntry(uuid)) as get_mock:
                response = await self.service.__reject__(InMemoryRequest(uuid))
        self.assertEqual([call(uuid)], get_mock.call_args_list)
        self.assertEqual([call()], reject_mock.call_args_list)
        self.assertEqual(None, response)

    async def test_reject_already(self):
        uuid = uuid4()
        with patch("minos.aggregate.TransactionEntry.reject") as reject_mock:
            with patch(
                "minos.aggregate.TransactionRepository.get",
                return_value=TransactionEntry(uuid, status=TransactionStatus.REJECTED),
            ) as get_mock:
                response = await self.service.__reject__(InMemoryRequest(uuid))
        self.assertEqual([call(uuid)], get_mock.call_args_list)
        self.assertEqual([], reject_mock.call_args_list)
        self.assertEqual(None, response)

    async def test_reject_raises(self):
        with patch("minos.aggregate.TransactionRepository.get", side_effect=TransactionNotFoundException("")):
            with self.assertRaises(ResponseException):
                await self.service.__reject__(InMemoryRequest(None))

        with patch("minos.aggregate.TransactionRepository.get", return_value=TransactionEntry()):
            with patch(
                "minos.aggregate.TransactionEntry.reject", side_effect=TransactionRepositoryConflictException("")
            ):
                with self.assertRaises(ResponseException):
                    await self.service.__reject__(InMemoryRequest(None))

    async def test_commit(self):
        uuid = uuid4()
        with patch("minos.aggregate.TransactionEntry.commit") as commit_mock:
            with patch("minos.aggregate.TransactionRepository.get", return_value=TransactionEntry(uuid)) as get_mock:
                response = await self.service.__commit__(InMemoryRequest(uuid))
        self.assertEqual([call(uuid)], get_mock.call_args_list)
        self.assertEqual([call()], commit_mock.call_args_list)
        self.assertEqual(None, response)

    async def test_commit_raises(self):
        with patch("minos.aggregate.TransactionRepository.get", side_effect=TransactionNotFoundException("")):
            with self.assertRaises(ResponseException):
                await self.service.__commit__(InMemoryRequest(None))

        with patch("minos.aggregate.TransactionRepository.get", return_value=TransactionEntry()):
            with patch(
                "minos.aggregate.TransactionEntry.commit", side_effect=TransactionRepositoryConflictException("")
            ):
                with self.assertRaises(ResponseException):
                    await self.service.__commit__(InMemoryRequest(None))

    async def test_reject_blocked(self):
        uuid = uuid4()
        with patch("minos.aggregate.TransactionEntry.reject") as reject_mock:
            with patch(
                "minos.aggregate.TransactionRepository.select", return_value=FakeAsyncIterator([TransactionEntry(uuid)])
            ) as select_mock:
                response = await self.service.__reject_blocked__(InMemoryRequest(uuid))

        self.assertEqual(1, select_mock.call_count)
        self.assertEqual((TransactionStatus.RESERVED,), select_mock.call_args.kwargs["status_in"])
        self.assertAlmostEqual(
            current_datetime() - timedelta(minutes=1),
            select_mock.call_args.kwargs["updated_at_lt"],
            delta=timedelta(seconds=5),
        )

        self.assertEqual([call()], reject_mock.call_args_list)
        self.assertEqual(None, response)

    async def test_reject_blocked_raises(self):
        uuid = uuid4()
        with patch(
            "minos.aggregate.TransactionEntry.reject", side_effect=[None, TransactionRepositoryConflictException("")]
        ) as reject_mock:
            with patch(
                "minos.aggregate.TransactionRepository.select",
                return_value=FakeAsyncIterator([TransactionEntry(uuid), TransactionEntry(uuid)]),
            ):
                response = await self.service.__reject_blocked__(InMemoryRequest(uuid))

        self.assertEqual([call(), call()], reject_mock.call_args_list)
        self.assertEqual(None, response)


if __name__ == "__main__":
    unittest.main()

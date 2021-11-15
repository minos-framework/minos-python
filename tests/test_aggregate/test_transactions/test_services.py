import unittest
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
    TransactionService,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    BrokerCommandEnrouteDecorator,
    ResponseException,
)
from tests.utils import (
    BASE_PATH,
    FakeRequest,
    MinosTestCase,
)


class TestSnapshotService(MinosTestCase, PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def setUp(self) -> None:
        super().setUp()

        self.service = TransactionService(config=self.config)

    def test_get_enroute(self):
        expected = {
            "__reserve__": {BrokerCommandEnrouteDecorator("ReserveOrderTransaction")},
            "__reject__": {BrokerCommandEnrouteDecorator("RejectOrderTransaction")},
            "__commit__": {BrokerCommandEnrouteDecorator("CommitOrderTransaction")},
        }
        observed = TransactionService.__get_enroute__(self.config)
        self.assertEqual(expected, observed)

    async def test_reserve(self):
        uuid = uuid4()
        with patch("minos.aggregate.TransactionEntry.reserve") as reserve_mock:
            with patch("minos.aggregate.TransactionRepository.get", return_value=TransactionEntry(uuid)) as get_mock:
                response = await self.service.__reserve__(FakeRequest(uuid))
        self.assertEqual([call(uuid)], get_mock.call_args_list)
        self.assertEqual([call()], reserve_mock.call_args_list)
        self.assertEqual(None, response)

    async def test_reserve_raises(self):
        with patch("minos.aggregate.TransactionRepository.get", side_effect=TransactionNotFoundException("")):
            with self.assertRaises(ResponseException):
                await self.service.__reserve__(FakeRequest(None))

        with patch("minos.aggregate.TransactionRepository.get", return_value=TransactionEntry()):
            with patch("minos.aggregate.TransactionEntry.reserve", side_effect=EventRepositoryConflictException("", 0)):
                with self.assertRaises(ResponseException):
                    await self.service.__reserve__(FakeRequest(None))

    async def test_reject(self):
        uuid = uuid4()
        with patch("minos.aggregate.TransactionEntry.reject") as reject_mock:
            with patch("minos.aggregate.TransactionRepository.get", return_value=TransactionEntry(uuid)) as get_mock:
                response = await self.service.__reject__(FakeRequest(uuid))
        self.assertEqual([call(uuid)], get_mock.call_args_list)
        self.assertEqual([call()], reject_mock.call_args_list)
        self.assertEqual(None, response)

    async def test_reject_raises(self):
        with patch("minos.aggregate.TransactionRepository.get", side_effect=TransactionNotFoundException("")):
            with self.assertRaises(ResponseException):
                await self.service.__reject__(FakeRequest(None))

    async def test_commit(self):
        uuid = uuid4()
        with patch("minos.aggregate.TransactionEntry.commit") as reject_mock:
            with patch("minos.aggregate.TransactionRepository.get", return_value=TransactionEntry(uuid)) as get_mock:
                response = await self.service.__commit__(FakeRequest(uuid))
        self.assertEqual([call(uuid)], get_mock.call_args_list)
        self.assertEqual([call()], reject_mock.call_args_list)
        self.assertEqual(None, response)

    async def test_commit_raises(self):
        with patch("minos.aggregate.TransactionRepository.get", side_effect=TransactionNotFoundException("")):
            with self.assertRaises(ResponseException):
                await self.service.__commit__(FakeRequest(None))


if __name__ == "__main__":
    unittest.main()

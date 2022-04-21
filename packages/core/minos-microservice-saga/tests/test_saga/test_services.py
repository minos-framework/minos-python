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

from minos.common import (
    Config,
    current_datetime,
)
from minos.networks import (
    BrokerCommandEnrouteDecorator,
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerRequest,
    InMemoryRequest,
    PeriodicEventEnrouteDecorator,
    ResponseException,
)
from minos.saga import (
    SagaManager,
    SagaResponse,
    SagaResponseStatus,
    SagaService,
)
from minos.transactions import (
    TransactionEntry,
    TransactionNotFoundException,
    TransactionRepository,
    TransactionRepositoryConflictException,
    TransactionStatus,
)
from tests.utils import (
    CONFIG_FILE_PATH,
    FakeAsyncIterator,
    SagaTestCase,
)


class TestSagaService(SagaTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.config = Config(CONFIG_FILE_PATH)

        self.saga_manager = SagaManager.from_config(self.config)
        self.service = SagaService(saga_manager=self.saga_manager)

    def test_get_enroute(self):
        expected = {
            "__reply__": {BrokerCommandEnrouteDecorator("orderReply")},
            "__reserve__": {BrokerCommandEnrouteDecorator("_ReserveOrderTransaction")},
            "__reject__": {BrokerCommandEnrouteDecorator("_RejectOrderTransaction")},
            "__commit__": {BrokerCommandEnrouteDecorator("_CommitOrderTransaction")},
            "__reject_blocked__": {PeriodicEventEnrouteDecorator("* * * * *")},
        }
        observed = SagaService.__get_enroute__(self.config)
        self.assertEqual(expected, observed)

    async def test_reply(self):
        uuid = uuid4()
        with patch("minos.saga.SagaManager.run") as run_mock:
            reply = BrokerMessageV1(
                "orderReply",
                BrokerMessageV1Payload(
                    "foo", headers={"saga": str(uuid), "transactions": str(uuid), "related_services": "ticket,product"}
                ),
            )
            response = await self.service.__reply__(BrokerRequest(reply))
        self.assertEqual(None, response)
        self.assertEqual(
            [
                call(
                    response=SagaResponse("foo", {"ticket", "product"}, SagaResponseStatus.SUCCESS, uuid),
                    pause_on_disk=True,
                    raise_on_error=False,
                    return_execution=False,
                )
            ],
            run_mock.call_args_list,
        )

    async def test_reserve(self):
        uuid = uuid4()
        with patch.object(TransactionEntry, "reserve") as reserve_mock:
            with patch.object(TransactionRepository, "get", return_value=TransactionEntry(uuid)) as get_mock:
                response = await self.service.__reserve__(InMemoryRequest(uuid))
        self.assertEqual([call(uuid)], get_mock.call_args_list)
        self.assertEqual([call()], reserve_mock.call_args_list)
        self.assertEqual(None, response)

    async def test_reserve_raises(self):
        with patch.object(TransactionRepository, "get", side_effect=TransactionNotFoundException("")):
            with self.assertRaises(ResponseException):
                await self.service.__reserve__(InMemoryRequest(None))

        with patch.object(TransactionRepository, "get", return_value=TransactionEntry()):
            with patch.object(TransactionEntry, "reserve", side_effect=TransactionRepositoryConflictException("")):
                with self.assertRaises(ResponseException):
                    await self.service.__reserve__(InMemoryRequest(None))

    async def test_reject(self):
        uuid = uuid4()
        with patch.object(TransactionEntry, "reject") as reject_mock:
            with patch.object(TransactionRepository, "get", return_value=TransactionEntry(uuid)) as get_mock:
                response = await self.service.__reject__(InMemoryRequest(uuid))
        self.assertEqual([call(uuid)], get_mock.call_args_list)
        self.assertEqual([call()], reject_mock.call_args_list)
        self.assertEqual(None, response)

    async def test_reject_already(self):
        uuid = uuid4()
        with patch.object(TransactionEntry, "reject") as reject_mock:
            with patch.object(
                TransactionRepository,
                "get",
                return_value=TransactionEntry(uuid, status=TransactionStatus.REJECTED),
            ) as get_mock:
                response = await self.service.__reject__(InMemoryRequest(uuid))
        self.assertEqual([call(uuid)], get_mock.call_args_list)
        self.assertEqual([], reject_mock.call_args_list)
        self.assertEqual(None, response)

    async def test_reject_raises(self):
        with patch.object(TransactionRepository, "get", side_effect=TransactionNotFoundException("")):
            with self.assertRaises(ResponseException):
                await self.service.__reject__(InMemoryRequest(None))

        with patch.object(TransactionRepository, "get", return_value=TransactionEntry()):
            with patch.object(TransactionEntry, "reject", side_effect=TransactionRepositoryConflictException("")):
                with self.assertRaises(ResponseException):
                    await self.service.__reject__(InMemoryRequest(None))

    async def test_commit(self):
        uuid = uuid4()
        with patch.object(TransactionEntry, "commit") as commit_mock:
            with patch.object(TransactionRepository, "get", return_value=TransactionEntry(uuid)) as get_mock:
                response = await self.service.__commit__(InMemoryRequest(uuid))
        self.assertEqual([call(uuid)], get_mock.call_args_list)
        self.assertEqual([call()], commit_mock.call_args_list)
        self.assertEqual(None, response)

    async def test_commit_raises(self):
        with patch.object(TransactionRepository, "get", side_effect=TransactionNotFoundException("")):
            with self.assertRaises(ResponseException):
                await self.service.__commit__(InMemoryRequest(None))

        with patch.object(TransactionRepository, "get", return_value=TransactionEntry()):
            with patch.object(TransactionEntry, "commit", side_effect=TransactionRepositoryConflictException("")):
                with self.assertRaises(ResponseException):
                    await self.service.__commit__(InMemoryRequest(None))

    async def test_reject_blocked(self):
        uuid = uuid4()
        with patch.object(TransactionEntry, "reject") as reject_mock:
            with patch.object(
                TransactionRepository, "select", return_value=FakeAsyncIterator([TransactionEntry(uuid)])
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
        with patch.object(
            TransactionEntry, "reject", side_effect=[None, TransactionRepositoryConflictException("")]
        ) as reject_mock:
            with patch.object(
                TransactionRepository,
                "select",
                return_value=FakeAsyncIterator([TransactionEntry(uuid), TransactionEntry(uuid)]),
            ):
                response = await self.service.__reject_blocked__(InMemoryRequest(uuid))

        self.assertEqual([call(), call()], reject_mock.call_args_list)
        self.assertEqual(None, response)


if __name__ == "__main__":
    unittest.main()

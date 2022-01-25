import unittest
import warnings
from shutil import (
    rmtree,
)
from unittest.mock import (
    AsyncMock,
    call,
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    NotProvidedException,
)
from minos.networks import (
    REQUEST_HEADERS_CONTEXT_VAR,
    REQUEST_USER_CONTEXT_VAR,
    BrokerMessageV1,
    BrokerMessageV1Payload,
)
from minos.saga import (
    SagaContext,
    SagaExecution,
    SagaExecutionNotFoundException,
    SagaExecutionStorage,
    SagaFailedExecutionException,
    SagaManager,
    SagaResponse,
    SagaStatus,
)
from tests.utils import (
    ADD_ORDER,
    BASE_PATH,
    DELETE_ORDER,
    Foo,
    MinosTestCase,
)


class TestSagaManager(MinosTestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()

        self.uuid = UUID("a74d9d6d-290a-492e-afcc-70607958f65d")

        self.receive_messages = [
            BrokerMessageV1(
                "topicReply",
                BrokerMessageV1Payload(
                    [Foo("foo")],
                    headers={"saga": str(self.uuid), "transactions": str(self.uuid), "related_services": "foo"},
                ),
            ),
            BrokerMessageV1(
                "topicReply",
                BrokerMessageV1Payload(
                    [Foo("foo")],
                    headers={"saga": str(self.uuid), "transactions": str(self.uuid), "related_services": "foo"},
                ),
            ),
        ]

        self.user = uuid4()
        REQUEST_USER_CONTEXT_VAR.set(self.user)
        self.manager: SagaManager = SagaManager.from_config(self.config)

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)
        super().tearDown()

    def test_constructor(self):
        self.assertIsInstance(self.manager.storage, SagaExecutionStorage)
        self.assertIsInstance(self.manager, SagaManager)

    def test_constructor_without_handler(self):
        with self.assertRaises(NotProvidedException):
            SagaManager.from_config(self.config, broker_pool=None)

    async def test_context_manager(self):
        async with self.manager as saga_manager:
            self.assertIsInstance(saga_manager, SagaManager)

    async def test_run_with_pause_on_memory(self):
        with patch("uuid.uuid4", return_value=self.uuid):
            self.broker_subscriber_builder.with_messages(
                [
                    *self.receive_messages,
                    BrokerMessageV1("", BrokerMessageV1Payload(None, headers={"related_services": "order"})),
                    BrokerMessageV1("", BrokerMessageV1Payload(None, headers={"related_services": "foo"})),
                    BrokerMessageV1("", BrokerMessageV1Payload(None, headers={"related_services": "order"})),
                    BrokerMessageV1("", BrokerMessageV1Payload(None, headers={"related_services": "foo"})),
                ]
            )

            execution = await self.manager.run(ADD_ORDER)
            self.assertEqual(SagaStatus.Finished, execution.status)
            with self.assertRaises(SagaExecutionNotFoundException):
                self.manager.storage.load(execution.uuid)

        observed = self.broker_publisher.messages
        expected = [
            BrokerMessageV1(
                topic="CreateOrder",
                payload=BrokerMessageV1Payload(
                    Foo("create_order!"),
                    headers={"saga": str(self.uuid), "transactions": str(self.uuid), "user": str(self.user)},
                ),
                identifier=observed[0].identifier,
                reply_topic=observed[0].reply_topic,
            ),
            BrokerMessageV1(
                topic="CreateTicket",
                payload=BrokerMessageV1Payload(
                    Foo("create_ticket!"),
                    headers={"saga": str(self.uuid), "transactions": str(self.uuid), "user": str(self.user)},
                ),
                identifier=observed[1].identifier,
                reply_topic=observed[1].reply_topic,
            ),
            BrokerMessageV1(
                topic="ReserveFooTransaction",
                payload=BrokerMessageV1Payload(execution.uuid),
                identifier=observed[2].identifier,
                reply_topic=observed[2].reply_topic,
            ),
            BrokerMessageV1(
                topic="ReserveOrderTransaction",
                payload=BrokerMessageV1Payload(execution.uuid),
                identifier=observed[3].identifier,
                reply_topic=observed[3].reply_topic,
            ),
            BrokerMessageV1(
                topic="CommitFooTransaction",
                payload=BrokerMessageV1Payload(execution.uuid),
                identifier=observed[4].identifier,
            ),
            BrokerMessageV1(
                topic="CommitOrderTransaction",
                payload=BrokerMessageV1Payload(execution.uuid),
                identifier=observed[5].identifier,
            ),
        ]
        self.assertEqual(expected, observed)

    async def test_run_with_pause_on_memory_without_commit(self):
        with patch("uuid.uuid4", return_value=self.uuid):
            self.broker_subscriber_builder.with_messages(self.receive_messages)

            with patch("minos.saga.SagaExecution.commit") as commit_mock:
                execution = await self.manager.run(ADD_ORDER, autocommit=False)
            self.assertEqual(SagaStatus.Finished, execution.status)

        self.assertEqual(0, commit_mock.call_count)

    async def test_run_with_pause_on_memory_with_headers(self):
        with patch("uuid.uuid4", return_value=self.uuid):
            self.broker_subscriber_builder.with_messages(self.receive_messages)

            request_headers = {"hello": "world"}
            REQUEST_HEADERS_CONTEXT_VAR.set(request_headers)
            await self.manager.run(ADD_ORDER, autocommit=False)
            self.assertEqual({"hello", "related_services"}, request_headers.keys())
            self.assertEqual("world", request_headers["hello"])
            self.assertEqual({"foo", "order"}, set(request_headers["related_services"].split(",")))

    async def test_run_with_pause_on_memory_with_headers_already_related_services(self):
        with patch("uuid.uuid4", return_value=self.uuid):
            self.broker_subscriber_builder.with_messages(self.receive_messages)

            request_headers = {"hello": "world", "related_services": "order,one"}
            REQUEST_HEADERS_CONTEXT_VAR.set(request_headers)
            await self.manager.run(ADD_ORDER, autocommit=False)
            self.assertEqual({"hello", "related_services"}, request_headers.keys())
            self.assertEqual("world", request_headers["hello"])
            self.assertEqual({"foo", "one", "order"}, set(request_headers["related_services"].split(",")))

    async def test_run_with_pause_on_memory_with_error(self):
        with patch("minos.networks.BrokerClient.receive", side_effect=ValueError):
            with patch("minos.saga.SagaExecution.reject") as reject_mock:
                execution = await self.manager.run(ADD_ORDER, raise_on_error=False)
            self.assertEqual(SagaStatus.Errored, execution.status)
            self.assertEqual([call()], reject_mock.call_args_list)

    async def test_run_with_pause_on_memory_without_autocommit(self):
        with patch("minos.networks.BrokerClient.receive", side_effect=ValueError):
            with patch("minos.saga.SagaExecution.reject") as reject_mock:
                execution = await self.manager.run(ADD_ORDER, autocommit=False, raise_on_error=False)
            self.assertEqual(SagaStatus.Errored, execution.status)
            self.assertEqual(0, reject_mock.call_count)

    async def test_run_with_pause_on_memory_with_error_raises(self):
        with patch("minos.networks.BrokerClient.receive", side_effect=ValueError):
            with self.assertRaises(SagaFailedExecutionException):
                await self.manager.run(ADD_ORDER)

    async def test_run_with_pause_on_disk(self):
        self.broker_subscriber_builder.with_messages(
            [BrokerMessageV1("", BrokerMessageV1Payload()), BrokerMessageV1("", BrokerMessageV1Payload())]
        )

        execution = await self.manager.run(ADD_ORDER, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse([Foo("foo")], {"foo"}, uuid=execution.uuid)
        execution = await self.manager.run(response=response, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse([Foo("foo")], {"foo"}, uuid=execution.uuid)
        execution = await self.manager.run(response=response, pause_on_disk=True)
        with self.assertRaises(SagaExecutionNotFoundException):
            self.manager.storage.load(execution.uuid)

        observed = self.broker_publisher.messages
        expected = [
            BrokerMessageV1(
                topic="CreateOrder",
                payload=BrokerMessageV1Payload(
                    Foo("create_order!"),
                    headers={"saga": str(execution.uuid), "transactions": str(execution.uuid), "user": str(self.user)},
                ),
                identifier=observed[0].identifier,
                reply_topic=observed[0].reply_topic,
            ),
            BrokerMessageV1(
                topic="CreateTicket",
                payload=BrokerMessageV1Payload(
                    Foo("create_ticket!"),
                    headers={"saga": str(execution.uuid), "transactions": str(execution.uuid), "user": str(self.user)},
                ),
                identifier=observed[1].identifier,
                reply_topic=observed[1].reply_topic,
            ),
            BrokerMessageV1(
                topic="ReserveFooTransaction",
                payload=BrokerMessageV1Payload(execution.uuid),
                identifier=observed[2].identifier,
                reply_topic=observed[2].reply_topic,
            ),
            BrokerMessageV1(
                topic="ReserveOrderTransaction",
                payload=BrokerMessageV1Payload(execution.uuid),
                identifier=observed[3].identifier,
                reply_topic=observed[3].reply_topic,
            ),
            BrokerMessageV1(
                topic="CommitFooTransaction",
                payload=BrokerMessageV1Payload(execution.uuid),
                identifier=observed[4].identifier,
            ),
            BrokerMessageV1(
                topic="CommitOrderTransaction",
                payload=BrokerMessageV1Payload(execution.uuid),
                identifier=observed[5].identifier,
            ),
        ]
        self.assertEqual(expected, observed)

    async def test_run_with_pause_on_disk_without_commit(self):
        self.broker_subscriber_builder.with_messages([BrokerMessageV1("", BrokerMessageV1Payload())])

        execution = await self.manager.run(ADD_ORDER, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse([Foo("foo")], {"foo"}, uuid=execution.uuid)
        execution = await self.manager.run(response=response, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse([Foo("foo")], {"foo"}, uuid=execution.uuid)

        with patch("minos.saga.SagaExecution.commit") as commit_mock:
            execution = await self.manager.run(response=response, pause_on_disk=True, autocommit=False)
        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(0, commit_mock.call_count)

    async def test_run_with_pause_on_disk_with_headers(self):
        self.broker_subscriber_builder.with_messages(
            [
                BrokerMessageV1("", BrokerMessageV1Payload()),
                BrokerMessageV1("", BrokerMessageV1Payload()),
                BrokerMessageV1("", BrokerMessageV1Payload()),
            ]
        )
        request_headers = {"related_services": "one"}
        REQUEST_HEADERS_CONTEXT_VAR.set(request_headers)
        execution = await self.manager.run(ADD_ORDER, pause_on_disk=True)
        self.assertEqual({"one", "order"}, set(request_headers["related_services"].split(",")))

        request_headers = {"related_services": "one"}
        REQUEST_HEADERS_CONTEXT_VAR.set(request_headers)
        response = SagaResponse([Foo("foo")], {"foo"}, uuid=execution.uuid)
        execution = await self.manager.run(response=response, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)
        self.assertEqual({"foo", "one", "order"}, set(request_headers["related_services"].split(",")))

        request_headers = {"related_services": "one"}
        REQUEST_HEADERS_CONTEXT_VAR.set(request_headers)
        response = SagaResponse([Foo("foo")], {"foo", "bar"}, uuid=execution.uuid)
        await self.manager.run(response=response, pause_on_disk=True)
        self.assertEqual({"foo", "bar", "one", "order"}, set(request_headers["related_services"].split(",")))

    async def test_run_with_pause_on_disk_returning_uuid(self):
        uuid = await self.manager.run(ADD_ORDER, return_execution=False, pause_on_disk=True)
        execution = self.manager.storage.load(uuid)
        self.assertIsInstance(execution, SagaExecution)
        self.assertEqual(SagaStatus.Paused, execution.status)

    async def test_run_with_pause_on_disk_with_context(self):
        context = SagaContext(foo=Foo("foo"), one=1, a="a")

        execution = await self.manager.run(ADD_ORDER, context=context, pause_on_disk=True)
        self.assertEqual(context, execution.context)

    async def test_run_with_pause_on_disk_with_error(self):
        self.broker_publisher.send = AsyncMock(side_effect=ValueError)

        with patch("minos.saga.SagaExecution.reject") as reject_mock:
            execution = await self.manager.run(DELETE_ORDER, pause_on_disk=True, raise_on_error=False)

        self.assertEqual(SagaStatus.Errored, execution.status)
        self.assertEqual([call()], reject_mock.call_args_list)

    async def test_run_with_pause_on_disk_without_autocommit(self):
        self.broker_publisher.send = AsyncMock(side_effect=ValueError)

        with patch("minos.saga.SagaExecution.reject") as reject_mock:
            execution = await self.manager.run(DELETE_ORDER, pause_on_disk=True, raise_on_error=False, autocommit=False)
        self.assertEqual(SagaStatus.Errored, execution.status)
        self.assertEqual(0, reject_mock.call_count)

    async def test_run_with_user_context_var(self):
        saga_manager = SagaManager.from_config(self.config)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            # noinspection PyUnresolvedReferences
            await saga_manager.run(ADD_ORDER, user=uuid4(), pause_on_disk=True)

        observed = self.broker_publisher.messages
        self.assertEqual(1, len(observed))

        self.assertEqual(str(self.user), observed[0].headers["user"])


if __name__ == "__main__":
    unittest.main()

import unittest
import warnings
from collections import (
    namedtuple,
)
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
    USER_CONTEXT_VAR,
    BrokerMessage,
    BrokerMessageStatus,
)
from minos.saga import (
    SagaContext,
    SagaExecution,
    SagaExecutionNotFoundException,
    SagaExecutionStorage,
    SagaFailedExecutionException,
    SagaManager,
    SagaResponse,
    SagaResponseStatus,
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

        self.user = uuid4()
        USER_CONTEXT_VAR.set(self.user)
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
        send_mock = AsyncMock()
        self.broker_publisher.send = send_mock

        Message = namedtuple("Message", ["data"])
        expected_uuid = UUID("a74d9d6d-290a-492e-afcc-70607958f65d")
        with patch("uuid.uuid4", return_value=expected_uuid):
            self.broker.get_one = AsyncMock(
                side_effect=[
                    Message(
                        BrokerMessage(
                            "topicReply",
                            [Foo("foo")],
                            "foo",
                            status=BrokerMessageStatus.SUCCESS,
                            headers={"saga": str(expected_uuid), "transaction": str(expected_uuid)},
                        )
                    ),
                    Message(
                        BrokerMessage(
                            "topicReply",
                            [Foo("foo")],
                            "foo",
                            status=BrokerMessageStatus.SUCCESS,
                            headers={"saga": str(expected_uuid), "transaction": str(expected_uuid)},
                        )
                    ),
                    Message(BrokerMessage("", None, "foo", status=BrokerMessageStatus.SUCCESS)),
                    Message(BrokerMessage("", None, "order", status=BrokerMessageStatus.SUCCESS)),
                    Message(BrokerMessage("", None, "foo", status=BrokerMessageStatus.SUCCESS)),
                    Message(BrokerMessage("", None, "order", status=BrokerMessageStatus.SUCCESS)),
                ]
            )

            execution = await self.manager.run(ADD_ORDER)
            self.assertEqual(SagaStatus.Finished, execution.status)
            with self.assertRaises(SagaExecutionNotFoundException):
                self.manager.storage.load(execution.uuid)

        self.assertEqual(
            [
                call(
                    topic="CreateOrder",
                    data=Foo("create_order!"),
                    headers={"saga": str(expected_uuid), "transaction": str(expected_uuid)},
                    user=self.user,
                    reply_topic="TheReplyTopic",
                ),
                call(
                    topic="CreateTicket",
                    data=Foo("create_ticket!"),
                    headers={"saga": str(expected_uuid), "transaction": str(expected_uuid)},
                    user=self.user,
                    reply_topic="TheReplyTopic",
                ),
                call(topic="ReserveFooTransaction", data=execution.uuid, reply_topic="TheReplyTopic"),
                call(topic="ReserveOrderTransaction", data=execution.uuid, reply_topic="TheReplyTopic"),
                call(topic="CommitFooTransaction", data=execution.uuid),
                call(topic="CommitOrderTransaction", data=execution.uuid),
            ],
            send_mock.call_args_list,
        )

    async def test_run_with_pause_on_memory_without_commit(self):
        send_mock = AsyncMock()
        self.broker_publisher.send = send_mock

        Message = namedtuple("Message", ["data"])
        expected_uuid = UUID("a74d9d6d-290a-492e-afcc-70607958f65d")
        with patch("uuid.uuid4", return_value=expected_uuid):
            self.broker.get_one = AsyncMock(
                side_effect=[
                    Message(
                        BrokerMessage(
                            "topicReply",
                            [Foo("foo")],
                            headers={"saga": str(expected_uuid), "transaction": str(expected_uuid)},
                            status=BrokerMessageStatus.SUCCESS,
                            service_name="foo",
                        )
                    ),
                    Message(
                        BrokerMessage(
                            "topicReply",
                            [Foo("foo")],
                            headers={"saga": str(expected_uuid), "transaction": str(expected_uuid)},
                            status=BrokerMessageStatus.SUCCESS,
                            service_name="foo",
                        )
                    ),
                ]
            )

            with patch("minos.saga.SagaExecution.commit") as commit_mock:
                execution = await self.manager.run(ADD_ORDER, autocommit=False)
            self.assertEqual(SagaStatus.Finished, execution.status)

        self.assertEqual(0, commit_mock.call_count)

    async def test_run_with_pause_on_memory_with_error(self):
        self.broker.get_one = AsyncMock(side_effect=ValueError)

        with patch("minos.saga.SagaExecution.reject") as reject_mock:
            execution = await self.manager.run(ADD_ORDER, raise_on_error=False)
        self.assertEqual(SagaStatus.Errored, execution.status)
        self.assertEqual([call()], reject_mock.call_args_list)

    async def test_run_with_pause_on_memory_without_autocommit(self):
        self.broker.get_one = AsyncMock(side_effect=ValueError)

        with patch("minos.saga.SagaExecution.reject") as reject_mock:
            execution = await self.manager.run(ADD_ORDER, autocommit=False, raise_on_error=False)
        self.assertEqual(SagaStatus.Errored, execution.status)
        self.assertEqual(0, reject_mock.call_count)

    async def test_run_with_pause_on_memory_with_error_raises(self):
        self.broker.get_one = AsyncMock(side_effect=ValueError)

        with self.assertRaises(SagaFailedExecutionException):
            await self.manager.run(ADD_ORDER)

    async def test_run_with_pause_on_disk(self):
        send_mock = AsyncMock()
        self.broker_publisher.send = send_mock

        get_mock = AsyncMock()
        get_mock.return_value.data.ok = True
        self.broker.get_one = get_mock

        execution = await self.manager.run(ADD_ORDER, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(
            [Foo("foo")], uuid=execution.uuid, status=SagaResponseStatus.SUCCESS, service_name="foo"
        )
        execution = await self.manager.run(response=response, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(
            [Foo("foo")], uuid=execution.uuid, status=SagaResponseStatus.SUCCESS, service_name="foo"
        )
        execution = await self.manager.run(response=response, pause_on_disk=True)
        with self.assertRaises(SagaExecutionNotFoundException):
            self.manager.storage.load(execution.uuid)

        self.assertEqual(
            [
                call(
                    topic="CreateOrder",
                    data=Foo("create_order!"),
                    headers={"saga": str(execution.uuid), "transaction": str(execution.uuid)},
                    user=self.user,
                    reply_topic="orderReply",
                ),
                call(
                    topic="CreateTicket",
                    data=Foo("create_ticket!"),
                    headers={"saga": str(execution.uuid), "transaction": str(execution.uuid)},
                    user=self.user,
                    reply_topic="orderReply",
                ),
                call(topic="ReserveFooTransaction", data=execution.uuid, reply_topic="TheReplyTopic"),
                call(topic="ReserveOrderTransaction", data=execution.uuid, reply_topic="TheReplyTopic"),
                call(topic="CommitFooTransaction", data=execution.uuid),
                call(topic="CommitOrderTransaction", data=execution.uuid),
            ],
            send_mock.call_args_list,
        )

    async def test_run_with_pause_on_disk_without_commit(self):
        send_mock = AsyncMock()
        self.broker_publisher.send = send_mock

        get_mock = AsyncMock()
        get_mock.return_value.data.ok = True
        self.broker.get_one = get_mock

        execution = await self.manager.run(ADD_ORDER, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(
            [Foo("foo")], uuid=execution.uuid, status=SagaResponseStatus.SUCCESS, service_name="foo"
        )
        execution = await self.manager.run(response=response, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(
            [Foo("foo")], uuid=execution.uuid, status=SagaResponseStatus.SUCCESS, service_name="foo"
        )

        with patch("minos.saga.SagaExecution.commit") as commit_mock:
            execution = await self.manager.run(response=response, pause_on_disk=True, autocommit=False)
        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(0, commit_mock.call_count)

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
        send_mock = AsyncMock()
        self.broker_publisher.send = send_mock

        saga_manager = SagaManager.from_config(self.config)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            # noinspection PyUnresolvedReferences
            await saga_manager.run(ADD_ORDER, user=uuid4(), pause_on_disk=True)

        self.assertEqual(self.user, send_mock.call_args.kwargs["user"])


if __name__ == "__main__":
    unittest.main()

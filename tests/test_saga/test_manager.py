import unittest
import warnings
from collections import (
    namedtuple,
)
from contextvars import (
    ContextVar,
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
    MinosConfig,
    NotProvidedException,
)
from minos.networks import (
    CommandReply,
    CommandStatus,
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
    FakeHandler,
    FakePool,
    Foo,
    MinosTestCase,
)

_USER_CONTEXT_VAR = ContextVar("user")


class TestSagaManager(MinosTestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.config = MinosConfig(BASE_PATH / "config.yml")
        self.handler = FakeHandler("TheReplyTopic")
        self.pool = FakePool(self.handler)
        self.user = uuid4()
        _USER_CONTEXT_VAR.set(self.user)
        # noinspection PyTypeChecker
        self.manager: SagaManager = SagaManager.from_config(self.config, dynamic_handler_pool=self.pool)

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)
        super().tearDown()

    def test_constructor(self):
        self.assertIsInstance(self.manager.storage, SagaExecutionStorage)
        self.assertIsInstance(self.manager, SagaManager)

    def test_constructor_without_handler(self):
        with self.assertRaises(NotProvidedException):
            SagaManager.from_config(self.config, handler=None)

    def test_from_config_with_user_context_var(self):
        _USER_CONTEXT_VAR.set(None)
        saga_manager = SagaManager.from_config(self.config, dynamic_handler_pool=self.pool)

        # noinspection PyUnresolvedReferences
        self.assertEqual(_USER_CONTEXT_VAR, saga_manager.user_context_var)

    async def test_context_manager(self):
        async with self.manager as saga_manager:
            self.assertIsInstance(saga_manager, SagaManager)

    async def test_run_with_pause_on_memory(self):
        send_mock = AsyncMock()
        self.command_broker.send = send_mock

        Message = namedtuple("Message", ["data"])
        expected_uuid = UUID("a74d9d6d-290a-492e-afcc-70607958f65d")
        with patch("uuid.uuid4", return_value=expected_uuid):
            self.handler.get_one = AsyncMock(
                side_effect=[
                    Message(CommandReply("topicReply", [Foo("foo")], expected_uuid, status=CommandStatus.SUCCESS)),
                    Message(CommandReply("topicReply", [Foo("foo")], expected_uuid, status=CommandStatus.SUCCESS)),
                ]
            )

            execution = await self.manager.run(ADD_ORDER)
            self.assertEqual(SagaStatus.Finished, execution.status)
            with self.assertRaises(SagaExecutionNotFoundException):
                self.manager.storage.load(execution.uuid)

        self.assertEqual(2, send_mock.call_count)
        self.assertEqual(
            [
                call(topic="CreateOrder", data=Foo("create_order!"), saga=expected_uuid, user=self.user,),
                call(topic="CreateTicket", data=Foo("create_ticket!"), saga=expected_uuid, user=self.user,),
            ],
            send_mock.call_args_list,
        )

    async def test_run_with_pause_on_memory_with_error(self):
        self.handler.get_one = AsyncMock(side_effect=ValueError)

        execution = await self.manager.run(ADD_ORDER, raise_on_error=False)
        self.assertEqual(SagaStatus.Errored, execution.status)

    async def test_run_with_pause_on_memory_with_error_raises(self):
        self.handler.get_one = AsyncMock(side_effect=ValueError)

        with self.assertRaises(SagaFailedExecutionException):
            await self.manager.run(ADD_ORDER)

    async def test_run_with_pause_on_disk(self):
        send_mock = AsyncMock()
        self.command_broker.send = send_mock

        execution = await self.manager.run(ADD_ORDER, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse([Foo("foo")], uuid=execution.uuid, status=SagaResponseStatus.SUCCESS)
        execution = await self.manager.run(response=response, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse([Foo("foo")], uuid=execution.uuid, status=SagaResponseStatus.SUCCESS)
        execution = await self.manager.run(response=response, pause_on_disk=True)
        with self.assertRaises(SagaExecutionNotFoundException):
            self.manager.storage.load(execution.uuid)

        self.assertEqual(2, send_mock.call_count)
        self.assertEqual(
            [
                call(topic="CreateOrder", data=Foo("create_order!"), saga=execution.uuid, user=self.user,),
                call(topic="CreateTicket", data=Foo("create_ticket!"), saga=execution.uuid, user=self.user,),
            ],
            send_mock.call_args_list,
        )

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
        execution = await self.manager.run(DELETE_ORDER, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse([Foo("foo")], uuid=execution.uuid, status=SagaResponseStatus.SUCCESS)
        execution = await self.manager.run(response=response, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse([Foo("foo")], uuid=execution.uuid, status=SagaResponseStatus.SUCCESS)
        execution = await self.manager.run(response=response, pause_on_disk=True, raise_on_error=False)
        self.assertEqual(SagaStatus.Errored, execution.status)

    async def test_run_with_user_context_var(self):
        send_mock = AsyncMock()
        self.command_broker.send = send_mock

        saga_manager = SagaManager.from_config(self.config, dynamic_handler_pool=self.pool)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            # noinspection PyUnresolvedReferences
            await saga_manager.run(ADD_ORDER, user=uuid4(), pause_on_disk=True)

        self.assertEqual(self.user, send_mock.call_args.kwargs["user"])


if __name__ == "__main__":
    unittest.main()

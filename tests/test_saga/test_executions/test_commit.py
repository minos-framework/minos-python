import unittest
from unittest.mock import (
    AsyncMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.saga import (
    ConditionalSagaStepExecution,
    LocalSagaStep,
    LocalSagaStepExecution,
    RemoteSagaStepExecution,
    Saga,
    SagaContext,
    SagaExecution,
    TransactionCommitter,
)
from tests.utils import (
    MinosTestCase,
)


class TestTransactionCommitter(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.execution_uuid = uuid4()

        # noinspection PyTypeChecker
        definition = LocalSagaStep(on_execute=LocalSagaStep)
        self.executed_steps = [
            RemoteSagaStepExecution(definition, service_name="foo"),
            LocalSagaStepExecution(definition, service_name="bar"),
            ConditionalSagaStepExecution(
                definition,
                inner=SagaExecution(
                    Saga(steps=[definition], committed=True),
                    self.execution_uuid,
                    SagaContext(),
                    steps=[
                        RemoteSagaStepExecution(definition, service_name="foo"),
                        RemoteSagaStepExecution(definition, service_name="foobar"),
                    ],
                ),
                service_name="bar",
            ),
            ConditionalSagaStepExecution(definition),
        ]

        self.committer = TransactionCommitter(self.execution_uuid, self.executed_steps)

    def test_transactions(self):
        expected = [
            (self.execution_uuid, "foo"),
            (self.execution_uuid, "bar"),
            (self.execution_uuid, "foobar"),
        ]
        self.assertEqual(expected, self.committer.transactions)

    async def test_commit_true(self):
        get_mock = AsyncMock()
        get_mock.return_value.data.ok = True
        self.broker.get_one = get_mock

        send_mock = AsyncMock()
        self.broker_publisher.send = send_mock

        await self.committer.commit()

        self.assertEqual(
            [
                call(data=self.execution_uuid, topic="ReserveFooTransaction", reply_topic="TheReplyTopic"),
                call(data=self.execution_uuid, topic="ReserveBarTransaction", reply_topic="TheReplyTopic"),
                call(data=self.execution_uuid, topic="ReserveFoobarTransaction", reply_topic="TheReplyTopic"),
                call(data=self.execution_uuid, topic="CommitFooTransaction"),
                call(data=self.execution_uuid, topic="CommitBarTransaction"),
                call(data=self.execution_uuid, topic="CommitFoobarTransaction"),
            ],
            send_mock.call_args_list,
        )

    async def test_commit_false(self):
        get_mock = AsyncMock()
        get_mock.return_value.data.ok = False
        self.broker.get_one = get_mock

        send_mock = AsyncMock()
        self.broker_publisher.send = send_mock

        with self.assertRaises(ValueError):
            await self.committer.commit()

        self.assertEqual(
            [
                call(data=self.execution_uuid, topic="ReserveFooTransaction", reply_topic="TheReplyTopic"),
                call(data=self.execution_uuid, topic="ReserveBarTransaction", reply_topic="TheReplyTopic"),
                call(data=self.execution_uuid, topic="ReserveFoobarTransaction", reply_topic="TheReplyTopic"),
                call(data=self.execution_uuid, topic="RejectFooTransaction"),
                call(data=self.execution_uuid, topic="RejectBarTransaction"),
                call(data=self.execution_uuid, topic="RejectFoobarTransaction"),
            ],
            send_mock.call_args_list,
        )

    async def test_reject(self):
        get_mock = AsyncMock()
        self.broker.get_one = get_mock

        send_mock = AsyncMock()
        self.broker_publisher.send = send_mock

        await self.committer.reject()

        self.assertEqual(
            [
                call(data=self.execution_uuid, topic="RejectFooTransaction"),
                call(data=self.execution_uuid, topic="RejectBarTransaction"),
                call(data=self.execution_uuid, topic="RejectFoobarTransaction"),
            ],
            send_mock.call_args_list,
        )


if __name__ == "__main__":
    unittest.main()

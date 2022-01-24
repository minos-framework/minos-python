import unittest
from uuid import (
    uuid4,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
    BrokerMessageV1Status,
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
            RemoteSagaStepExecution(definition, {"foo"}),
            LocalSagaStepExecution(definition, {"bar"}),
            ConditionalSagaStepExecution(
                definition,
                {"bar"},
                inner=SagaExecution(
                    Saga(steps=[definition], committed=True),
                    self.execution_uuid,
                    SagaContext(),
                    steps=[
                        RemoteSagaStepExecution(definition, {"foo"}),
                        RemoteSagaStepExecution(definition, {"foobar"}),
                    ],
                ),
            ),
            ConditionalSagaStepExecution(definition),
        ]

        self.committer = TransactionCommitter(self.execution_uuid, self.executed_steps)

    def test_transactions(self):
        expected = [
            (self.execution_uuid, "bar"),
            (self.execution_uuid, "foo"),
            (self.execution_uuid, "foobar"),
        ]
        self.assertEqual(expected, self.committer.transactions)

    async def test_commit_true(self):
        self.broker_subscriber_builder.with_messages(
            [
                BrokerMessageV1("", BrokerMessageV1Payload(None)),
                BrokerMessageV1("", BrokerMessageV1Payload(None)),
                BrokerMessageV1("", BrokerMessageV1Payload(None)),
            ]
        )

        await self.committer.commit()
        observed = self.broker_publisher.messages
        expected = [
            BrokerMessageV1(
                payload=BrokerMessageV1Payload(self.execution_uuid),
                topic="ReserveBarTransaction",
                reply_topic=observed[0].reply_topic,
                identifier=observed[0].identifier,
            ),
            BrokerMessageV1(
                payload=BrokerMessageV1Payload(self.execution_uuid),
                topic="ReserveFooTransaction",
                reply_topic=observed[1].reply_topic,
                identifier=observed[1].identifier,
            ),
            BrokerMessageV1(
                payload=BrokerMessageV1Payload(self.execution_uuid),
                topic="ReserveFoobarTransaction",
                reply_topic=observed[2].reply_topic,
                identifier=observed[2].identifier,
            ),
            BrokerMessageV1(
                payload=BrokerMessageV1Payload(self.execution_uuid),
                topic="CommitBarTransaction",
                identifier=observed[3].identifier,
            ),
            BrokerMessageV1(
                payload=BrokerMessageV1Payload(self.execution_uuid),
                topic="CommitFooTransaction",
                identifier=observed[4].identifier,
            ),
            BrokerMessageV1(
                payload=BrokerMessageV1Payload(self.execution_uuid),
                topic="CommitFoobarTransaction",
                identifier=observed[5].identifier,
            ),
        ]

        self.assertEqual(
            expected, observed,
        )

    async def test_commit_false(self):
        self.broker_subscriber_builder.with_messages(
            [
                BrokerMessageV1("", BrokerMessageV1Payload(None, status=BrokerMessageV1Status.ERROR)),
                BrokerMessageV1("", BrokerMessageV1Payload(None)),
                BrokerMessageV1("", BrokerMessageV1Payload(None)),
            ]
        )

        with self.assertRaises(ValueError):
            await self.committer.commit()

        observed = self.broker_publisher.messages
        expected = [
            BrokerMessageV1(
                payload=BrokerMessageV1Payload(self.execution_uuid),
                topic="ReserveBarTransaction",
                reply_topic=observed[0].reply_topic,
                identifier=observed[0].identifier,
            ),
            BrokerMessageV1(
                payload=BrokerMessageV1Payload(self.execution_uuid),
                topic="ReserveFooTransaction",
                reply_topic=observed[1].reply_topic,
                identifier=observed[1].identifier,
            ),
            BrokerMessageV1(
                payload=BrokerMessageV1Payload(self.execution_uuid),
                topic="ReserveFoobarTransaction",
                reply_topic=observed[2].reply_topic,
                identifier=observed[2].identifier,
            ),
            BrokerMessageV1(
                payload=BrokerMessageV1Payload(self.execution_uuid),
                topic="RejectBarTransaction",
                identifier=observed[3].identifier,
            ),
            BrokerMessageV1(
                payload=BrokerMessageV1Payload(self.execution_uuid),
                topic="RejectFooTransaction",
                identifier=observed[4].identifier,
            ),
            BrokerMessageV1(
                payload=BrokerMessageV1Payload(self.execution_uuid),
                topic="RejectFoobarTransaction",
                identifier=observed[5].identifier,
            ),
        ]
        self.assertEqual(expected, observed)

    async def test_reject(self):
        await self.committer.reject()

        observed = self.broker_publisher.messages
        expected = [
            BrokerMessageV1(
                topic="RejectBarTransaction",
                payload=BrokerMessageV1Payload(self.execution_uuid),
                identifier=observed[0].identifier,
            ),
            BrokerMessageV1(
                topic="RejectFooTransaction",
                payload=BrokerMessageV1Payload(self.execution_uuid),
                identifier=observed[1].identifier,
            ),
            BrokerMessageV1(
                topic="RejectFoobarTransaction",
                payload=BrokerMessageV1Payload(self.execution_uuid),
                identifier=observed[2].identifier,
            ),
        ]
        self.assertEqual(
            expected, observed,
        )


if __name__ == "__main__":
    unittest.main()

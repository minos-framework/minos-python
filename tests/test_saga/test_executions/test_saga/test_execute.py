import unittest
from unittest.mock import (
    patch,
)
from uuid import (
    uuid4,
)

from minos.networks import (
    BrokerMessageV1,
    BrokerMessageV1Payload,
)
from minos.saga import (
    Saga,
    SagaContext,
    SagaExecution,
    SagaExecutionAlreadyExecutedException,
    SagaFailedCommitCallbackException,
    SagaFailedExecutionStepException,
    SagaPausedExecutionStepException,
    SagaResponse,
    SagaRollbackExecutionException,
    SagaStatus,
)
from tests.utils import (
    Foo,
    MinosTestCase,
    handle_order_success,
    handle_ticket_success,
    handle_ticket_success_raises,
    send_create_order,
    send_create_ticket,
    send_delete_order,
    send_delete_ticket,
)


class TestSagaExecution(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()

    async def test_execute(self):
        self.broker_subscriber_builder.with_messages(
            [BrokerMessageV1("", BrokerMessageV1Payload()), BrokerMessageV1("", BrokerMessageV1Payload())]
        )
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_ticket)
            .on_success(handle_ticket_success)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        self.assertEqual(1, len(self.broker_publisher.messages))
        self.broker_publisher.messages.clear()

        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("order"), {"ticket"})
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)
        self.assertEqual(1, len(self.broker_publisher.messages))
        self.broker_publisher.messages.clear()
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("ticket"), {"ticket"})
        context = await execution.execute(response)
        self.assertEqual(4, len(self.broker_publisher.messages))
        self.broker_publisher.messages.clear()
        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(SagaContext(order=Foo("order"), ticket=Foo("ticket")), context)

        with self.assertRaises(SagaExecutionAlreadyExecutedException):
            await execution.execute()

    async def test_execute_and_publish(self):
        user = uuid4()

        definition = Saga().remote_step(send_create_order).commit()
        execution = SagaExecution.from_definition(definition, user=user)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()

        observed = self.broker_publisher.messages
        expected = [
            BrokerMessageV1(
                topic="CreateOrder",
                payload=BrokerMessageV1Payload(
                    Foo(foo="create_order!"),
                    headers={"saga": str(execution.uuid), "transactions": str(execution.uuid), "user": str(user)},
                ),
                reply_topic=observed[0].reply_topic,
                identifier=observed[0].identifier,
            )
        ]
        self.assertEqual(expected, observed)
        self.assertEqual(SagaStatus.Paused, execution.status)

    async def test_execute_failure(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_ticket)
            .on_success(handle_ticket_success_raises)
            .on_failure(send_delete_ticket)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        self.assertEqual(1, len(self.broker_publisher.messages))
        self.broker_publisher.messages.clear()
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("order"), {"ticket"})
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)
        self.assertEqual(1, len(self.broker_publisher.messages))
        self.broker_publisher.messages.clear()
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("ticket"), {"ticket"})
        with self.assertRaises(SagaFailedExecutionStepException):
            await execution.execute(response)
        self.assertEqual(4, len(self.broker_publisher.messages))
        self.broker_publisher.messages.clear()

        self.assertEqual(SagaStatus.Errored, execution.status)

        response = SagaResponse(Foo("fixed failure!"), {"ticket"})
        await execution.execute(response)
        self.assertEqual(0, len(self.broker_publisher.messages))

        with self.assertRaises(SagaExecutionAlreadyExecutedException):
            await execution.execute()

    async def test_execute_commit(self):  # FIXME: This test must be rewritten according to transactions integration
        self.broker_subscriber_builder.with_messages(
            [BrokerMessageV1("", BrokerMessageV1Payload()), BrokerMessageV1("", BrokerMessageV1Payload())]
        )

        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_ticket)
            .on_success(handle_ticket_success)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("order"), {"ticket"})
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("ticket"), {"ticket"})
        self.broker_publisher.messages.clear()
        context = await execution.execute(response)

        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(SagaContext(order=Foo("order"), ticket=Foo("ticket")), context)
        self.assertEqual(4, len(self.broker_publisher.messages))

    async def test_execute_commit_without_autocommit(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_ticket)
            .on_success(handle_ticket_success)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("order"), {"ticket"})
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("ticket"), {"ticket"})
        self.broker_publisher.messages.clear()
        context = await execution.execute(response, autocommit=False)

        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(SagaContext(order=Foo("order"), ticket=Foo("ticket")), context)
        self.assertEqual(0, len(self.broker_publisher.messages))

    async def test_execute_commit_raises(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_ticket)
            .on_success(handle_ticket_success)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("order1"), {"ticket"})
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)
        self.assertEqual(SagaStatus.Paused, execution.status)

        self.broker_publisher.messages.clear()
        response = SagaResponse(Foo("order2"), {"ticket"})
        with patch("minos.saga.TransactionCommitter.commit", side_effect=ValueError):
            with self.assertRaises(SagaFailedCommitCallbackException):
                await execution.execute(response)

        self.assertEqual(SagaStatus.Errored, execution.status)
        self.assertEqual(1, len(self.broker_publisher.messages))

    async def test_commit_raises(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_ticket)
            .on_success(handle_ticket_success)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)

        with self.assertRaises(ValueError):
            await execution.commit()

    async def test_rollback(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_order)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        response = SagaResponse(Foo("order1"), {"ticket"})
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)

        self.broker_publisher.messages.clear()
        await execution.rollback()
        self.assertEqual(3, len(self.broker_publisher.messages))
        self.broker_publisher.messages.clear()

        with self.assertRaises(SagaRollbackExecutionException):
            await execution.rollback()
        self.assertEqual(0, len(self.broker_publisher.messages))
        self.broker_publisher.messages.clear()

    async def test_rollback_without_autoreject(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_order)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        response = SagaResponse(Foo("order1"), {"ticket"})
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)

        self.broker_publisher.messages.clear()
        await execution.rollback(autoreject=False)
        self.assertEqual(1, len(self.broker_publisher.messages))

    async def test_rollback_raises(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_order)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        response = SagaResponse(Foo("order1"), {"ticket"})
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)

        with patch("minos.networks.InMemoryBrokerPublisher.send", side_effect=ValueError):
            with self.assertRaises(SagaRollbackExecutionException):
                await execution.rollback()


if __name__ == "__main__":
    unittest.main()

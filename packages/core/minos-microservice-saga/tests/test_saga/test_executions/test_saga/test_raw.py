import unittest
from unittest.mock import (
    MagicMock,
    patch,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.saga import (
    SagaContext,
    SagaExecution,
    SagaPausedExecutionStepException,
    SagaResponse,
)
from tests.utils import (
    ADD_ORDER,
    Foo,
    SagaTestCase,
)


class TestSagaExecution(SagaTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.user = uuid4()
        self.publish_mock = MagicMock(side_effect=self.broker_publisher.send)
        self.broker_publisher.send = self.publish_mock

    def test_from_raw(self):
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_definition(ADD_ORDER, user=self.user)
        observed = SagaExecution.from_raw(expected)
        self.assertEqual(expected, observed)

    def test_from_raw_without_user(self):
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_definition(ADD_ORDER)
        observed = SagaExecution.from_raw(expected)
        self.assertEqual(expected, observed)

    def test_created(self):
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            execution = SagaExecution.from_definition(ADD_ORDER, user=self.user)

        expected = {
            "already_rollback": False,
            "context": SagaContext().avro_str,
            "definition": {
                "committed": True,
                "steps": [
                    {
                        "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                        "order": None,
                        "on_execute": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.DeleteOrderSaga.handle_order_success"},
                        "on_error": None,
                        "on_failure": {"callback": "tests.utils.DeleteOrderSaga.send_delete_order"},
                    },
                    {
                        "cls": "minos.saga.definitions.steps.local.LocalSagaStep",
                        "order": None,
                        "on_execute": {"callback": "tests.utils.create_payment"},
                        "on_failure": {"callback": "tests.utils.delete_payment"},
                    },
                    {
                        "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                        "order": None,
                        "on_execute": {"callback": "tests.utils.send_create_ticket"},
                        "on_success": {"callback": "tests.utils.handle_ticket_success"},
                        "on_error": {"callback": "tests.utils.handle_ticket_error"},
                        "on_failure": {"callback": "tests.utils.DeleteOrderSaga.send_delete_ticket"},
                    },
                ],
            },
            "executed_steps": [],
            "paused_step": None,
            "status": "created",
            "user": str(self.user),
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }
        observed = execution.raw
        self.assertEqual(
            SagaContext.from_avro_str(expected.pop("context")), SagaContext.from_avro_str(observed.pop("context"))
        )
        self.assertEqual(expected, observed)

    def test_created_without_user(self):
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            execution = SagaExecution.from_definition(ADD_ORDER)

        expected = {
            "already_rollback": False,
            "context": SagaContext().avro_str,
            "definition": {
                "committed": True,
                "steps": [
                    {
                        "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                        "order": None,
                        "on_execute": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.DeleteOrderSaga.handle_order_success"},
                        "on_error": None,
                        "on_failure": {"callback": "tests.utils.DeleteOrderSaga.send_delete_order"},
                    },
                    {
                        "cls": "minos.saga.definitions.steps.local.LocalSagaStep",
                        "order": None,
                        "on_execute": {"callback": "tests.utils.create_payment"},
                        "on_failure": {"callback": "tests.utils.delete_payment"},
                    },
                    {
                        "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                        "order": None,
                        "on_execute": {"callback": "tests.utils.send_create_ticket"},
                        "on_success": {"callback": "tests.utils.handle_ticket_success"},
                        "on_error": {"callback": "tests.utils.handle_ticket_error"},
                        "on_failure": {"callback": "tests.utils.DeleteOrderSaga.send_delete_ticket"},
                    },
                ],
            },
            "executed_steps": [],
            "paused_step": None,
            "status": "created",
            "user": None,
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }
        observed = execution.raw
        self.assertEqual(
            SagaContext.from_avro_str(expected.pop("context")), SagaContext.from_avro_str(observed.pop("context"))
        )
        self.assertEqual(expected, observed)

    async def test_partial_step(self):
        raw = {
            "already_rollback": False,
            "context": SagaContext().avro_str,
            "definition": {
                "committed": True,
                "steps": [
                    {
                        "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                        "on_execute": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.DeleteOrderSaga.handle_order_success"},
                        "on_error": None,
                        "on_failure": {"callback": "tests.utils.DeleteOrderSaga.send_delete_order"},
                    },
                    {
                        "cls": "minos.saga.definitions.steps.local.LocalSagaStep",
                        "on_execute": {"callback": "tests.utils.create_payment"},
                        "on_failure": {"callback": "tests.utils.delete_payment"},
                    },
                    {
                        "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                        "on_execute": {"callback": "tests.utils.send_create_ticket"},
                        "on_success": {"callback": "tests.utils.handle_ticket_success"},
                        "on_error": {"callback": "tests.utils.handle_ticket_error"},
                        "on_failure": {"callback": "tests.utils.DeleteOrderSaga.send_delete_ticket"},
                    },
                ],
            },
            "executed_steps": [],
            "paused_step": {
                "cls": "minos.saga.executions.steps.remote.RemoteSagaStepExecution",
                "definition": {
                    "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                    "on_execute": {"callback": "tests.utils.send_create_order"},
                    "on_success": {"callback": "tests.utils.DeleteOrderSaga.handle_order_success"},
                    "on_error": {"callback": "tests.utils.handle_ticket_error"},
                    "on_failure": {"callback": "tests.utils.DeleteOrderSaga.send_delete_order"},
                },
                "status": "paused-by-on-execute",
                "already_rollback": False,
            },
            "user": str(self.user),
            "status": "paused",
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }

        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_definition(ADD_ORDER, user=self.user)
            with self.assertRaises(SagaPausedExecutionStepException):
                await expected.execute()

        observed = SagaExecution.from_raw(raw)
        self.assertEqual(expected, observed)

    async def test_executed_step(self):
        raw = {
            "already_rollback": False,
            "context": SagaContext(order=Foo("hola"), payment="payment").avro_str,
            "definition": {
                "committed": True,
                "steps": [
                    {
                        "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                        "on_execute": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.DeleteOrderSaga.handle_order_success"},
                        "on_error": None,
                        "on_failure": {"callback": "tests.utils.DeleteOrderSaga.send_delete_order"},
                    },
                    {
                        "cls": "minos.saga.definitions.steps.local.LocalSagaStep",
                        "on_execute": {"callback": "tests.utils.create_payment"},
                        "on_failure": {"callback": "tests.utils.delete_payment"},
                    },
                    {
                        "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                        "on_execute": {"callback": "tests.utils.send_create_ticket"},
                        "on_success": {"callback": "tests.utils.handle_ticket_success"},
                        "on_error": {"callback": "tests.utils.handle_ticket_error"},
                        "on_failure": {"callback": "tests.utils.DeleteOrderSaga.send_delete_ticket"},
                    },
                ],
            },
            "executed_steps": [
                {
                    "cls": "minos.saga.executions.steps.remote.RemoteSagaStepExecution",
                    "definition": {
                        "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                        "on_execute": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.DeleteOrderSaga.DeleteOrderSaga"},
                        "on_error": None,
                        "on_failure": {"callback": "tests.utils.DeleteOrderSaga.send_delete_order"},
                    },
                    "status": "finished",
                    "related_services": ["ticket", "order"],
                    "already_rollback": False,
                },
                {
                    "cls": "minos.saga.executions.steps.local.LocalSagaStepExecution",
                    "definition": {
                        "cls": "minos.saga.definitions.steps.local.LocalSagaStep",
                        "on_execute": {"callback": "tests.utils.create_payment"},
                        "on_failure": {"callback": "tests.utils.delete_payment"},
                    },
                    "status": "finished",
                    "related_services": ["order"],
                    "already_rollback": False,
                },
            ],
            "paused_step": {
                "cls": "minos.saga.executions.steps.remote.RemoteSagaStepExecution",
                "definition": {
                    "cls": "minos.saga.definitions.steps.remote.RemoteSagaStep",
                    "on_execute": {"callback": "tests.utils.send_create_ticket"},
                    "on_success": {"callback": "tests.utils.handle_ticket_success"},
                    "on_error": {"callback": "tests.utils.handle_ticket_error"},
                    "on_failure": {"callback": "tests.utils.DeleteOrderSaga.send_delete_ticket"},
                },
                "status": "paused-by-on-execute",
                "already_rollback": False,
                "related_services": ["order"],
            },
            "user": str(self.user),
            "status": "paused",
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }

        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_definition(ADD_ORDER, user=self.user)
            with self.assertRaises(SagaPausedExecutionStepException):
                await expected.execute()

            response = SagaResponse(Foo("hola"), {"ticket"})
            with self.assertRaises(SagaPausedExecutionStepException):
                await expected.execute(response)

        observed = SagaExecution.from_raw(raw)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

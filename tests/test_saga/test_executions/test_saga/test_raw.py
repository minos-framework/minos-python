import unittest
from unittest.mock import (
    MagicMock,
    patch,
)
from uuid import (
    UUID,
)

from minos.common import (
    MinosConfig,
)
from minos.saga import (
    MinosSagaPausedExecutionStepException,
    Saga,
    SagaContext,
    SagaExecution,
)
from tests.utils import (
    BASE_PATH,
    Foo,
    NaiveBroker,
    fake_reply,
    handle_order_success,
    handle_ticket_success_raises,
    send_create_order,
    send_create_ticket,
    send_delete_order,
    send_delete_ticket,
)


class TestSagaExecution(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.saga = (
            Saga()
            .step()
            .invoke_participant(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .step()
            .invoke_participant(send_create_ticket)
            .on_success(handle_ticket_success_raises)
            .on_failure(send_delete_ticket)
            .commit()
        )

    def setUp(self) -> None:
        self.config = MinosConfig(path=BASE_PATH / "config.yml")
        self.broker = NaiveBroker()

        self.publish_mock = MagicMock(side_effect=self.broker.send)
        self.broker.send = self.publish_mock

    def test_from_raw(self):
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_saga(self.saga)
        observed = SagaExecution.from_raw(expected)
        self.assertEqual(expected, observed)

    def test_created(self):
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            execution = SagaExecution.from_saga(self.saga)

        expected = {
            "already_rollback": False,
            "context": SagaContext().avro_str,
            "definition": {
                "commit": {"callback": "minos.saga.definitions.operations.identity_fn"},
                "steps": [
                    {
                        "invoke_participant": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.handle_order_success"},
                        "on_failure": {"callback": "tests.utils.send_delete_order"},
                    },
                    {
                        "invoke_participant": {"callback": "tests.utils.send_create_ticket"},
                        "on_success": {"callback": "tests.utils.handle_ticket_success_raises"},
                        "on_failure": {"callback": "tests.utils.send_delete_ticket"},
                    },
                ],
            },
            "executed_steps": [],
            "paused_step": None,
            "status": "created",
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
                "commit": {"callback": "minos.saga.definitions.operations.identity_fn"},
                "steps": [
                    {
                        "invoke_participant": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.handle_order_success"},
                        "on_failure": {"callback": "tests.utils.send_delete_order"},
                    },
                    {
                        "invoke_participant": {"callback": "tests.utils.send_create_ticket"},
                        "on_success": {"callback": "tests.utils.handle_ticket_success_raises"},
                        "on_failure": {"callback": "tests.utils.send_delete_ticket"},
                    },
                ],
            },
            "executed_steps": [],
            "paused_step": {
                "definition": {
                    "invoke_participant": {"callback": "tests.utils.send_create_order"},
                    "on_success": {"callback": "tests.utils.handle_order_success"},
                    "on_failure": {"callback": "tests.utils.send_delete_order"},
                },
                "status": "paused-on-reply",
                "already_rollback": False,
            },
            "status": "paused",
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }

        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_saga(self.saga)
            with self.assertRaises(MinosSagaPausedExecutionStepException):
                await expected.execute(broker=self.broker)

        observed = SagaExecution.from_raw(raw)
        self.assertEqual(expected, observed)

    async def test_executed_step(self):
        raw = {
            "already_rollback": False,
            "context": SagaContext(order=Foo("hola")).avro_str,
            "definition": {
                "commit": {"callback": "minos.saga.definitions.operations.identity_fn"},
                "steps": [
                    {
                        "invoke_participant": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.handle_order_success"},
                        "on_failure": {"callback": "tests.utils.send_delete_order"},
                    },
                    {
                        "invoke_participant": {"callback": "tests.utils.send_create_ticket"},
                        "on_success": {"callback": "tests.utils.handle_ticket_success_raises"},
                        "on_failure": {"callback": "tests.utils.send_delete_ticket"},
                    },
                ],
            },
            "executed_steps": [
                {
                    "definition": {
                        "invoke_participant": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.handle_order_success"},
                        "on_failure": {"callback": "tests.utils.send_delete_order"},
                    },
                    "status": "finished",
                    "already_rollback": False,
                }
            ],
            "paused_step": {
                "definition": {
                    "invoke_participant": {"callback": "tests.utils.send_create_ticket"},
                    "on_success": {"callback": "tests.utils.handle_ticket_success_raises"},
                    "on_failure": {"callback": "tests.utils.send_delete_ticket"},
                },
                "status": "paused-on-reply",
                "already_rollback": False,
            },
            "status": "paused",
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }

        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_saga(self.saga)
            with self.assertRaises(MinosSagaPausedExecutionStepException):
                await expected.execute(broker=self.broker)

            reply = fake_reply(Foo("hola"))
            with self.assertRaises(MinosSagaPausedExecutionStepException):
                await expected.execute(reply=reply, broker=self.broker)

        observed = SagaExecution.from_raw(raw)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

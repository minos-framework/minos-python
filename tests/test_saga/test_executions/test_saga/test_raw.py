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
    SagaContext,
    SagaExecution,
)
from tests.utils import (
    ADD_ORDER,
    BASE_PATH,
    Foo,
    NaiveBroker,
    fake_reply,
)


class TestSagaExecution(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(path=BASE_PATH / "config.yml")
        self.broker = NaiveBroker()

        self.publish_mock = MagicMock(side_effect=self.broker.send)
        self.broker.send = self.publish_mock

    def test_from_raw(self):
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_saga(ADD_ORDER)
        observed = SagaExecution.from_raw(expected)
        self.assertEqual(expected, observed)

    def test_created(self):
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            execution = SagaExecution.from_saga(ADD_ORDER)

        expected = {
            "already_rollback": False,
            "context": SagaContext().avro_str,
            "definition": {
                "commit": {"callback": "minos.saga.definitions.operations.identity_fn"},
                "steps": [
                    {
                        "on_execute": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.handle_order_success"},
                        "on_failure": {"callback": "tests.utils.send_delete_order"},
                    },
                    {
                        "on_execute": {"callback": "tests.utils.send_create_ticket"},
                        "on_success": {"callback": "tests.utils.handle_ticket_success"},
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
                        "on_execute": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.handle_order_success"},
                        "on_failure": {"callback": "tests.utils.send_delete_order"},
                    },
                    {
                        "on_execute": {"callback": "tests.utils.send_create_ticket"},
                        "on_success": {"callback": "tests.utils.handle_ticket_success"},
                        "on_failure": {"callback": "tests.utils.send_delete_ticket"},
                    },
                ],
            },
            "executed_steps": [],
            "paused_step": {
                "definition": {
                    "on_execute": {"callback": "tests.utils.send_create_order"},
                    "on_success": {"callback": "tests.utils.handle_order_success"},
                    "on_failure": {"callback": "tests.utils.send_delete_order"},
                },
                "status": "paused-on-success",
                "already_rollback": False,
            },
            "status": "paused",
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }

        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_saga(ADD_ORDER)
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
                        "on_execute": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.handle_order_success"},
                        "on_failure": {"callback": "tests.utils.send_delete_order"},
                    },
                    {
                        "on_execute": {"callback": "tests.utils.send_create_ticket"},
                        "on_success": {"callback": "tests.utils.handle_ticket_success"},
                        "on_failure": {"callback": "tests.utils.send_delete_ticket"},
                    },
                ],
            },
            "executed_steps": [
                {
                    "definition": {
                        "on_execute": {"callback": "tests.utils.send_create_order"},
                        "on_success": {"callback": "tests.utils.handle_order_success"},
                        "on_failure": {"callback": "tests.utils.send_delete_order"},
                    },
                    "status": "finished",
                    "already_rollback": False,
                }
            ],
            "paused_step": {
                "definition": {
                    "on_execute": {"callback": "tests.utils.send_create_ticket"},
                    "on_success": {"callback": "tests.utils.handle_ticket_success"},
                    "on_failure": {"callback": "tests.utils.send_delete_ticket"},
                },
                "status": "paused-on-success",
                "already_rollback": False,
            },
            "status": "paused",
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }

        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_saga(ADD_ORDER)
            with self.assertRaises(MinosSagaPausedExecutionStepException):
                await expected.execute(broker=self.broker)

            reply = fake_reply(Foo("hola"))
            with self.assertRaises(MinosSagaPausedExecutionStepException):
                await expected.execute(reply=reply, broker=self.broker)

        observed = SagaExecution.from_raw(raw)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

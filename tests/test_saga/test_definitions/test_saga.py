import unittest
from shutil import (
    rmtree,
)

from minos.saga import (
    MinosAlreadyOnSagaException,
    MinosSagaAlreadyCommittedException,
    MinosSagaException,
    Saga,
    SagaExecution,
    SagaOperation,
    SagaStep,
    identity_fn,
)
from tests.callbacks import (
    create_ticket_on_reply_callback,
)
from tests.utils import (
    BASE_PATH,
    foo_fn,
)


class TestSaga(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    # noinspection PyMissingOrEmptyDocstring
    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_commit(self):
        saga = Saga()
        observed = saga.commit()
        self.assertEqual(saga, observed)
        self.assertEqual(SagaOperation(identity_fn), saga.commit_operation)

    def test_commit_define_callback(self):
        saga = Saga()
        observed = saga.commit(foo_fn)
        self.assertEqual(saga, observed)
        self.assertEqual(SagaOperation(foo_fn), saga.commit_operation)

    def test_commit_raises(self):
        saga = Saga().commit()
        with self.assertRaises(MinosSagaAlreadyCommittedException):
            saga.commit()

    def test_committed_true(self):
        saga = Saga()
        saga.commit_operation = SagaOperation(identity_fn)
        self.assertTrue(saga.committed)

    def test_committed_false(self):
        saga = Saga()
        self.assertFalse(saga.committed)

    def test_step_raises(self):
        saga = Saga().commit()
        with self.assertRaises(MinosSagaAlreadyCommittedException):
            saga.step()

    def test_empty_step_must_throw_exception(self):
        with self.assertRaises(MinosSagaException) as exc:
            (
                Saga()
                .step()
                .invoke_participant("CreateOrder", foo_fn)
                .with_compensation("DeleteOrder", foo_fn)
                .with_compensation("DeleteOrder2", foo_fn)
                .step()
                .step()
                .invoke_participant("CreateTicket", foo_fn)
                .on_reply("ticket", create_ticket_on_reply_callback)
                .step()
                .invoke_participant("VerifyConsumer", foo_fn)
                .commit()
            )

            self.assertEqual("A 'SagaStep' can only define one 'with_compensation' method.", str(exc))

    def test_wrong_step_action_must_throw_exception(self):
        with self.assertRaises(MinosSagaException) as exc:
            (
                Saga()
                .step()
                .invoke_participant("CreateOrder", foo_fn)
                .with_compensation("DeleteOrder", foo_fn)
                .with_compensation("DeleteOrder2", foo_fn)
                .step()
                .on_reply("ticket", create_ticket_on_reply_callback)
                .step()
                .invoke_participant("VerifyConsumer", foo_fn)
                .commit()
            )

            self.assertEqual("A 'SagaStep' can only define one 'with_compensation' method.", str(exc))

    def test_build_execution(self):
        saga = Saga().step().invoke_participant("CreateOrder", foo_fn).with_compensation("DeleteOrder", foo_fn).commit()
        execution = SagaExecution.from_saga(saga)
        self.assertIsInstance(execution, SagaExecution)

    def test_add_step(self):
        step = SagaStep().invoke_participant("CreateOrder", foo_fn)
        saga = Saga().step(step).commit()

        self.assertEqual([step], saga.steps)

    def test_add_step_raises(self):
        step = SagaStep(Saga()).invoke_participant("CreateOrder", foo_fn)
        with self.assertRaises(MinosAlreadyOnSagaException):
            Saga().step(step)

    def test_raw(self):
        saga = (
            Saga()
            .step()
            .invoke_participant("CreateOrder", foo_fn)
            .with_compensation("DeleteOrder", foo_fn)
            .step()
            .invoke_participant("CreateTicket", foo_fn)
            .on_reply("ticket", create_ticket_on_reply_callback)
            .step()
            .invoke_participant("VerifyConsumer", foo_fn)
            .commit()
        )
        expected = {
            "commit": {"callback": "minos.saga.definitions.operations.identity_fn"},
            "steps": [
                {
                    "invoke_participant": {"callback": "tests.utils.foo_fn", "name": "CreateOrder"},
                    "on_reply": None,
                    "with_compensation": {"callback": "tests.utils.foo_fn", "name": "DeleteOrder"},
                },
                {
                    "invoke_participant": {"callback": "tests.utils.foo_fn", "name": "CreateTicket"},
                    "on_reply": {"callback": "tests.callbacks.create_ticket_on_reply_callback", "name": "ticket"},
                    "with_compensation": None,
                },
                {
                    "invoke_participant": {"callback": "tests.utils.foo_fn", "name": "VerifyConsumer"},
                    "on_reply": None,
                    "with_compensation": None,
                },
            ],
        }
        self.assertEqual(expected, saga.raw)

    def test_from_raw(self):
        raw = {
            "name": "CreateShipment",
            "commit": {"callback": "minos.saga.definitions.operations.identity_fn"},
            "steps": [
                {
                    "invoke_participant": {"callback": "tests.utils.foo_fn", "name": "CreateOrder"},
                    "on_reply": None,
                    "with_compensation": {"callback": "tests.utils.foo_fn", "name": "DeleteOrder"},
                },
                {
                    "invoke_participant": {"callback": "tests.utils.foo_fn", "name": "CreateTicket"},
                    "on_reply": {"callback": "tests.callbacks.create_ticket_on_reply_callback", "name": "ticket"},
                    "with_compensation": None,
                },
                {
                    "invoke_participant": {"callback": "tests.utils.foo_fn", "name": "VerifyConsumer"},
                    "on_reply": None,
                    "with_compensation": None,
                },
            ],
        }
        expected = (
            Saga()
            .step()
            .invoke_participant("CreateOrder", foo_fn)
            .with_compensation("DeleteOrder", foo_fn)
            .step()
            .invoke_participant("CreateTicket", foo_fn)
            .on_reply("ticket", create_ticket_on_reply_callback)
            .step()
            .invoke_participant("VerifyConsumer", foo_fn)
            .commit()
        )
        self.assertEqual(expected, Saga.from_raw(raw))

    def test_from_raw_already(self):
        expected = (
            Saga()
            .step()
            .invoke_participant("CreateOrder", foo_fn)
            .with_compensation("DeleteOrder", foo_fn)
            .step()
            .invoke_participant("CreateTicket", foo_fn)
            .on_reply("ticket", create_ticket_on_reply_callback)
            .step()
            .invoke_participant("VerifyConsumer", foo_fn)
            .commit()
        )
        self.assertEqual(expected, Saga.from_raw(expected))


if __name__ == "__main__":
    unittest.main()

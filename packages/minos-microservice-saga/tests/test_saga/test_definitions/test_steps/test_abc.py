import unittest
import warnings
from unittest.mock import (
    MagicMock,
    call,
    patch,
)

from minos.common import (
    classname,
)
from minos.saga import (
    ConditionalSagaStep,
    LocalSagaStep,
    RemoteSagaStep,
    Saga,
    SagaNotDefinedException,
    SagaStep,
)
from tests.utils import (
    create_payment,
    send_create_ticket,
)


class TestSagaStep(unittest.TestCase):
    def test_from_raw(self):
        with patch("minos.saga.SagaStep._from_raw") as mock:
            mock.return_value = 56
            observed = SagaStep.from_raw(dict())
            self.assertEqual(1, mock.call_count)
            self.assertEqual(56, observed)

    def test_from_raw_with_cls(self):
        with patch("minos.saga.RemoteSagaStep._from_raw") as mock:
            mock.return_value = 56
            observed = SagaStep.from_raw({"cls": classname(RemoteSagaStep)})
            self.assertEqual(1, mock.call_count)
            self.assertEqual(56, observed)

    def test_from_raw_raises(self):
        with self.assertRaises(TypeError):
            SagaStep.from_raw({"cls": "datetime.datetime"})

    def test_saga(self):
        saga = Saga()
        step = RemoteSagaStep(saga=saga)
        self.assertEqual(saga, step.saga)

    def test_conditional_step(self):
        step = RemoteSagaStep(saga=Saga())
        mock = MagicMock(return_value=True)
        step.validate = mock
        local_step = step.conditional_step()
        self.assertEqual(1, mock.call_count)
        self.assertIsInstance(local_step, ConditionalSagaStep)

    def test_conditional_step_raises(self):
        with self.assertRaises(SagaNotDefinedException):
            RemoteSagaStep(send_create_ticket).conditional_step()

    def test_local_step(self):
        step = RemoteSagaStep(saga=Saga())
        mock = MagicMock(return_value=True)
        step.validate = mock
        local_step = step.local_step()
        self.assertEqual(1, mock.call_count)
        self.assertIsInstance(local_step, LocalSagaStep)

    def test_local_step_raises(self):
        with self.assertRaises(SagaNotDefinedException):
            RemoteSagaStep(send_create_ticket).local_step()

    def test_step(self):
        step = RemoteSagaStep(send_create_ticket, saga=Saga())
        mock = MagicMock(side_effect=step.remote_step)
        step.remote_step = mock
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            step = step.step()

        self.assertEqual(1, mock.call_count)
        self.assertIsInstance(step, RemoteSagaStep)

    def test_remote_step(self):
        step = RemoteSagaStep(saga=Saga())
        mock = MagicMock(return_value=True)
        step.validate = mock
        remote_step = step.remote_step()
        self.assertEqual(1, mock.call_count)
        self.assertIsInstance(remote_step, RemoteSagaStep)

    def test_remote_step_raises(self):
        with self.assertRaises(SagaNotDefinedException):
            RemoteSagaStep(send_create_ticket).remote_step()

    def test_commit(self):
        saga = Saga()
        step = RemoteSagaStep(send_create_ticket, saga=saga)
        mock = MagicMock(return_value=56)
        saga.commit = mock
        observed = step.commit(create_payment)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(create_payment), mock.call_args)
        self.assertEqual(56, observed)

    def test_commit_validate(self):
        step = RemoteSagaStep(send_create_ticket, saga=Saga())
        mock = MagicMock(return_value=True)
        step.validate = mock
        step.commit()
        self.assertEqual(1, mock.call_count)

    def test_commit_raises(self):
        with self.assertRaises(SagaNotDefinedException):
            RemoteSagaStep(send_create_ticket).commit()


if __name__ == "__main__":
    unittest.main()

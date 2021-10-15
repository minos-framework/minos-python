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
    LocalSagaStep,
    RemoteSagaStep,
    Saga,
    SagaNotDefinedException,
    SagaStep,
)
from tests.utils import (
    commit_callback,
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

    def test_local_validate(self):
        step = RemoteSagaStep(saga=Saga())
        mock = MagicMock(return_value=True)
        step.validate = mock
        local = step.local()
        self.assertEqual(1, mock.call_count)
        self.assertIsInstance(local, LocalSagaStep)

    def test_local_raises(self):
        with self.assertRaises(SagaNotDefinedException):
            RemoteSagaStep(send_create_ticket).local()

    def test_step(self):
        step = RemoteSagaStep(send_create_ticket, saga=Saga())
        mock = MagicMock(side_effect=step.remote)
        step.remote = mock
        with warnings.catch_warnings():
            step = step.step()

        self.assertEqual(1, mock.call_count)
        self.assertIsInstance(step, RemoteSagaStep)

    def test_remote_validate(self):
        step = RemoteSagaStep(saga=Saga())
        mock = MagicMock(return_value=True)
        step.validate = mock
        remote = step.remote()
        self.assertEqual(1, mock.call_count)
        self.assertIsInstance(remote, RemoteSagaStep)

    def test_remote_raises(self):
        with self.assertRaises(SagaNotDefinedException):
            RemoteSagaStep(send_create_ticket).remote()

    def test_commit(self):
        saga = Saga()
        step = RemoteSagaStep(send_create_ticket, saga=saga)
        mock = MagicMock(return_value=56)
        saga.commit = mock
        observed = step.commit(commit_callback)
        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(commit_callback), mock.call_args)
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

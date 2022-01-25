import unittest

from minos.saga import (
    SagaStatus,
    SagaStepStatus,
)


class TestSagaStatus(unittest.TestCase):
    def test_from_raw(self):
        self.assertEqual(SagaStatus.Finished, SagaStatus.from_raw("finished"))

    def test_from_raw_already(self):
        self.assertEqual(SagaStatus.Finished, SagaStatus.from_raw(SagaStatus.Finished))


class TestSagaStepStatus(unittest.TestCase):
    def test_from_raw(self):
        self.assertEqual(SagaStepStatus.Finished, SagaStepStatus.from_raw("finished"))

    def test_from_raw_already(self):
        self.assertEqual(SagaStepStatus.Finished, SagaStepStatus.from_raw(SagaStepStatus.Finished))


if __name__ == "__main__":
    unittest.main()

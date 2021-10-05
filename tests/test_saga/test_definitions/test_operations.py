import unittest

from minos.saga import (
    SagaContext,
    SagaOperation,
)
from tests.utils import (
    send_create_ticket,
)


class TestSagaOperation(unittest.TestCase):
    def test_raw(self):
        step = SagaOperation(send_create_ticket)
        expected = {"callback": "tests.utils.send_create_ticket"}
        self.assertEqual(expected, step.raw)

    def test_raw_with_parameters(self):
        step = SagaOperation(send_create_ticket, parameters=SagaContext(foo="bar"))
        expected = {"callback": "tests.utils.send_create_ticket", "parameters": SagaContext(foo="bar").avro_str}
        observed = step.raw
        self.assertEqual(
            SagaContext.from_avro_str(expected.pop("parameters")), SagaContext.from_avro_str(observed.pop("parameters"))
        )
        self.assertEqual(expected, observed)

    def test_from_raw(self):
        raw = {"callback": "tests.utils.send_create_ticket"}

        expected = SagaOperation(send_create_ticket)
        self.assertEqual(expected, SagaOperation.from_raw(raw))

    def test_from_raw_with_name(self):
        raw = {"callback": "tests.utils.send_create_ticket"}

        expected = SagaOperation(send_create_ticket)
        self.assertEqual(expected, SagaOperation.from_raw(raw))

    def test_from_raw_with_parameters(self):
        raw = {"callback": "tests.utils.send_create_ticket", "parameters": SagaContext(foo="bar").avro_str}

        expected = SagaOperation(send_create_ticket, parameters=SagaContext(foo="bar"))
        self.assertEqual(expected, SagaOperation.from_raw(raw))

    def test_from_raw_already(self):
        expected = SagaOperation(send_create_ticket)
        observed = SagaOperation.from_raw(expected)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

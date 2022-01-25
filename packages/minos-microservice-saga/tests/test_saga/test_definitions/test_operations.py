import unittest

from minos.saga import (
    SagaContext,
    SagaOperation,
)
from tests.utils import (
    send_create_ticket,
)


class TestSagaOperation(unittest.TestCase):
    def test_callback(self):
        operation = SagaOperation(send_create_ticket)
        self.assertEqual(send_create_ticket, operation.callback)

    def test_parameters(self):
        operation = SagaOperation(send_create_ticket, SagaContext(one=1))
        self.assertEqual(SagaContext(one=1), operation.parameters)

    def test_parameters_dict(self):
        operation = SagaOperation(send_create_ticket, {"one": 1})
        self.assertEqual(SagaContext(one=1), operation.parameters)

    def test_parameters_kwargs(self):
        operation = SagaOperation(send_create_ticket, one=1)
        self.assertEqual(SagaContext(one=1), operation.parameters)

    def test_raw(self):
        operation = SagaOperation(send_create_ticket)
        expected = {"callback": "tests.utils.send_create_ticket"}
        self.assertEqual(expected, operation.raw)

    def test_raw_with_parameters(self):
        operation = SagaOperation(send_create_ticket, parameters=SagaContext(foo="bar"))
        expected = {"callback": "tests.utils.send_create_ticket", "parameters": SagaContext(foo="bar").avro_str}
        observed = operation.raw
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

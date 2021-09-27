import unittest

from minos.saga import (
    SagaContext,
    SagaOperation,
)
from tests.utils import (
    foo_fn,
)


class TestSagaOperation(unittest.TestCase):
    def test_raw(self):
        step = SagaOperation(foo_fn)
        expected = {"callback": "tests.utils.foo_fn"}
        self.assertEqual(expected, step.raw)

    def test_raw_with_name(self):
        step = SagaOperation(foo_fn, name="CreateFoo")
        expected = {"callback": "tests.utils.foo_fn", "name": "CreateFoo"}
        self.assertEqual(expected, step.raw)

    def test_raw_with_parameters(self):
        step = SagaOperation(foo_fn, parameters=SagaContext(foo="bar"))
        expected = {"callback": "tests.utils.foo_fn", "parameters": SagaContext(foo="bar").avro_str}
        observed = step.raw
        self.assertEqual(
            SagaContext.from_avro_str(expected.pop("parameters")), SagaContext.from_avro_str(observed.pop("parameters"))
        )
        self.assertEqual(expected, observed)

    def test_from_raw(self):
        raw = {"callback": "tests.utils.foo_fn"}

        expected = SagaOperation(foo_fn)
        self.assertEqual(expected, SagaOperation.from_raw(raw))

    def test_from_raw_with_name(self):
        raw = {"callback": "tests.utils.foo_fn", "name": "CreateFoo"}

        expected = SagaOperation(foo_fn, name="CreateFoo")
        self.assertEqual(expected, SagaOperation.from_raw(raw))

    def test_from_raw_with_parameters(self):
        raw = {"callback": "tests.utils.foo_fn", "parameters": SagaContext(foo="bar").avro_str}

        expected = SagaOperation(foo_fn, parameters=SagaContext(foo="bar"))
        self.assertEqual(expected, SagaOperation.from_raw(raw))

    def test_from_raw_already(self):
        expected = SagaOperation(foo_fn, "CreateFoo")
        observed = SagaOperation.from_raw(expected)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()

import unittest

from minos.common import (
    ModelType,
)
from minos.saga import (
    SagaRequest,
    SagaResponse,
)


class TestSagaRequest(unittest.TestCase):
    def test_create(self):
        self.assertEqual(ModelType.build("SagaRequest", {"foo": str}), SagaRequest({"foo": str}))


class TestSagaResponse(unittest.TestCase):
    def test_create(self):
        self.assertEqual(ModelType.build("SagaResponse", {"foo": str}), SagaResponse({"foo": str}))


if __name__ == "__main__":
    unittest.main()

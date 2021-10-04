import unittest

from minos.saga import (
    SagaRequest,
    SagaResponse,
    SagaResponseStatus,
)


class TestSagaRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.request = SagaRequest("UpdateProductPrice", 56)

    def test_targetd(self):
        self.assertEqual("UpdateProductPrice", self.request.target)

    async def test_content(self):
        self.assertEqual(56, await self.request.content())

    def test_eq(self):
        self.assertEqual(SagaRequest("UpdateProductPrice", 56), self.request)
        self.assertNotEqual(SagaRequest("UpdateProductTitle", 56), self.request)
        self.assertNotEqual(SagaRequest("UpdateProductPrice", 42), self.request)

    def test_repr(self):
        self.assertEqual("SagaRequest('UpdateProductPrice', 56)", repr(self.request))

    def test_hash(self):
        self.assertIsInstance(hash(self.request), int)


class TestSagaResponse(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.response = SagaResponse(56)

    async def test_content(self):
        self.assertEqual(56, await self.response.content())

    def test_ok(self):
        self.assertTrue(SagaResponse(56, SagaResponseStatus.SUCCESS).ok)
        self.assertFalse(SagaResponse(56, SagaResponseStatus.ERROR).ok)

    def test_default_status(self):
        self.assertEqual(SagaResponseStatus.SUCCESS, self.response.status)

    def test_status(self):
        response = SagaResponse(56, SagaResponseStatus.SYSTEM_ERROR)
        self.assertEqual(SagaResponseStatus.SYSTEM_ERROR, response.status)

    def test_eq(self):
        self.assertEqual(SagaResponse(56), self.response)
        self.assertNotEqual(SagaResponse(42), self.response)
        self.assertNotEqual(SagaResponse(56, SagaResponseStatus.SYSTEM_ERROR), self.response)

    def test_repr(self):
        self.assertEqual("SagaResponse(56, <SagaResponseStatus.SUCCESS: 200>)", repr(self.response))

    def test_hash(self):
        self.assertIsInstance(hash(self.response), int)


if __name__ == "__main__":
    unittest.main()

import unittest
from uuid import (
    uuid4,
)

from minos.saga import (
    SagaRequest,
    SagaResponse,
    SagaResponseStatus,
)


class TestSagaRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.request = SagaRequest("UpdateProductPrice", 56)

    async def test_no_content(self):
        request = SagaRequest("UpdateProductPrice")
        self.assertIsNone(await request.content())

    def test_target(self):
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
        self.uuid = uuid4()
        self.response = SagaResponse(56, service_name="ticket", uuid=self.uuid)

    async def test_content(self):
        self.assertEqual(56, await self.response.content())

    def test_service_name(self):
        self.assertEqual("ticket", self.response.service_name)

    def test_service_name_raises(self):
        with self.assertRaises(ValueError):
            SagaResponse(56)

    def test_uuid(self):
        self.assertEqual(self.uuid, self.response.uuid)

    def test_ok(self):
        self.assertTrue(SagaResponse(56, SagaResponseStatus.SUCCESS, service_name="ticket").ok)
        self.assertFalse(SagaResponse(56, SagaResponseStatus.ERROR, service_name="ticket").ok)

    def test_default_status(self):
        self.assertEqual(SagaResponseStatus.SUCCESS, self.response.status)

    def test_status(self):
        response = SagaResponse(56, SagaResponseStatus.SYSTEM_ERROR, service_name="ticket")
        self.assertEqual(SagaResponseStatus.SYSTEM_ERROR, response.status)

    def test_status_raw(self):
        response = SagaResponse(56, status=200, service_name="ticket")
        self.assertEqual(SagaResponseStatus.SUCCESS, response.status)

    def test_eq(self):
        self.assertEqual(SagaResponse(56, service_name="ticket", uuid=self.uuid), self.response)
        self.assertNotEqual(SagaResponse(42, service_name="ticket"), self.response)
        self.assertNotEqual(SagaResponse(56, SagaResponseStatus.SYSTEM_ERROR, service_name="ticket"), self.response)

    def test_repr(self):
        self.assertEqual(
            f"SagaResponse(56, {SagaResponseStatus.SUCCESS!r}, 'ticket', {self.uuid!r})", repr(self.response)
        )

    def test_hash(self):
        self.assertIsInstance(hash(self.response), int)


if __name__ == "__main__":
    unittest.main()

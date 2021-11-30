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
        self.related_services = {"ticket"}
        self.response = SagaResponse(56, self.related_services, uuid=self.uuid)

    async def test_content(self):
        self.assertEqual(56, await self.response.content())

    def test_service_name(self):
        self.assertEqual(self.related_services, self.response.related_services)

    def test_service_name_empty(self):
        self.assertEqual(set(), SagaResponse(56).related_services)

    def test_uuid(self):
        self.assertEqual(self.uuid, self.response.uuid)

    def test_ok(self):
        self.assertTrue(SagaResponse(56, self.related_services, SagaResponseStatus.SUCCESS).ok)
        self.assertFalse(SagaResponse(56, self.related_services, SagaResponseStatus.ERROR).ok)

    def test_default_status(self):
        self.assertEqual(SagaResponseStatus.SUCCESS, self.response.status)

    def test_status(self):
        response = SagaResponse(56, self.related_services, SagaResponseStatus.SYSTEM_ERROR)
        self.assertEqual(SagaResponseStatus.SYSTEM_ERROR, response.status)

    def test_status_raw(self):
        response = SagaResponse(56, self.related_services, status=200)
        self.assertEqual(SagaResponseStatus.SUCCESS, response.status)

    def test_eq(self):
        self.assertEqual(SagaResponse(56, self.related_services, uuid=self.uuid), self.response)
        self.assertNotEqual(SagaResponse(42, self.related_services), self.response)
        self.assertNotEqual(SagaResponse(56, self.related_services, SagaResponseStatus.SYSTEM_ERROR), self.response)
        self.assertNotEqual(SagaResponse(56, {"product"}, uuid=self.uuid), self.response)

    def test_repr(self):

        self.assertEqual(
            f"SagaResponse(56, {SagaResponseStatus.SUCCESS!r}, {self.related_services!r}, {self.uuid!r})",
            repr(self.response),
        )

    def test_hash(self):
        self.assertIsInstance(hash(self.response), int)


if __name__ == "__main__":
    unittest.main()

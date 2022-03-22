import sys
import unittest

from faker import (
    Faker,
)
from minos.networks import (
    InMemoryRequest,
    Response,
)

from src import (
    Payment,
    PaymentCommandService,
)
from tests.utils import (
    build_dependency_injector,
)


class TestPaymentCommandService(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        service = PaymentCommandService()
        self.assertIsInstance(service, PaymentCommandService)

    async def test_create_payment(self):
        service = PaymentCommandService()
        faker = Faker()
        faker.factories
        Faker.seed(0)
        user_name = faker.first_name()
        user_surname = faker.last_name()
        expire = faker.credit_card_expire()
        card_number = faker.credit_card_number()
        security_code = faker.credit_card_security_code()
        request = InMemoryRequest(
            {
                "card_number": card_number,
                "validity": expire,
                "security_code": security_code,
                "name": user_name,
                "surname": user_surname,
            }
        )
        response = await service.create_payment(request)

        self.assertIsInstance(response, Response)

        observed = await response.content()

        self.assertEqual({"status": "payment accepted"}, observed)

        self.assertEqual(1, len([p async for p in Payment.get_all()]))


if __name__ == "__main__":
    unittest.main()

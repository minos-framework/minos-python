from minos.cqrs import (
    CommandService,
)
from minos.networks import (
    Request,
    Response,
    ResponseException,
    enroute,
)

from ..aggregates import (
    PaymentAggregate,
)


class PaymentCommandService(CommandService):
    """PaymentCommandService class."""

    def validate_card(self, card_number: str) -> bool:
        def digits_of(n):
            return [int(d) for d in str(n)]

        digits = digits_of(card_number)
        odd_digits = digits[-1::-2]
        even_digits = digits[-2::-2]
        checksum = 0
        checksum += sum(odd_digits)
        for d in even_digits:
            checksum += sum(digits_of(d * 2))
        return_value = checksum % 10
        if return_value == 0:
            return True
        return False

    @enroute.broker.command("CreatePayment")
    async def create_payment(self, request: Request) -> Response:
        """Create a new ``Payment`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            content = await request.content()
            if self.validate_card(content["card_number"]):
                payment = await PaymentAggregate.create(
                    content["card_number"],
                    content["validity"],
                    content["security_code"],
                    content["name"],
                    content["surname"],
                )

            return Response({"status": "payment accepted"})
        except Exception as exc:
            raise ResponseException(f"An error occurred during Payment creation: {exc}")

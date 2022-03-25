from uuid import (
    UUID,
)

from minos.aggregate import (
    Aggregate,
    RootEntity,
)


class Payment(RootEntity):
    """Payment RootEntity class."""

    card_number: str
    validity: str
    security_code: str
    user_name: str
    user_surname: str


class PaymentAggregate(Aggregate[Payment]):
    """PaymentAggregate class."""

    @staticmethod
    async def create(card: str, validity: str, security_code: str, name: str, surname: str) -> UUID:
        """Create a new instance."""
        payment = await Payment.create(
            card_number=card, validity=validity, security_code=security_code, user_name=name, user_surname=surname
        )
        return payment.uuid

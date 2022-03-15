import os.path as op
import urllib.request
from datetime import (
    date,
)

from currency_converter import (
    ECB_URL,
    CurrencyConverter,
)

from minos.cqrs import (
    CommandService,
)
from minos.networks import (
    Request,
    Response,
    ResponseException,
    enroute,
)


class CurrencyCommandService(CommandService):
    """CurrencyCommandService class."""

    @enroute.broker.command("GetCurrencyQuote")
    async def create_currency(self, request: Request) -> Response:
        """Create a new ``Currency`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            # check if the filename already exist, if not download
            filename = f"ecb_{date.today():%Y%m%d}.zip"
            current_path = op.dirname(op.abspath(__file__))
            filename_path = op.join(current_path, "..", "currency_files", filename)
            if not op.isfile(filename_path):
                urllib.request.urlretrieve(ECB_URL, filename_path)
            c = CurrencyConverter(filename_path)

            content = await request.content()  # get the request payload
            quantity = content["quantity"]
            from_currency = content["from"]
            to_currency = content["to"]
            if from_currency == to_currency:
                final_value = quantity
            else:
                final_value = c.convert(quantity, from_currency, to_currency)
            return Response({"quote": final_value, "currency": to_currency})
        except Exception as exc:
            raise ResponseException(f"An error occurred during the Query process: {exc}")

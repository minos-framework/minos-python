import haversine as haversine
from minos.cqrs import (
    CommandService,
)
from minos.networks import (
    Request,
    Response,
    ResponseException,
    enroute,
)

from geopy.geocoders import Nominatim


class ShippingCommandService(CommandService):
    """ShippingCommandService class."""

    @enroute.broker.command("GetShippingQuote")
    async def create_shipping(self, request: Request) -> Response:
        """Return a Shipping cuote.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            madrid = (40.416775, -3.703790)
            price_per_km = 0.001 # euro cents
            content = await request.content()  # get the request payload
            destination = content['destination']
            geolocator = Nominatim(user_agent="Shipping Microservice")
            location = geolocator.geocode(destination) # get latitude and longitude from destination
            km_distance = haversine(madrid, (location.latitude, location.longitude))
            price_total = round(km_distance) * price_per_km
            price_total = round(price_total) # price in euro
            items_count = content['items']
            # check if items are more than 2
            if items_count > 2:
                # on that case we put a surcharge of 10 euros
                price_total += 10
            return Response({'quote': price_total})
        except Exception as exc:
            raise ResponseException(f"An error occurred during the Query process: {exc}")

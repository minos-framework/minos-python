from ..decorators import (
    enroute,
)
from ..requests import (
    Request,
    Response,
)
from ..utils import (
    get_host_ip,
)


class SystemService:
    """System Service class."""

    # noinspection PyUnusedLocal
    @enroute.rest.command("/system/health", "GET")
    def check_health(self, request: Request) -> Response:
        """Get the system health.

        :param request: The given request.
        :return: A Response containing the system status.
        """
        return Response({"host": get_host_ip()})

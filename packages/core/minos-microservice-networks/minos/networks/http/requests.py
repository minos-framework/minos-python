from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from cached_property import (
    cached_property,
)

from ..requests import (
    Request,
    Response,
    ResponseException,
)


class HttpRequest(Request, ABC):
    """Http Request class."""

    @cached_property
    def user(self) -> Optional[UUID]:
        """
        Returns the UUID of the user making the Request.
        """
        if "user" not in self.headers:
            return None
        return UUID(self.headers["user"])

    @property
    @abstractmethod
    def headers(self) -> dict[str, str]:
        """Get the headers of the request.

        :return: A dictionary in which keys are ``str`` instances and values are ``str`` instances.
        """

    @property
    @abstractmethod
    def content_type(self) -> str:
        """Get the content type.

        :return: A ``str`` value.
        """

    @abstractmethod
    async def url_params(self, type_: Optional[Union[type, str]] = None, **kwargs) -> Any:
        """Get the url params.

        :param type_: Optional ``type`` or ``str`` (classname) that defines the request content type.
        :param kwargs: Additional named arguments.
        :return: A dictionary instance.
        """

    @property
    @abstractmethod
    def has_url_params(self) -> bool:
        """Check if the request has url params.

        :return: ``True`` if it has url params or ``False`` otherwise.
        """

    @abstractmethod
    async def query_params(self, type_: Optional[Union[type, str]] = None, **kwargs) -> Any:
        """Get the query params.


        :param type_: Optional ``type`` or ``str`` (classname) that defines the request content type.
        :param kwargs: Additional named arguments.
        :return: A dictionary instance.
        """

    @property
    @abstractmethod
    def has_query_params(self) -> bool:
        # noinspection GrazieInspection
        """Check if the request has query params.

        :return: ``True`` if it has query params or ``False`` otherwise.
        """


class HttpResponse(Response, ABC):
    """Http Response class."""

    def __init__(self, *args, content_type: str = "application/json", **kwargs):
        super().__init__(*args, **kwargs)
        self._content_type = content_type

    @property
    def content_type(self) -> str:
        """Get the content type.

        :return: A ``str`` value.
        """
        return self._content_type

    @classmethod
    def from_response(cls, response: Optional[Response]) -> HttpResponse:
        """Build a new ``RestRequest`` from another response.

        :param response: The base response.
        :return: A ``RestResponse`` instance.
        """
        if isinstance(response, cls):
            return response

        if response is None:
            return cls()

        return cls(response._data, status=response.status)


class HttpResponseException(ResponseException):
    """Http Response Exception class."""

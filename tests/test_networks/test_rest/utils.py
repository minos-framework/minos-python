from json import (
    JSONDecodeError,
)
from uuid import (
    uuid4,
)

from yarl import (
    URL,
)


class MockedRequest:
    def __init__(self, data=None, user=None):
        if user is None:
            user = uuid4()
        self.data = data
        self.remote = "127.0.0.1"
        self.rel_url = URL("localhost")
        self.match_info = dict()
        self.headers = {"User": str(user), "something": "123"}

    def __repr__(self):
        return "repr"

    async def json(self):
        if self.data is None:
            raise JSONDecodeError("", "", 1)
        return self.data

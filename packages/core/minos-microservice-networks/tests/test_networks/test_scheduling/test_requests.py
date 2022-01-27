import unittest

from minos.common import (
    DeclarativeModel,
    current_datetime,
)
from minos.networks import (
    NotHasParamsException,
    ScheduledRequest,
    ScheduledRequestContent,
)


class TestScheduledRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.now = current_datetime()
        self.request = ScheduledRequest(self.now)

    def test_has_content(self):
        self.assertEqual(True, self.request.has_content)

    async def test_content(self):
        self.assertEqual(ScheduledRequestContent(self.now), await self.request.content())

    def test_has_params(self):
        self.assertEqual(False, self.request.has_params)

    async def test_params_raises(self):
        with self.assertRaises(NotHasParamsException):
            await self.request.params()

    def test_user(self):
        self.assertIsNone(self.request.user)

    def test_eq(self):
        self.assertEqual(self.request, ScheduledRequest(self.now))
        self.assertNotEqual(self.request, ScheduledRequest(current_datetime()))

    def test_repr(self):
        content = ScheduledRequestContent(self.now)
        self.assertEqual(f"ScheduledRequest({content!r})", repr(self.request))


class TestScheduledRequestContent(unittest.IsolatedAsyncioTestCase):
    def test_subclass(self):
        self.assertTrue(issubclass(ScheduledRequestContent, DeclarativeModel))

    def test_scheduled_at(self):
        now = current_datetime()
        content = ScheduledRequestContent(now)
        self.assertEqual(now, content.scheduled_at)


if __name__ == "__main__":
    unittest.main()

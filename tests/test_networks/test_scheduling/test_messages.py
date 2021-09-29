import unittest

from minos.common import (
    DeclarativeModel,
    current_datetime,
)
from minos.networks import (
    SchedulingRequest,
    SchedulingRequestContent,
)


class TestSchedulingRequest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.now = current_datetime()
        self.request = SchedulingRequest(self.now)

    async def test_content(self):
        self.assertEqual(SchedulingRequestContent(self.now), await self.request.content())

    def test_user(self):
        self.assertIsNone(self.request.user)

    def test_eq(self):
        self.assertEqual(self.request, SchedulingRequest(self.now))
        self.assertNotEqual(self.request, SchedulingRequest(current_datetime()))

    def test_repr(self):
        self.assertEqual(f"SchedulingRequest(SchedulingRequestContent(scheduled_at={self.now!s}))", repr(self.request))


class TestSchedulingRequestContent(unittest.IsolatedAsyncioTestCase):
    def test_subclass(self):
        self.assertTrue(issubclass(SchedulingRequestContent, DeclarativeModel))

    def test_scheduled_at(self):
        now = current_datetime()
        content = SchedulingRequestContent(now)
        self.assertEqual(now, content.scheduled_at)


if __name__ == "__main__":
    unittest.main()

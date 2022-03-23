import sys
import unittest

from src import (
    Notifier,
    NotifierCommandService,
)

from minos.networks import (
    InMemoryRequest,
    Response,
)
from tests.utils import (
    build_dependency_injector,
)


class TestNotifierCommandService(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.injector = build_dependency_injector()

    async def asyncSetUp(self) -> None:
        await self.injector.wire(modules=[sys.modules[__name__]])

    async def asyncTearDown(self) -> None:
        await self.injector.unwire()

    def test_constructor(self):
        service = NotifierCommandService()
        self.assertIsInstance(service, NotifierCommandService)

    async def test_send_notification(self):
        service = NotifierCommandService()
        text_message = """\
        This is a test message from Minos
        """
        html_message = """\
        <html>
          <body>
            <p>Hi,<br>
               This is a test message from <a href="https://github.com/minos-framework/minos-python">Minos</a>
            </p>
          </body>
        </html>
        """
        request = InMemoryRequest(
            {"to": "cingusoft@gmail.com", "subject": "Test Email", "text": text_message, "html": html_message}
        )
        response = await service.send_notification_email(request)

        self.assertIsInstance(response, Response)

        observed = await response.content()


if __name__ == "__main__":
    unittest.main()

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from minos.cqrs import (
    CommandService,
)
from minos.networks import (
    Request,
    Response,
    ResponseException,
    enroute,
)

import ssl, smtplib


class NotifierCommandService(CommandService):
    """NotifierCommandService class."""

    @enroute.broker.command("SendEmailNotification")
    async def send_notification_email(self, request: Request) -> Response:
        """Create a new ``Notifier`` instance.

        :param request: The ``Request`` instance.
        :return: A ``Response`` instance.
        """
        try:
            smtp_server = "smtp.gmail.com"
            port = 465
            content = await request.content()  # get the request payload
            email_from = "andrea@clariteia.com"
            password = "khegjvbpqfrrlvgd"
            email_to = content["to"]
            message = MIMEMultipart("alternative")
            message["Subject"] = content["subject"]
            message["From"] = email_from
            message["To"] = email_to
            text_part = ""
            if "text" in content:
                text_part = MIMEText(content["text"], "plain")
            html_part = MIMEText(content["html"], "html")
            message.attach(text_part)
            message.attach(html_part)
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_server, context=context) as server:
                server.login(email_from, password)
                server.sendmail(email_from, email_to, message.as_string())
            return Response({"message": "Email Sent"})
        except Exception as exc:
            raise ResponseException(f"An error occurred during Notifier creation: {exc}")

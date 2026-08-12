from __future__ import annotations

from email.message import EmailMessage
import smtplib
import ssl

from .config import Settings
from .message import HeartbeatMessage


class SmtpMailer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def send_heartbeat(self, heartbeat: HeartbeatMessage) -> None:
        message = EmailMessage()
        message["From"] = self._settings.smtp_from
        message["To"] = ", ".join(self._settings.smtp_to)
        message["Subject"] = (
            f"{self._settings.mail_subject_prefix} {heartbeat.display_name()}"
        )
        message.set_content(
            "Kafka heartbeat 메시지를 수신했습니다.\n\n"
            f"수신 시각(UTC): {heartbeat.consumed_at.isoformat()}\n"
            f"Topic: {heartbeat.topic}\n"
            f"Partition: {heartbeat.partition}\n"
            f"Offset: {heartbeat.offset}\n"
            f"Key: {heartbeat.key or '-'}\n\n"
            "Payload:\n"
            f"{heartbeat.pretty_payload()}\n"
        )

        context = ssl.create_default_context()
        if self._settings.smtp_use_ssl:
            with smtplib.SMTP_SSL(
                self._settings.smtp_host,
                self._settings.smtp_port,
                timeout=30,
                context=context,
            ) as smtp:
                self._authenticate_and_send(smtp, message)
            return

        with smtplib.SMTP(
            self._settings.smtp_host, self._settings.smtp_port, timeout=30
        ) as smtp:
            if self._settings.smtp_use_starttls:
                smtp.starttls(context=context)
            self._authenticate_and_send(smtp, message)

    def _authenticate_and_send(
        self, smtp: smtplib.SMTP, message: EmailMessage
    ) -> None:
        if self._settings.smtp_username:
            smtp.login(
                self._settings.smtp_username, self._settings.smtp_password
            )
        smtp.send_message(message)


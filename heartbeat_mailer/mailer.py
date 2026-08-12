from __future__ import annotations

from email.message import EmailMessage
import smtplib
import ssl

from .config import Settings
from .message import HeartbeatMessage


class SmtpMailer:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def send_heartbeat(
        self, heartbeat: HeartbeatMessage, notification: str, detail: str = ""
    ) -> None:
        labels = {
            "ALERT": "장애",
            "RECOVERY": "복구",
            "MISSING": "Heartbeat 미수신",
        }
        label = labels.get(notification, notification)
        message = EmailMessage()
        message["From"] = self._settings.smtp_from
        message["To"] = ", ".join(self._settings.smtp_to)
        message["Subject"] = (
            f"{self._settings.mail_subject_prefix} [{label}] "
            f"{heartbeat.display_name()}"
        )
        message.set_content(
            f"알림 유형: {label}\n"
            f"상세: {detail or '-'}\n\n"
            f"장비 ID: {heartbeat.device_id}\n"
            f"시스템 ID: {heartbeat.system_id}\n"
            f"호스트: {heartbeat.hostname}\n"
            f"IP: {heartbeat.ip_address}\n"
            f"프로그램: {heartbeat.program_name} {heartbeat.program_version}\n"
            f"상태: {heartbeat.status_level} / {heartbeat.status_code}\n"
            f"메시지: {heartbeat.status_message}\n"
            f"Sequence: {heartbeat.sequence}\n"
            f"생성 시각: {heartbeat.generated_at}\n"
            f"수신 시각(UTC): {heartbeat.consumed_at.isoformat()}\n"
            f"CloudEvent ID: {heartbeat.event_id}\n"
            f"Topic: {heartbeat.topic}\n"
            f"Partition: {heartbeat.partition}\n"
            f"Offset: {heartbeat.offset}\n"
            f"Key: {heartbeat.key or '-'}\n\n"
            "\nPayload:\n"
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

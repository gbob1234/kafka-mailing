from __future__ import annotations

from email.message import EmailMessage
import smtplib

from .config import Settings
from .message import HeartbeatMessage


class SmtpMailer:
    """설정된 SMTP 서버를 통해 장비 상태 알림을 전송한다."""

    def __init__(self, settings: Settings) -> None:
        """SMTP 발송기를 초기화한다.

        입력:
            settings: SMTP 주소, 포트, 발신자와 수신자를 포함한 설정.
        반환:
            없음.
        """
        self._settings = settings

    def send_heartbeat(
        self, heartbeat: HeartbeatMessage, notification: str, detail: str = ""
    ) -> None:
        """heartbeat 상태를 사람이 읽을 수 있는 이메일로 발송한다.

        입력:
            heartbeat: 메일 본문에 포함할 장비 및 CloudEvent 정보.
            notification: ``ALERT``, ``RECOVERY``, ``MISSING`` 중 알림 유형.
            detail: 알림 원인을 설명하는 선택 문자열.
        반환:
            없음. SMTP 서버가 메시지를 받아들이면 정상 종료한다.
        예외:
            smtplib.SMTPException: 연결, 인증 또는 발송에 실패한 경우.
            OSError: SMTP 서버와 네트워크 연결을 만들 수 없는 경우.
        """
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

        with smtplib.SMTP(
            self._settings.smtp_host, self._settings.smtp_port, timeout=30
        ) as smtp:
            smtp.send_message(message)

from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from heartbeat_mailer.mailer import SmtpMailer
from heartbeat_mailer.message import HeartbeatMessage
from tests.test_message import FakeRecord, cloud_event


class FakePlainSmtp:
    """TLS와 인증 메서드 없이 plain SMTP 발송만 제공하는 테스트 대역."""

    instances: list["FakePlainSmtp"] = []

    def __init__(self, host: str, port: int, timeout: int) -> None:
        """연결 인자와 발송 메시지를 기록한다."""
        self.host = host
        self.port = port
        self.timeout = timeout
        self.messages = []
        self.instances.append(self)

    def __enter__(self) -> "FakePlainSmtp":
        """context manager 진입 시 현재 SMTP 대역을 반환한다."""
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        """context manager 종료를 허용하며 예외를 숨기지 않는다."""
        return None

    def send_message(self, message) -> None:
        """발송된 MIME 메시지를 목록에 기록한다."""
        self.messages.append(message)


class SmtpMailerTest(unittest.TestCase):
    """메일 발송기가 암호화·인증 없이 plain SMTP만 사용하는지 검증한다."""

    def test_uses_plain_smtp_without_tls_or_auth(self) -> None:
        """SMTP 생성과 send_message 외 메서드가 필요하지 않은지 확인한다."""
        FakePlainSmtp.instances.clear()
        settings = SimpleNamespace(
            smtp_host="mail.internal",
            smtp_port=25,
            smtp_from="sender@example.com",
            smtp_to=("receiver@example.com",),
            mail_subject_prefix="[Kafka Heartbeat]",
        )
        heartbeat = HeartbeatMessage.from_kafka_record(
            FakeRecord(cloud_event())
        )
        original_generated_at = heartbeat.payload["data"]["heartbeat"][
            "generatedAt"
        ]

        with patch("heartbeat_mailer.mailer.smtplib.SMTP", FakePlainSmtp):
            SmtpMailer(settings).send_heartbeat(
                heartbeat, "ALERT", "test alert"
            )

        smtp = FakePlainSmtp.instances[0]
        self.assertEqual(
            ("mail.internal", 25, 30),
            (smtp.host, smtp.port, smtp.timeout),
        )
        self.assertEqual(1, len(smtp.messages))
        body = smtp.messages[0].get_content()
        self.assertIn(
            "CloudEvent 시각(KST): 2026-08-12 11:14:37.000 KST (UTC+09:00)",
            body,
        )
        self.assertIn(
            '"generatedAt": "2026-08-12T11:14:37+09:00"',
            body,
        )
        self.assertNotIn("메일러 수신 시각(UTC)", body)
        self.assertEqual(
            original_generated_at,
            heartbeat.payload["data"]["heartbeat"]["generatedAt"],
        )


if __name__ == "__main__":
    unittest.main()

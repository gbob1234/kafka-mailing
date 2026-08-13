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
    """plain SMTP와 Outlook 호환 수집기 알림 본문을 검증한다."""

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
                heartbeat, "ALERT", "장비가 비정상 상태를 보고했습니다."
            )

        smtp = FakePlainSmtp.instances[0]
        self.assertEqual(
            ("mail.internal", 25, 30),
            (smtp.host, smtp.port, smtp.timeout),
        )
        self.assertEqual(1, len(smtp.messages))
        sent = smtp.messages[0]
        plain = sent.get_body(preferencelist=("plain",)).get_content()
        html = sent.get_body(preferencelist=("html",)).get_content()
        self.assertIn(
            "수집기 보고 시각: 2026-08-12 11:14:37 KST",
            plain,
        )
        self.assertIn("수집기가 비정상 상태를 보고했습니다.", html)
        self.assertIn("대상 장비 ID", html)
        self.assertIn("수집기 호스트", html)
        self.assertIn("IMAGE COLLECTOR MONITOR", html)
        self.assertIn('role="presentation"', html)
        self.assertIn("[수집기 경고]", str(sent["Subject"]))
        self.assertNotIn("Payload", plain)
        self.assertNotIn("Payload", html)
        self.assertNotIn("Topic", html)
        self.assertNotIn("Partition", html)
        self.assertNotIn("장비가 비정상", plain)
        self.assertNotIn("장비가 비정상", html)
        self.assertIn("수집기가 비정상 상태를 보고했습니다.", plain)
        self.assertEqual(
            original_generated_at,
            heartbeat.payload["data"]["heartbeat"]["generatedAt"],
        )

    def test_recovery_and_missing_use_collector_wording(self) -> None:
        """복구와 미수신 본문도 장비가 아닌 수집기를 주체로 표시한다."""
        FakePlainSmtp.instances.clear()
        settings = SimpleNamespace(
            smtp_host="mail.internal",
            smtp_port=25,
            smtp_from="sender@example.com",
            smtp_to=("receiver@example.com",),
            mail_subject_prefix="[Collector]",
        )
        heartbeat = HeartbeatMessage.from_kafka_record(
            FakeRecord(cloud_event())
        )

        with patch("heartbeat_mailer.mailer.smtplib.SMTP", FakePlainSmtp):
            mailer = SmtpMailer(settings)
            mailer.send_heartbeat(heartbeat, "RECOVERY")
            mailer.send_heartbeat(heartbeat, "MISSING")

        recovery_html = FakePlainSmtp.instances[0].messages[0].get_body(
            preferencelist=("html",)
        ).get_content()
        missing_html = FakePlainSmtp.instances[1].messages[0].get_body(
            preferencelist=("html",)
        ).get_content()
        self.assertIn("수집기가 UP 상태로 복구되었습니다.", recovery_html)
        self.assertIn("수집기의 Heartbeat가 일정 시간 수신되지 않았습니다.", missing_html)
        self.assertNotIn("장비가 UP", recovery_html)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
import json
import smtplib
from typing import Any

from .config import Settings
from .message import HeartbeatMessage


KST = timezone(timedelta(hours=9), name="KST")


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
        event_time_kst = _format_iso_time_kst(heartbeat.event_time)
        generated_at_kst = _format_iso_time_kst(heartbeat.generated_at)
        consumed_at_kst = _format_datetime_kst(heartbeat.consumed_at)
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
            f"CloudEvent 시각(KST): {event_time_kst}\n"
            f"Heartbeat 생성 시각(KST): {generated_at_kst}\n"
            f"메일러 수신 시각(KST): {consumed_at_kst}\n"
            f"CloudEvent ID: {heartbeat.event_id}\n"
            f"Topic: {heartbeat.topic}\n"
            f"Partition: {heartbeat.partition}\n"
            f"Offset: {heartbeat.offset}\n"
            f"Key: {heartbeat.key or '-'}\n\n"
            "\nPayload (시간 필드는 KST 표시용으로 변환):\n"
            f"{_pretty_payload_kst(heartbeat.payload)}\n"
        )

        with smtplib.SMTP(
            self._settings.smtp_host, self._settings.smtp_port, timeout=30
        ) as smtp:
            smtp.send_message(message)


def _format_datetime_kst(value: datetime) -> str:
    """timezone-aware datetime을 사람이 읽기 쉬운 한국 시각으로 변환한다.

    입력:
        value: 변환할 datetime. timezone 정보가 없으면 UTC로 간주한다.
    반환:
        ``YYYY-MM-DD HH:MM:SS.mmm KST (UTC+09:00)`` 형식 문자열.
    """
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    converted = aware.astimezone(KST)
    return f"{converted.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} KST (UTC+09:00)"


def _format_iso_time_kst(value: str) -> str:
    """RFC 3339/ISO 8601 문자열을 한국 시각 표시 문자열로 변환한다.

    입력:
        value: ``Z`` 또는 UTC offset을 포함한 날짜·시간 문자열.
    반환:
        파싱 성공 시 KST 문자열, 실패 시 장애 분석을 위해 원문 문자열.
    """
    try:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
        return _format_datetime_kst(parsed)
    except (TypeError, ValueError):
        return value


def _pretty_payload_kst(payload: dict[str, Any]) -> str:
    """CloudEvent 복사본의 시간 필드를 KST ISO 문자열로 변환한다.

    입력:
        payload: UTC 원본 CloudEvent JSON 객체.
    반환:
        원본을 변경하지 않고 ``time`` 및 ``*At`` 문자열만 KST offset으로
        변환한 들여쓰기 JSON 문자열.
    """
    converted = deepcopy(payload)
    _convert_time_fields(converted)
    return json.dumps(converted, ensure_ascii=False, indent=2)


def _convert_time_fields(value: Any) -> None:
    """중첩 JSON 객체에서 시간 필드를 찾아 KST ISO 문자열로 치환한다.

    입력:
        value: 순회할 dict, list 또는 단일 JSON 값.
    반환:
        없음. 전달된 이메일 표시용 복사본을 제자리에서 변경한다.
    """
    if isinstance(value, dict):
        for key, item in value.items():
            is_time_field = key == "time" or key.lower().endswith("at")
            if is_time_field and isinstance(item, str):
                formatted = _format_iso_time_kst(item)
                if formatted != item:
                    normalized = (
                        item[:-1] + "+00:00" if item.endswith("Z") else item
                    )
                    parsed = datetime.fromisoformat(normalized)
                    value[key] = parsed.astimezone(KST).isoformat()
            else:
                _convert_time_fields(item)
    elif isinstance(value, list):
        for item in value:
            _convert_time_fields(item)

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from html import escape
import smtplib

from .config import Settings
from .message import HeartbeatMessage


KST = timezone(timedelta(hours=9), name="KST")


class SmtpMailer:
    """설정된 SMTP 서버를 통해 수집기 상태 알림을 전송한다."""

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
        """수집기 상태를 텍스트와 Outlook 호환 HTML 이메일로 발송한다.

        입력:
            heartbeat: 메일 본문에 포함할 수집기와 대상 장비 정보.
            notification: ``ALERT``, ``RECOVERY``, ``MISSING`` 중 알림 유형.
            detail: 알림 원인을 설명하는 선택 문자열.
        반환:
            없음. SMTP 서버가 메시지를 받아들이면 정상 종료한다.
        예외:
            smtplib.SMTPException: 연결 또는 발송에 실패한 경우.
            OSError: SMTP 서버와 네트워크 연결을 만들 수 없는 경우.
        """
        presentation = _presentation(notification)
        collector_detail = _collector_detail(detail)
        generated_at_kst = _format_iso_time_kst(heartbeat.generated_at)
        consumed_at_kst = _format_datetime_kst(heartbeat.consumed_at)

        message = EmailMessage()
        message["From"] = self._settings.smtp_from
        message["To"] = ", ".join(self._settings.smtp_to)
        message["Subject"] = (
            f"{self._settings.mail_subject_prefix} "
            f"[{presentation['label']}] {heartbeat.display_name()}"
        )
        message.set_content(
            _plain_body(
                heartbeat,
                presentation,
                collector_detail,
                generated_at_kst,
                consumed_at_kst,
            )
        )
        message.add_alternative(
            _html_body(
                heartbeat,
                presentation,
                collector_detail,
                generated_at_kst,
                consumed_at_kst,
            ),
            subtype="html",
        )

        with smtplib.SMTP(
            self._settings.smtp_host, self._settings.smtp_port, timeout=30
        ) as smtp:
            smtp.send_message(message)


def _presentation(notification: str) -> dict[str, str]:
    """알림 유형별 현업 표시 문구와 Outlook 안전 색상을 반환한다.

    입력:
        notification: ALERT, RECOVERY 또는 MISSING.
    반환:
        제목, 안내 문구, 강조색과 배경색을 포함한 문자열 사전.
    """
    values = {
        "ALERT": {
            "label": "수집기 경고",
            "title": "수집기 상태 확인이 필요합니다",
            "summary": "수집기가 비정상 상태를 보고했습니다.",
            "color": "#B42318",
            "soft_color": "#FEF3F2",
            "border_color": "#FECDCA",
        },
        "RECOVERY": {
            "label": "수집기 복구",
            "title": "수집기가 정상 상태로 복구되었습니다",
            "summary": "수집기가 UP 상태로 복구되었습니다.",
            "color": "#027A48",
            "soft_color": "#ECFDF3",
            "border_color": "#ABEFC6",
        },
        "MISSING": {
            "label": "수집기 미수신",
            "title": "수집기 연결 상태를 확인해 주세요",
            "summary": "수집기의 Heartbeat가 일정 시간 수신되지 않았습니다.",
            "color": "#B54708",
            "soft_color": "#FFFAEB",
            "border_color": "#FEDF89",
        },
    }
    return values.get(
        notification,
        {
            "label": notification,
            "title": "수집기 상태 알림",
            "summary": "수집기 상태가 변경되었습니다.",
            "color": "#344054",
            "soft_color": "#F2F4F7",
            "border_color": "#D0D5DD",
        },
    )


def _collector_detail(detail: str) -> str:
    """기존 큐에 남은 장비 중심 문구를 수집기 중심 표현으로 변환한다.

    입력:
        detail: 알림 큐에 저장된 상세 설명.
    반환:
        장비와 수집기를 구분한 현업 표시 문구.
    """
    replacements = {
        "장비가 UP 상태로 복구되었습니다.": "수집기가 UP 상태로 복구되었습니다.",
        "장비가 비정상 상태를 보고했습니다.": "수집기가 비정상 상태를 보고했습니다.",
        "heartbeat 수신은 재개되었지만 장비가 비정상 상태를 보고했습니다.": (
            "Heartbeat 수신은 재개되었지만 수집기가 비정상 상태를 보고했습니다."
        ),
    }
    return replacements.get(detail, detail)


def _plain_body(
    heartbeat: HeartbeatMessage,
    presentation: dict[str, str],
    detail: str,
    generated_at_kst: str,
    consumed_at_kst: str,
) -> str:
    """HTML을 지원하지 않는 메일 클라이언트용 간결한 본문을 생성한다."""
    return (
        f"{presentation['title']}\n"
        f"{presentation['summary']}\n"
        f"{detail or '-'}\n\n"
        f"대상 장비 ID: {heartbeat.device_id}\n"
        f"수집기 호스트: {heartbeat.hostname}\n"
        f"수집기 IP: {heartbeat.ip_address}\n"
        f"수집기 프로그램: {heartbeat.program_name} {heartbeat.program_version}\n"
        f"수집기 상태: {heartbeat.status_level} / {heartbeat.status_code}\n"
        f"상태 메시지: {heartbeat.status_message}\n"
        f"수집기 보고 시각: {generated_at_kst}\n"
        f"모니터링 수신 시각: {consumed_at_kst}\n"
        f"참조 ID: {heartbeat.event_id}\n"
    )


def _html_body(
    heartbeat: HeartbeatMessage,
    presentation: dict[str, str],
    detail: str,
    generated_at_kst: str,
    consumed_at_kst: str,
) -> str:
    """Outlook 호환 테이블과 인라인 CSS로 HTML 본문을 생성한다.

    입력:
        heartbeat: 표시할 수집기 상태와 식별정보.
        presentation: 알림 유형별 문구와 색상.
        detail: 상태 판정에 대한 상세 설명.
        generated_at_kst: 수집기가 보고한 한국 시각.
        consumed_at_kst: 모니터링 서비스가 수신한 한국 시각.
    반환:
        외부 리소스가 없는 완전한 HTML 문서 문자열.
    """
    value = lambda item: escape(str(item), quote=True)
    rows = (
        ("대상 장비 ID", heartbeat.device_id),
        ("시스템 ID", heartbeat.system_id),
        ("수집기 호스트", heartbeat.hostname),
        ("수집기 IP", heartbeat.ip_address),
        ("수집기 프로그램", f"{heartbeat.program_name} {heartbeat.program_version}"),
        ("수집기 상태", f"{heartbeat.status_level} / {heartbeat.status_code}"),
        ("상태 메시지", heartbeat.status_message),
        ("수집기 보고 시각", generated_at_kst),
        ("모니터링 수신 시각", consumed_at_kst),
    )
    table_rows = "".join(
        "<tr>"
        '<td style="padding:10px 16px;border-bottom:1px solid #EAECF0;'
        'color:#667085;font-size:13px;line-height:20px;width:34%;'
        'vertical-align:top;">'
        f"{value(label)}</td>"
        '<td style="padding:10px 16px;border-bottom:1px solid #EAECF0;'
        'color:#101828;font-size:13px;line-height:20px;font-weight:600;'
        'vertical-align:top;word-break:break-word;">'
        f"{value(content)}</td></tr>"
        for label, content in rows
    )
    detail_text = value(detail or presentation["summary"])
    return f"""<!doctype html>
<html lang="ko">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width"></head>
<body style="margin:0;padding:0;background-color:#F2F4F7;">
<table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;background-color:#F2F4F7;">
  <tr>
    <td align="center" style="padding:32px 12px;">
      <table role="presentation" width="640" cellspacing="0" cellpadding="0" border="0" style="width:100%;max-width:640px;background-color:#FFFFFF;border:1px solid #EAECF0;border-radius:12px;">
        <tr>
          <td style="padding:26px 28px 22px 28px;border-top:5px solid {presentation['color']};font-family:Arial,'Malgun Gothic',sans-serif;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
              <tr>
                <td style="color:#667085;font-size:12px;line-height:18px;font-weight:bold;letter-spacing:0.5px;">IMAGE COLLECTOR MONITOR</td>
                <td align="right"><span style="display:inline-block;padding:5px 10px;background-color:{presentation['soft_color']};border:1px solid {presentation['border_color']};color:{presentation['color']};font-size:12px;line-height:16px;font-weight:bold;border-radius:12px;">{value(presentation['label'])}</span></td>
              </tr>
            </table>
            <h1 style="margin:20px 0 8px 0;color:#101828;font-size:22px;line-height:30px;font-weight:bold;">{value(presentation['title'])}</h1>
            <p style="margin:0;color:#475467;font-size:14px;line-height:22px;">{value(presentation['summary'])}</p>
          </td>
        </tr>
        <tr>
          <td style="padding:0 28px 22px 28px;font-family:Arial,'Malgun Gothic',sans-serif;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="background-color:{presentation['soft_color']};border:1px solid {presentation['border_color']};">
              <tr><td style="padding:12px 14px;color:#344054;font-size:13px;line-height:20px;">{detail_text}</td></tr>
            </table>
          </td>
        </tr>
        <tr>
          <td style="padding:0 28px 28px 28px;font-family:Arial,'Malgun Gothic',sans-serif;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="border:1px solid #EAECF0;border-collapse:collapse;">
              {table_rows}
            </table>
          </td>
        </tr>
        <tr>
          <td style="padding:18px 28px;background-color:#F9FAFB;border-top:1px solid #EAECF0;font-family:Arial,'Malgun Gothic',sans-serif;color:#98A2B3;font-size:11px;line-height:18px;">
            자동 발송된 수집기 모니터링 알림입니다. &nbsp;|&nbsp; 참조 ID: {value(heartbeat.event_id)}
          </td>
        </tr>
      </table>
    </td>
  </tr>
</table>
</body>
</html>"""


def _format_datetime_kst(value: datetime) -> str:
    """timezone-aware datetime을 사람이 읽기 쉬운 한국 시각으로 변환한다.

    입력:
        value: 변환할 datetime. timezone 정보가 없으면 UTC로 간주한다.
    반환:
        ``YYYY-MM-DD HH:MM:SS KST`` 형식 문자열.
    """
    aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    converted = aware.astimezone(KST)
    return f"{converted.strftime('%Y-%m-%d %H:%M:%S')} KST"


def _format_iso_time_kst(value: str) -> str:
    """RFC 3339/ISO 8601 문자열을 한국 시각 표시 문자열로 변환한다.

    입력:
        value: ``Z`` 또는 UTC offset을 포함한 날짜·시간 문자열.
    반환:
        파싱 성공 시 KST 문자열, 실패 시 원문 문자열.
    """
    try:
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
        return _format_datetime_kst(parsed)
    except (TypeError, ValueError):
        return value

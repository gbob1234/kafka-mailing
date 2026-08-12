import logging

from heartbeat_mailer.config import Settings
from heartbeat_mailer.runner import HeartbeatNotifier


def main() -> None:
    """애플리케이션 설정을 읽고 heartbeat 감시 루프를 시작한다.

    입력:
        없음. 설정값은 환경변수 또는 로컬 ``.env`` 파일에서 읽는다.
    반환:
        없음. 종료 신호를 받을 때까지 현재 프로세스에서 실행된다.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    settings = Settings.from_env()
    HeartbeatNotifier(settings).run()


if __name__ == "__main__":
    main()

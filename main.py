import logging

from heartbeat_mailer.config import Settings
from heartbeat_mailer.runner import HeartbeatNotifier


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    settings = Settings.from_env()
    HeartbeatNotifier(settings).run()


if __name__ == "__main__":
    main()


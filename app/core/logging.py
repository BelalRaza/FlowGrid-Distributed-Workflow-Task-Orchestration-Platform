import logging

from app.core.config import get_settings


def configure_logging() -> None:
    """Configure a simple, consistent log format for API and worker processes."""
    logging.basicConfig(
        level=get_settings().log_level.upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

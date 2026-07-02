"""Structured logging setup for the ETL pipeline.

Provides a factory function that creates pre-configured loggers with
both console (colored) and rotating file handlers. All loggers write
to a shared ``logs/etl_pipeline.log`` file with automatic rotation.

Example:
    >>> from src.utils.logger import get_logger
    >>> logger = get_logger(__name__)
    >>> logger.info("Pipeline started")
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_LOG_DIR: Path = Path(__file__).resolve().parent.parent.parent / "logs"
_LOG_FILE: Path = _LOG_DIR / "etl_pipeline.log"

_CONSOLE_FORMAT: str = "[%(asctime)s] %(levelname)s - %(name)s - %(message)s"
_FILE_FORMAT: str = (
    "[%(asctime)s] %(levelname)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s"
)
_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"

_MAX_BYTES: int = 5 * 1024 * 1024  # 5 MB
_BACKUP_COUNT: int = 3

# ANSI color codes for console output
_COLORS: dict[int, str] = {
    logging.DEBUG: "\033[36m",     # Cyan
    logging.INFO: "\033[32m",      # Green
    logging.WARNING: "\033[33m",   # Yellow
    logging.ERROR: "\033[31m",     # Red
    logging.CRITICAL: "\033[1;31m",  # Bold Red
}
_RESET: str = "\033[0m"


# ---------------------------------------------------------------------------
# Custom Formatter
# ---------------------------------------------------------------------------


class ColoredFormatter(logging.Formatter):
    """A logging formatter that adds ANSI color codes to console output.

    Each log level is assigned a distinct color for quick visual
    identification in terminal output.

    Attributes:
        _use_color: Whether to apply ANSI color codes. Disabled when
            stdout is not a TTY (e.g., piped to a file).
    """

    def __init__(self, fmt: str, datefmt: str | None = None) -> None:
        """Initialize the colored formatter.

        Args:
            fmt: Log message format string.
            datefmt: Date/time format string, or None for default.
        """
        super().__init__(fmt=fmt, datefmt=datefmt)
        self._use_color: bool = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record with optional ANSI color codes.

        Args:
            record: The log record to format.

        Returns:
            Formatted (and optionally colored) log message string.
        """
        message: str = super().format(record)

        if self._use_color:
            color: str = _COLORS.get(record.levelno, "")
            return f"{color}{message}{_RESET}"

        return message


# ---------------------------------------------------------------------------
# Logger Factory
# ---------------------------------------------------------------------------


def _resolve_log_level() -> int:
    """Determine the effective log level from settings or environment.

    Priority order:
        1. ``LOG_LEVEL`` environment variable (if set).
        2. ``Settings.log_level`` from pydantic-settings.
        3. Falls back to ``INFO`` if neither is available.

    Returns:
        Numeric logging level (e.g., ``logging.INFO``).
    """
    level_name: str = os.environ.get("LOG_LEVEL", "")

    if not level_name:
        try:
            from src.utils.config import get_settings

            level_name = get_settings().log_level
        except Exception:  # noqa: BLE001
            level_name = "INFO"

    return getattr(logging, level_name.upper(), logging.INFO)


def get_logger(name: str) -> logging.Logger:
    """Create and return a configured logger instance.

    Each logger gets two handlers (added only once):
        - **Console handler**: Colored output to ``sys.stdout``.
        - **File handler**: Rotating file at ``logs/etl_pipeline.log``
          (5 MB per file, 3 backups).

    Args:
        name: Logger name, typically ``__name__`` of the calling module.

    Returns:
        A fully configured ``logging.Logger`` instance.

    Example:
        >>> logger = get_logger("src.extractors.api")
        >>> logger.info("Fetching data from API")
    """
    logger: logging.Logger = logging.getLogger(name)

    # Prevent adding duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    log_level: int = _resolve_log_level()
    logger.setLevel(log_level)

    # --- Console Handler ---
    console_handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)  # type: ignore[type-arg]
    console_handler.setLevel(log_level)
    console_handler.setFormatter(
        ColoredFormatter(fmt=_CONSOLE_FORMAT, datefmt=_DATE_FORMAT)
    )
    logger.addHandler(console_handler)

    # --- File Handler ---
    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    file_handler: RotatingFileHandler = RotatingFileHandler(
        filename=str(_LOG_FILE),
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(
        logging.Formatter(fmt=_FILE_FORMAT, datefmt=_DATE_FORMAT)
    )
    logger.addHandler(file_handler)

    # Prevent log propagation to the root logger
    logger.propagate = False

    return logger

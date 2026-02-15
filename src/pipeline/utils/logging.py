"""Logging setup with Rich console and file handlers."""

import logging

from rich.console import Console
from rich.logging import RichHandler

from pipeline.config import LOG_FILE

console = Console()

_configured = False


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure and return the pipeline logger."""
    global _configured

    logger = logging.getLogger("pipeline")

    if _configured:
        return logger

    logger.setLevel(level)

    # Rich console handler
    console_handler = RichHandler(console=console, show_path=False, markup=True)
    console_handler.setLevel(level)

    # File handler
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s — %(message)s")
    file_handler.setFormatter(file_fmt)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    _configured = True
    return logger

"""
Logging configuration for gh-autofollow.

Sets up:
  - Rotating file handler (in config.log_dir)
  - Console handler (if not running as daemon)
  - Structured JSON formatting option
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from gh_autofollow.config import Config


class _JSONFormatter(logging.Formatter):
    """Emit log records as newline-delimited JSON."""

    def format(self, record: logging.LogRecord) -> str:
        data = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            data["exc"] = self.formatException(record.exc_info)
        return json.dumps(data)


def setup_logging(
    config: Config,
    daemon: bool = False,
    json_format: bool = False,
) -> None:
    """
    Configure root logger for the gh-autofollow namespace.

    :param config:      Loaded Config object.
    :param daemon:      If True, suppress console output.
    :param json_format: If True, use JSON structured logging to file.
    """
    Path(config.log_dir).mkdir(parents=True, exist_ok=True)

    level = getattr(logging, config.log_level.upper(), logging.INFO)

    root = logging.getLogger("gh_autofollow")
    root.setLevel(level)
    root.handlers.clear()
    root.propagate = False

    # ── File handler ──────────────────────────────────────────────────────────
    file_handler = logging.handlers.RotatingFileHandler(
        filename=config.log_path,
        maxBytes=config.log_max_bytes,
        backupCount=config.log_backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    if json_format:
        file_handler.setFormatter(_JSONFormatter())
    else:
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    root.addHandler(file_handler)

    # ── Console handler ───────────────────────────────────────────────────────
    if not daemon:
        console = logging.StreamHandler(sys.stderr)
        console.setLevel(level)
        console.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)-8s] %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        root.addHandler(console)

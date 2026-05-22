"""Logging configuration: timestamped file + console, per-source log dir."""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path


def setup_logging(name: str, log_root: Path, level: int = logging.INFO) -> logging.Logger:
    log_root.mkdir(parents=True, exist_ok=True)
    log_file = log_root / f"{name}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    root = logging.getLogger()
    root.setLevel(level)
    # Clear handlers so repeated calls don't duplicate
    root.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(ch)

    logger = logging.getLogger(name)
    logger.info(f"Logging to {log_file}")
    return logger

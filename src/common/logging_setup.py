"""
src/common/logging_setup.py
────────────────────────────
Structured JSON logging for the pipeline.
Every log record includes timestamps, level, logger name, and any extra
context fields bound via `logger.bind(partition=0, sku="85123A")`.

Usage:
    from src.common.logging_setup import get_logger
    log = get_logger(__name__)
    log.info("consumed event", partition=0, offset=123, sku="85123A")
"""
from __future__ import annotations

import logging
import sys
from typing import Any

try:
    import structlog  # preferred: pip install structlog
    _USE_STRUCTLOG = True
except ImportError:
    _USE_STRUCTLOG = False


def _configure_structlog() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(sys.stdout),
        cache_logger_on_first_use=True,
    )


def setup_logging(level: str = "INFO") -> None:
    """Call once at process start."""
    numeric = getattr(logging, level.upper(), logging.INFO)
    if _USE_STRUCTLOG:
        logging.basicConfig(format="%(message)s", stream=sys.stdout, level=numeric)
        _configure_structlog()
    else:
        logging.basicConfig(
            format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
            stream=sys.stdout,
            level=numeric,
        )


class _StdlibBoundLogger:
    """Thin wrapper around stdlib logger that accepts keyword context."""

    def __init__(self, name: str) -> None:
        self._log = logging.getLogger(name)

    def _msg(self, level: int, msg: str, **kw: Any) -> None:
        extra = " ".join(f"{k}={v}" for k, v in kw.items())
        self._log.log(level, f"{msg} {extra}".rstrip())

    def debug(self, msg: str, **kw: Any) -> None:
        self._msg(logging.DEBUG, msg, **kw)

    def info(self, msg: str, **kw: Any) -> None:
        self._msg(logging.INFO, msg, **kw)

    def warning(self, msg: str, **kw: Any) -> None:
        self._msg(logging.WARNING, msg, **kw)

    def error(self, msg: str, **kw: Any) -> None:
        self._msg(logging.ERROR, msg, **kw)

    def exception(self, msg: str, **kw: Any) -> None:
        self._msg(logging.ERROR, msg, exc_info=True, **kw)

    def bind(self, **kw: Any) -> "_StdlibBoundLogger":
        """Return self (context not propagated in stdlib fallback)."""
        return self


def get_logger(name: str) -> Any:
    """Return a structlog logger if available, else a stdlib fallback."""
    if _USE_STRUCTLOG:
        return structlog.get_logger(name)
    return _StdlibBoundLogger(name)

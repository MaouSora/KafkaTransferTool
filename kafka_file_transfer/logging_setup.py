"""日志初始化。"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .config import LoggingConfig
from .version import APP_NAME, __version__


def setup_logging(cfg: LoggingConfig) -> logging.Logger:
    """按配置初始化根日志，并返回应用 logger。"""
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(cfg.level)

    formatter = logging.Formatter(fmt=cfg.format, datefmt=cfg.datefmt)

    if cfg.console:
        console = logging.StreamHandler()
        console.setLevel(cfg.level)
        console.setFormatter(formatter)
        root.addHandler(console)

    if cfg.file:
        log_path = Path(cfg.file).expanduser().resolve()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=cfg.max_bytes,
            backupCount=cfg.backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(cfg.level)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    for name in cfg.quiet_loggers:
        logging.getLogger(name).setLevel(logging.WARNING)

    app_logger = logging.getLogger(APP_NAME)
    app_logger.debug(
        "日志已初始化 level=%s console=%s file=%s",
        cfg.level,
        cfg.console,
        cfg.file,
    )
    return app_logger


def log_startup(
    logger: logging.Logger,
    *,
    command: str,
    config_path: Path,
    config_version: str,
) -> None:
    logger.info(
        "%s v%s 启动 | command=%s | config=%s | config.version=%s",
        APP_NAME,
        __version__,
        command,
        config_path,
        config_version,
    )

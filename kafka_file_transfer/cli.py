"""命令行入口：仅选择 send/receive，其余全部由配置文件提供。"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from . import __version__
from .config import AppConfig, ConfigError, load_config
from .logging_setup import log_startup, setup_logging
from .receiver import FileReceiver
from .sender import FileSender
from .version import APP_NAME

DEFAULT_CONFIG_PATH = "config.yaml"
logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=APP_NAME,
        description=(
            "通过 Kafka 指定 Topic 发送/接收文件（支持 zip、h5 等常见格式）。"
            "所有运行参数均在配置文件中设置。"
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "-c",
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"YAML 配置文件路径（默认: {DEFAULT_CONFIG_PATH}）",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("send", help="按配置文件发送文件")
    sub.add_parser("receive", help="按配置文件接收文件")
    return parser


def cmd_send(cfg: AppConfig) -> int:
    if cfg.send is None:
        raise ConfigError("发送模式需要配置 send.file")

    file_path = Path(cfg.send.file).expanduser()
    logger.info(
        "准备发送 file=%s topic=%s brokers=%s chunk_size=%s",
        file_path,
        cfg.kafka.topic,
        cfg.kafka.brokers,
        cfg.transfer.chunk_size,
    )

    def on_progress(done: int, total: int, nbytes: int) -> None:
        percent = (done / total) * 100 if total else 100.0
        if done == total or done == 1 or done % max(1, total // 10) == 0:
            logger.info(
                "发送进度 %s/%s (%.1f%%) chunk_bytes=%s",
                done,
                total,
                percent,
                nbytes,
            )

    with FileSender(
        cfg.kafka.brokers,
        cfg.kafka.topic,
        chunk_size=cfg.transfer.chunk_size,
        acks=cfg.kafka.acks,
        retries=cfg.kafka.retries,
        max_request_size=cfg.kafka.max_request_size,
        send_timeout=cfg.kafka.send_timeout,
        extra_producer_config=cfg.kafka.producer_extra,
    ) as sender:
        meta = sender.send_file(file_path, progress=on_progress)

    logger.info(
        "发送成功 filename=%s size=%s chunks=%s sha256=%s file_id=%s content_type=%s",
        meta.filename,
        meta.size,
        meta.total_chunks,
        meta.sha256,
        meta.file_id,
        meta.content_type,
    )
    return 0


def cmd_receive(cfg: AppConfig) -> int:
    if cfg.receive is None:
        raise ConfigError("接收模式需要配置 receive.output_dir 等接收参数")

    recv = cfg.receive
    logger.info(
        "准备接收 topic=%s brokers=%s output_dir=%s group_id=%s "
        "idle_timeout=%s max_files=%s auto_offset_reset=%s",
        cfg.kafka.topic,
        cfg.kafka.brokers,
        recv.output_dir,
        recv.group_id,
        recv.idle_timeout,
        recv.max_files,
        recv.auto_offset_reset,
    )

    def on_file(path: Path, meta) -> None:
        logger.info(
            "已保存 path=%s size=%s content_type=%s sha256=%s file_id=%s",
            path,
            meta.size,
            meta.content_type,
            meta.sha256,
            meta.file_id,
        )

    with FileReceiver(
        cfg.kafka.brokers,
        cfg.kafka.topic,
        recv.output_dir,
        group_id=recv.group_id,
        auto_offset_reset=recv.auto_offset_reset,
        idle_timeout=recv.idle_timeout,
        poll_timeout_ms=cfg.kafka.poll_timeout_ms,
        extra_consumer_config=cfg.kafka.consumer_extra,
    ) as receiver:
        saved = receiver.receive_forever(on_file=on_file, max_files=recv.max_files)

    logger.info("接收结束 total_files=%s", len(saved))
    return 0


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"配置错误: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    except Exception as exc:
        print(f"读取配置失败: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    app_logger = setup_logging(cfg.logging)
    log_startup(
        app_logger,
        command=args.command,
        config_path=cfg.path,
        config_version=cfg.config_version,
    )
    if cfg.config_version != cfg.app_version:
        app_logger.warning(
            "配置文件 version=%s 与程序版本 %s 不一致，请确认配置兼容性",
            cfg.config_version,
            cfg.app_version,
        )

    try:
        if args.command == "send":
            code = cmd_send(cfg)
        elif args.command == "receive":
            code = cmd_receive(cfg)
        else:
            parser.error(f"未知命令: {args.command}")
            code = 2
    except ConfigError as exc:
        logger.error("配置错误: %s", exc)
        raise SystemExit(2) from exc
    except SystemExit:
        raise
    except Exception as exc:
        logger.exception("执行失败: %s", exc)
        raise SystemExit(1) from exc

    logger.info("%s v%s 正常退出 code=%s", APP_NAME, __version__, code)
    raise SystemExit(code)


if __name__ == "__main__":
    main()

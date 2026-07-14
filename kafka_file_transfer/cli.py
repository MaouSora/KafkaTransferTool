"""命令行入口。"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

import yaml

from . import __version__
from .chunker import DEFAULT_CHUNK_SIZE
from .receiver import FileReceiver
from .sender import FileSender


def _load_config(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    config_path = Path(path).expanduser().resolve()
    if not config_path.is_file():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    with config_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError("配置文件根节点必须是映射对象")
    return data


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("-c", "--config", help="YAML 配置文件路径")
    parser.add_argument("-b", "--brokers", help="Kafka brokers，逗号分隔，如 localhost:9092")
    parser.add_argument("-t", "--topic", help="传输使用的 Kafka topic")
    parser.add_argument("-v", "--verbose", action="store_true", help="输出调试日志")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kafka-file-transfer",
        description="通过 Kafka 指定 Topic 发送/接收文件（支持 zip、h5 等常见格式）",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    send = sub.add_parser("send", help="发送文件到 Kafka topic")
    _add_common_args(send)
    send.add_argument("-f", "--file", help="待发送文件路径")
    send.add_argument(
        "--chunk-size",
        type=int,
        help=f"分片大小（字节），默认 {DEFAULT_CHUNK_SIZE}",
    )

    recv = sub.add_parser("receive", help="从 Kafka topic 接收文件")
    _add_common_args(recv)
    recv.add_argument("-o", "--output-dir", help="文件保存目录")
    recv.add_argument("-g", "--group-id", help="Kafka 消费组 ID")
    recv.add_argument(
        "--idle-timeout",
        type=float,
        help="空闲超时秒数；0 表示持续监听，默认 30",
    )
    recv.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="最多接收文件数后退出；默认不限制",
    )
    recv.add_argument(
        "--from-beginning",
        action="store_true",
        help="从最早 offset 开始消费（auto_offset_reset=earliest）",
    )
    return parser


def _require(value: Any, name: str) -> Any:
    if value is None or value == "":
        raise SystemExit(f"缺少必要参数: {name}")
    return value


def cmd_send(args: argparse.Namespace, config: dict[str, Any]) -> int:
    brokers = _require(args.brokers or config.get("brokers"), "--brokers")
    topic = _require(args.topic or config.get("topic"), "--topic")
    file_path = _require(args.file or config.get("file"), "--file")
    chunk_size = args.chunk_size or int(config.get("chunk_size") or DEFAULT_CHUNK_SIZE)

    def on_progress(done: int, total: int, nbytes: int) -> None:
        percent = (done / total) * 100 if total else 100.0
        print(f"\r进度: {done}/{total} ({percent:.1f}%)", end="", flush=True)

    with FileSender(brokers, topic, chunk_size=chunk_size) as sender:
        meta = sender.send_file(file_path, progress=on_progress)
    print()
    print(
        f"发送成功: {meta.filename} | size={meta.size} | "
        f"chunks={meta.total_chunks} | sha256={meta.sha256} | file_id={meta.file_id}"
    )
    return 0


def cmd_receive(args: argparse.Namespace, config: dict[str, Any]) -> int:
    brokers = _require(args.brokers or config.get("brokers"), "--brokers")
    topic = _require(args.topic or config.get("topic"), "--topic")
    output_dir = _require(args.output_dir or config.get("output_dir"), "--output-dir")
    group_id = args.group_id or config.get("group_id") or "kafka-file-transfer-receiver"
    if args.idle_timeout is not None:
        idle_timeout = args.idle_timeout
    else:
        idle_timeout = float(config.get("idle_timeout", 30))
    auto_offset_reset = "earliest" if args.from_beginning else "latest"

    def on_file(path: Path, meta) -> None:
        print(
            f"已保存: {path} | size={meta.size} | "
            f"type={meta.content_type} | sha256={meta.sha256}"
        )

    with FileReceiver(
        brokers,
        topic,
        output_dir,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        idle_timeout=idle_timeout,
    ) as receiver:
        saved = receiver.receive_forever(on_file=on_file, max_files=args.max_files)
    print(f"本次共接收 {len(saved)} 个文件")
    return 0


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    try:
        config = _load_config(args.config)
    except Exception as exc:
        print(f"读取配置失败: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    try:
        if args.command == "send":
            code = cmd_send(args, config)
        elif args.command == "receive":
            code = cmd_receive(args, config)
        else:
            parser.error(f"未知命令: {args.command}")
            code = 2
    except SystemExit:
        raise
    except Exception as exc:
        logging.getLogger(__name__).exception("执行失败")
        print(f"执行失败: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    raise SystemExit(code)


if __name__ == "__main__":
    main()

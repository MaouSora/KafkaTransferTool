"""配置加载与校验。所有运行参数均来自 Python 配置模块（settings.py）。"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

from .chunker import DEFAULT_CHUNK_SIZE
from .version import __version__


class ConfigError(ValueError):
    """配置非法。"""


@dataclass(frozen=True)
class KafkaConfig:
    brokers: str
    topic: str
    acks: str | int = "all"
    retries: int = 3
    max_request_size: int | None = None
    send_timeout: float = 60.0
    poll_timeout_ms: int = 1000
    producer_extra: dict[str, Any] = field(default_factory=dict)
    consumer_extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TransferConfig:
    chunk_size: int = DEFAULT_CHUNK_SIZE


@dataclass(frozen=True)
class SendConfig:
    file: str


@dataclass(frozen=True)
class ReceiveConfig:
    output_dir: str
    group_id: str = "kafka-file-transfer-receiver"
    idle_timeout: float = 30.0
    max_files: int | None = None
    auto_offset_reset: str = "earliest"


@dataclass(frozen=True)
class LoggingConfig:
    level: str = "INFO"
    format: str = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
    datefmt: str = "%Y-%m-%d %H:%M:%S"
    console: bool = True
    file: str | None = "./logs/kafka-file-transfer.log"
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 5
    quiet_loggers: tuple[str, ...] = ("kafka", "urllib3")


@dataclass(frozen=True)
class AppConfig:
    """完整应用配置。"""

    path: Path
    config_version: str
    kafka: KafkaConfig
    transfer: TransferConfig
    logging: LoggingConfig
    send: SendConfig | None = None
    receive: ReceiveConfig | None = None

    @property
    def app_version(self) -> str:
        return __version__


_SECTION_ALIASES = {
    "kafka": ("KAFKA", "Kafka"),
    "transfer": ("TRANSFER", "Transfer"),
    "send": ("SEND", "Send"),
    "receive": ("RECEIVE", "Receive"),
    "logging": ("LOGGING", "Logging"),
}


def _as_dict(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, type):
        return {
            k: v
            for k, v in vars(value).items()
            if not k.startswith("_") and not callable(v)
        }
    # class instance / SimpleNamespace / 普通对象
    if hasattr(value, "__dict__"):
        return {
            k: v
            for k, v in vars(value).items()
            if not k.startswith("_") and not callable(v)
        }
    raise ConfigError(f"配置项 '{name}' 必须是 dict 或 class")


def _require_str(data: dict[str, Any], key: str, section: str) -> str:
    value = data.get(key)
    if value is None or str(value).strip() == "":
        raise ConfigError(f"缺少必要配置: {section}.{key}")
    return str(value).strip()


def _optional_int(data: dict[str, Any], key: str, default: int | None) -> int | None:
    if key not in data or data[key] is None:
        return default
    try:
        return int(data[key])
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"配置项 '{key}' 必须是整数") from exc


def _optional_float(data: dict[str, Any], key: str, default: float) -> float:
    if key not in data or data[key] is None:
        return default
    try:
        return float(data[key])
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"配置项 '{key}' 必须是数字") from exc


def _load_module(path: Path) -> ModuleType:
    if path.suffix != ".py":
        raise ConfigError(f"配置必须是 Python 文件（.py）: {path}")
    module_name = f"kft_user_settings_{abs(hash(str(path)))}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ConfigError(f"无法加载配置模块: {path}")
    module = importlib.util.module_from_spec(spec)
    # 允许配置文件内使用相对路径时基于其所在目录
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        raise ConfigError(f"执行配置模块失败: {path}: {exc}") from exc
    return module


def _section_from_module(module: ModuleType, section: str) -> Any:
    for name in _SECTION_ALIASES[section]:
        if hasattr(module, name):
            return getattr(module, name)
    return None


def module_to_raw(module: ModuleType) -> dict[str, Any]:
    """将 Python 配置模块转为内部统一字典结构。"""
    raw: dict[str, Any] = {}
    version = getattr(module, "VERSION", None)
    if version is None:
        version = getattr(module, "version", None)
    if version is not None:
        raw["version"] = version

    for section in _SECTION_ALIASES:
        value = _section_from_module(module, section)
        if value is not None:
            raw[section] = _as_dict(value, section)

    # 兼容扁平字段（直接写在模块顶层）
    for key in (
        "brokers",
        "topic",
        "chunk_size",
        "file",
        "output_dir",
        "group_id",
        "idle_timeout",
        "max_files",
        "auto_offset_reset",
        "from_beginning",
    ):
        if hasattr(module, key) and key not in raw:
            raw[key] = getattr(module, key)
        upper = key.upper()
        if hasattr(module, upper) and key not in raw:
            raw[key] = getattr(module, upper)

    return raw


def load_raw_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path).expanduser().resolve()
    if not config_path.is_file():
        raise ConfigError(f"配置文件不存在: {config_path}")
    module = _load_module(config_path)
    return module_to_raw(module)


def parse_config(data: dict[str, Any], *, path: Path | None = None) -> AppConfig:
    """解析并校验配置字典。"""
    config_version = str(data.get("version") or __version__)

    kafka_raw = _as_dict(data.get("kafka"), "kafka")
    if "brokers" in data and "brokers" not in kafka_raw:
        kafka_raw["brokers"] = data["brokers"]
    if "topic" in data and "topic" not in kafka_raw:
        kafka_raw["topic"] = data["topic"]

    transfer_raw = _as_dict(data.get("transfer"), "transfer")
    if "chunk_size" in data and "chunk_size" not in transfer_raw:
        transfer_raw["chunk_size"] = data["chunk_size"]

    send_raw = _as_dict(data.get("send"), "send")
    if "file" in data and "file" not in send_raw:
        send_raw["file"] = data["file"]

    receive_raw = _as_dict(data.get("receive"), "receive")
    for key in ("output_dir", "group_id", "idle_timeout", "max_files", "auto_offset_reset"):
        if key in data and key not in receive_raw:
            receive_raw[key] = data[key]
    if data.get("from_beginning") is True and "auto_offset_reset" not in receive_raw:
        receive_raw["auto_offset_reset"] = "earliest"

    logging_raw = _as_dict(data.get("logging"), "logging")

    kafka = KafkaConfig(
        brokers=_require_str(kafka_raw, "brokers", "kafka"),
        topic=_require_str(kafka_raw, "topic", "kafka"),
        acks=kafka_raw.get("acks", "all"),
        retries=int(kafka_raw.get("retries", 3)),
        max_request_size=_optional_int(kafka_raw, "max_request_size", None),
        send_timeout=_optional_float(kafka_raw, "send_timeout", 60.0),
        poll_timeout_ms=int(kafka_raw.get("poll_timeout_ms", 1000)),
        producer_extra=_as_dict(kafka_raw.get("producer_extra"), "kafka.producer_extra"),
        consumer_extra=_as_dict(kafka_raw.get("consumer_extra"), "kafka.consumer_extra"),
    )

    chunk_size = int(transfer_raw.get("chunk_size", DEFAULT_CHUNK_SIZE))
    if chunk_size <= 0:
        raise ConfigError("transfer.chunk_size 必须大于 0")
    transfer = TransferConfig(chunk_size=chunk_size)

    send: SendConfig | None = None
    if send_raw:
        send = SendConfig(file=_require_str(send_raw, "file", "send"))

    receive: ReceiveConfig | None = None
    if receive_raw:
        auto_offset_reset = str(receive_raw.get("auto_offset_reset", "earliest")).lower()
        if auto_offset_reset not in {"earliest", "latest"}:
            raise ConfigError("receive.auto_offset_reset 只能是 earliest 或 latest")
        max_files = receive_raw.get("max_files")
        if max_files is not None:
            max_files = int(max_files)
            if max_files <= 0:
                raise ConfigError("receive.max_files 必须为正整数，或不配置/设为 None")
        receive = ReceiveConfig(
            output_dir=_require_str(receive_raw, "output_dir", "receive"),
            group_id=str(receive_raw.get("group_id") or "kafka-file-transfer-receiver"),
            idle_timeout=_optional_float(receive_raw, "idle_timeout", 30.0),
            max_files=max_files,
            auto_offset_reset=auto_offset_reset,
        )

    quiet = logging_raw.get("quiet_loggers", ["kafka", "urllib3"])
    if quiet is None:
        quiet_tuple: tuple[str, ...] = ()
    elif isinstance(quiet, (list, tuple)):
        quiet_tuple = tuple(str(x) for x in quiet)
    else:
        raise ConfigError("logging.quiet_loggers 必须是字符串列表/元组")

    logging_cfg = LoggingConfig(
        level=str(logging_raw.get("level", "INFO")).upper(),
        format=str(
            logging_raw.get(
                "format",
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            )
        ),
        datefmt=str(logging_raw.get("datefmt", "%Y-%m-%d %H:%M:%S")),
        console=bool(logging_raw.get("console", True)),
        file=(
            None
            if logging_raw.get("file") in (None, "", False)
            else str(logging_raw.get("file"))
        ),
        max_bytes=int(logging_raw.get("max_bytes", 10 * 1024 * 1024)),
        backup_count=int(logging_raw.get("backup_count", 5)),
        quiet_loggers=quiet_tuple,
    )
    if logging_cfg.level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ConfigError(
            "logging.level 必须是 DEBUG/INFO/WARNING/ERROR/CRITICAL 之一"
        )
    if not logging_cfg.console and not logging_cfg.file:
        raise ConfigError("logging.console 与 logging.file 不能同时关闭")

    return AppConfig(
        path=path or Path("."),
        config_version=config_version,
        kafka=kafka,
        transfer=transfer,
        logging=logging_cfg,
        send=send,
        receive=receive,
    )


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).expanduser().resolve()
    raw = load_raw_config(config_path)
    return parse_config(raw, path=config_path)

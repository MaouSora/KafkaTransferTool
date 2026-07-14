from pathlib import Path

import pytest

from kafka_file_transfer.config import ConfigError, load_config, parse_config
from kafka_file_transfer.version import __version__


def _write_settings(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


DICT_SETTINGS = '''
VERSION = "1.0.0"
KAFKA = {
    "brokers": "localhost:9092",
    "topic": "file-transfer",
    "send_timeout": 30,
}
TRANSFER = {"chunk_size": 1024}
SEND = {"file": "./a.zip"}
RECEIVE = {
    "output_dir": "./out",
    "group_id": "g1",
    "idle_timeout": 0,
    "max_files": 2,
    "auto_offset_reset": "latest",
}
LOGGING = {"level": "DEBUG", "console": True, "file": None}
'''


CLASS_SETTINGS = '''
VERSION = "1.0.0"

class Kafka:
    brokers = "b:9092"
    topic = "tt"

class Transfer:
    chunk_size = 2048

class Send:
    file = "x.h5"

class Receive:
    output_dir = "./r"
    auto_offset_reset = "earliest"

class Logging:
    level = "INFO"
    console = True
    file = None
'''


def test_load_dict_settings(tmp_path: Path):
    path = _write_settings(tmp_path / "settings.py", DICT_SETTINGS)
    cfg = load_config(path)
    assert cfg.app_version == __version__
    assert cfg.config_version == "1.0.0"
    assert cfg.path == path.resolve()
    assert cfg.kafka.brokers == "localhost:9092"
    assert cfg.kafka.send_timeout == 30
    assert cfg.transfer.chunk_size == 1024
    assert cfg.send is not None and cfg.send.file == "./a.zip"
    assert cfg.receive is not None
    assert cfg.receive.max_files == 2
    assert cfg.receive.auto_offset_reset == "latest"
    assert cfg.logging.level == "DEBUG"
    assert cfg.logging.file is None


def test_load_class_settings(tmp_path: Path):
    path = _write_settings(tmp_path / "class_settings.py", CLASS_SETTINGS)
    cfg = load_config(path)
    assert cfg.kafka.brokers == "b:9092"
    assert cfg.transfer.chunk_size == 2048
    assert cfg.send is not None and cfg.send.file == "x.h5"
    assert cfg.receive is not None
    assert cfg.receive.auto_offset_reset == "earliest"


def test_missing_brokers():
    with pytest.raises(ConfigError, match="kafka.brokers"):
        parse_config({"kafka": {"topic": "t"}, "transfer": {}})


def test_invalid_log_level():
    with pytest.raises(ConfigError, match="logging.level"):
        parse_config(
            {
                "kafka": {"brokers": "a:9092", "topic": "t"},
                "logging": {"level": "VERBOSE", "console": True, "file": None},
            }
        )


def test_flat_compat_config():
    cfg = parse_config(
        {
            "brokers": "b:9092",
            "topic": "tt",
            "chunk_size": 2048,
            "file": "x.h5",
            "output_dir": "./r",
            "from_beginning": True,
        }
    )
    assert cfg.kafka.brokers == "b:9092"
    assert cfg.transfer.chunk_size == 2048
    assert cfg.send is not None and cfg.send.file == "x.h5"
    assert cfg.receive is not None
    assert cfg.receive.auto_offset_reset == "earliest"


def test_reject_non_py(tmp_path: Path):
    path = tmp_path / "c.yaml"
    path.write_text("brokers: x\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="Python 文件"):
        load_config(path)


def test_logging_must_have_output():
    with pytest.raises(ConfigError, match="不能同时关闭"):
        parse_config(
            {
                "kafka": {"brokers": "a:9092", "topic": "t"},
                "logging": {"console": False, "file": None},
            }
        )

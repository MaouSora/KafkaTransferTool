from pathlib import Path

import pytest

from kafka_file_transfer.config import ConfigError, load_config, parse_config
from kafka_file_transfer.version import __version__


FULL_CONFIG = """
version: "1.0.0"
kafka:
  brokers: "localhost:9092"
  topic: "file-transfer"
  send_timeout: 30
transfer:
  chunk_size: 1024
send:
  file: "./a.zip"
receive:
  output_dir: "./out"
  group_id: "g1"
  idle_timeout: 0
  max_files: 2
  auto_offset_reset: latest
logging:
  level: DEBUG
  console: true
  file: null
"""


def test_parse_full_config():
    cfg = parse_config(__import__("yaml").safe_load(FULL_CONFIG))
    assert cfg.app_version == __version__
    assert cfg.config_version == "1.0.0"
    assert cfg.kafka.brokers == "localhost:9092"
    assert cfg.kafka.topic == "file-transfer"
    assert cfg.kafka.send_timeout == 30
    assert cfg.transfer.chunk_size == 1024
    assert cfg.send is not None and cfg.send.file == "./a.zip"
    assert cfg.receive is not None
    assert cfg.receive.output_dir == "./out"
    assert cfg.receive.max_files == 2
    assert cfg.receive.auto_offset_reset == "latest"
    assert cfg.logging.level == "DEBUG"
    assert cfg.logging.file is None


def test_load_config_file(tmp_path: Path):
    path = tmp_path / "config.yaml"
    path.write_text(FULL_CONFIG, encoding="utf-8")
    cfg = load_config(path)
    assert cfg.path == path.resolve()
    assert cfg.kafka.topic == "file-transfer"


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


def test_logging_must_have_output():
    with pytest.raises(ConfigError, match="不能同时关闭"):
        parse_config(
            {
                "kafka": {"brokers": "a:9092", "topic": "t"},
                "logging": {"console": False, "file": None},
            }
        )

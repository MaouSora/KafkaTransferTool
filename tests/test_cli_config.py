from pathlib import Path

import pytest

from kafka_file_transfer.cli import build_parser
from kafka_file_transfer.config import load_config
from kafka_file_transfer.logging_setup import setup_logging
from kafka_file_transfer.version import __version__


def test_parser_config_only():
    parser = build_parser()
    send_args = parser.parse_args(["-c", "my.yaml", "send"])
    assert send_args.command == "send"
    assert send_args.config == "my.yaml"

    recv_args = parser.parse_args(["receive"])
    assert recv_args.command == "receive"
    assert recv_args.config == "config.yaml"


def test_parser_rejects_old_flags():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["send", "-b", "localhost:9092"])


def test_version_option(capsys):
    parser = build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["--version"])
    assert exc.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_setup_logging_file(tmp_path: Path):
    cfg_path = tmp_path / "c.yaml"
    log_file = tmp_path / "app.log"
    cfg_path.write_text(
        f"""
version: "{__version__}"
kafka:
  brokers: "localhost:9092"
  topic: "t"
transfer:
  chunk_size: 1024
send:
  file: "./a.zip"
receive:
  output_dir: "./out"
logging:
  level: INFO
  console: false
  file: "{log_file.as_posix()}"
""",
        encoding="utf-8",
    )
    cfg = load_config(cfg_path)
    logger = setup_logging(cfg.logging)
    logger.info("hello-log")
    assert log_file.exists()
    assert "hello-log" in log_file.read_text(encoding="utf-8")

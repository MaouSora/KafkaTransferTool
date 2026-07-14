from pathlib import Path

import pytest

from kafka_file_transfer.cli import build_parser
from kafka_file_transfer.config import load_config
from kafka_file_transfer.logging_setup import setup_logging
from kafka_file_transfer.version import __version__


def test_parser_config_only():
    parser = build_parser()
    send_args = parser.parse_args(["-c", "my_settings.py", "send"])
    assert send_args.command == "send"
    assert send_args.config == "my_settings.py"

    recv_args = parser.parse_args(["receive"])
    assert recv_args.command == "receive"
    assert recv_args.config == "settings.py"


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
    log_file = tmp_path / "app.log"
    cfg_path = tmp_path / "settings.py"
    cfg_path.write_text(
        f'''
VERSION = "{__version__}"
KAFKA = {{"brokers": "localhost:9092", "topic": "t"}}
TRANSFER = {{"chunk_size": 1024}}
SEND = {{"file": "./a.zip"}}
RECEIVE = {{"output_dir": "./out"}}
LOGGING = {{
    "level": "INFO",
    "console": False,
    "file": r"{log_file.as_posix()}",
}}
''',
        encoding="utf-8",
    )
    cfg = load_config(cfg_path)
    logger = setup_logging(cfg.logging)
    logger.info("hello-log")
    assert log_file.exists()
    assert "hello-log" in log_file.read_text(encoding="utf-8")

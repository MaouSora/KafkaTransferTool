from pathlib import Path

from kafka_file_transfer.cli import _load_config, build_parser


def test_load_config(tmp_path: Path):
    cfg = tmp_path / "c.yaml"
    cfg.write_text(
        "brokers: localhost:9092\ntopic: file-transfer\nchunk_size: 1024\n",
        encoding="utf-8",
    )
    data = _load_config(str(cfg))
    assert data["brokers"] == "localhost:9092"
    assert data["topic"] == "file-transfer"
    assert data["chunk_size"] == 1024


def test_parser_send_and_receive():
    parser = build_parser()
    send_args = parser.parse_args(
        ["send", "-b", "localhost:9092", "-t", "t1", "-f", "a.zip", "--chunk-size", "2048"]
    )
    assert send_args.command == "send"
    assert send_args.file == "a.zip"
    assert send_args.chunk_size == 2048

    recv_args = parser.parse_args(
        ["receive", "-b", "localhost:9092", "-t", "t1", "-o", "./out", "--idle-timeout", "0"]
    )
    assert recv_args.command == "receive"
    assert recv_args.output_dir == "./out"
    assert recv_args.idle_timeout == 0.0

from pathlib import Path

from kafka_file_transfer.protocol import (
    MSG_META,
    FileMeta,
    build_headers,
    guess_content_type,
    parse_headers,
    sha256_bytes,
)


def test_file_meta_roundtrip():
    meta = FileMeta(
        file_id="abc123",
        filename="demo.h5",
        size=1024,
        sha256="d" * 64,
        total_chunks=2,
        chunk_size=512,
        content_type="application/x-hdf5",
    )
    restored = FileMeta.from_bytes(meta.to_bytes())
    assert restored == meta


def test_guess_content_type_common_files(tmp_path: Path):
    assert guess_content_type(tmp_path / "a.zip") == "application/zip"
    assert guess_content_type(tmp_path / "b.h5") == "application/x-hdf5"
    assert guess_content_type(tmp_path / "c.hdf5") == "application/x-hdf5"
    assert guess_content_type(tmp_path / "d.bin") == "application/octet-stream"


def test_headers_roundtrip():
    headers = build_headers(MSG_META, "fid", chunk_index=3, total_chunks=10)
    parsed = parse_headers(headers)
    assert parsed["kft-type"] == MSG_META
    assert parsed["kft-file-id"] == "fid"
    assert parsed["kft-chunk-index"] == "3"
    assert parsed["kft-total-chunks"] == "10"


def test_sha256_bytes():
    assert sha256_bytes(b"hello") == (
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    )

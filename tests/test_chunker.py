import zipfile
from pathlib import Path

import pytest

from kafka_file_transfer.chunker import (
    FileAssembler,
    build_file_meta,
    calc_total_chunks,
    iter_file_chunks,
)
from kafka_file_transfer.protocol import sha256_file


def test_calc_total_chunks():
    assert calc_total_chunks(0, 512) == 1
    assert calc_total_chunks(512, 512) == 1
    assert calc_total_chunks(513, 512) == 2
    with pytest.raises(ValueError):
        calc_total_chunks(10, 0)


def test_iter_and_assemble_binary(tmp_path: Path):
    src = tmp_path / "payload.bin"
    data = bytes(range(256)) * 40  # 10 KiB
    src.write_bytes(data)

    meta = build_file_meta(src, chunk_size=1024)
    assert meta.filename == "payload.bin"
    assert meta.size == len(data)
    assert meta.total_chunks == 10
    assert meta.sha256 == sha256_file(src)

    out_dir = tmp_path / "out"
    assembler = FileAssembler(meta, out_dir)
    for index, chunk in iter_file_chunks(src, chunk_size=1024):
        assembler.add_chunk(index, chunk)
    final = assembler.finalize()

    assert final.read_bytes() == data
    assert final.name == "payload.bin"


def test_assemble_out_of_order(tmp_path: Path):
    src = tmp_path / "demo.zip"
    with zipfile.ZipFile(src, "w") as zf:
        zf.writestr("hello.txt", "kafka-file-transfer")

    meta = build_file_meta(src, chunk_size=32)
    chunks = list(iter_file_chunks(src, chunk_size=32))
    assert len(chunks) == meta.total_chunks

    assembler = FileAssembler(meta, tmp_path / "recv")
    for index, chunk in reversed(chunks):
        assembler.add_chunk(index, chunk)
    final = assembler.finalize()
    assert sha256_file(final) == meta.sha256


def test_empty_file(tmp_path: Path):
    src = tmp_path / "empty.h5"
    src.write_bytes(b"")
    meta = build_file_meta(src, chunk_size=1024)
    assert meta.total_chunks == 1
    assert meta.content_type == "application/x-hdf5"

    assembler = FileAssembler(meta, tmp_path / "out")
    for index, chunk in iter_file_chunks(src, chunk_size=1024):
        assembler.add_chunk(index, chunk)
    final = assembler.finalize()
    assert final.read_bytes() == b""


def test_duplicate_filename(tmp_path: Path):
    src = tmp_path / "a.zip"
    src.write_bytes(b"abc")
    meta = build_file_meta(src, chunk_size=1024)
    out = tmp_path / "out"
    out.mkdir()
    (out / "a.zip").write_bytes(b"old")

    assembler = FileAssembler(meta, out)
    for index, chunk in iter_file_chunks(src, chunk_size=1024):
        assembler.add_chunk(index, chunk)
    final = assembler.finalize()
    assert final.name == "a_1.zip"
    assert final.read_bytes() == b"abc"

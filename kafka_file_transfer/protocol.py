"""文件传输消息协议。

同一 file_id 作为 Kafka message key，保证落到同一分区并按序消费。

消息类型：
- meta: 文件元信息（文件名、大小、校验和、分片数等）
- chunk: 文件分片二进制内容
- complete: 发送完成标记
"""

from __future__ import annotations

import hashlib
import json
import mimetypes
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

MSG_META = "meta"
MSG_CHUNK = "chunk"
MSG_COMPLETE = "complete"

HEADER_TYPE = "kft-type"
HEADER_FILE_ID = "kft-file-id"
HEADER_CHUNK_INDEX = "kft-chunk-index"
HEADER_TOTAL_CHUNKS = "kft-total-chunks"


@dataclass(frozen=True)
class FileMeta:
    file_id: str
    filename: str
    size: int
    sha256: str
    total_chunks: int
    chunk_size: int
    content_type: str

    def to_bytes(self) -> bytes:
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "FileMeta":
        payload: dict[str, Any] = json.loads(data.decode("utf-8"))
        return cls(
            file_id=str(payload["file_id"]),
            filename=str(payload["filename"]),
            size=int(payload["size"]),
            sha256=str(payload["sha256"]),
            total_chunks=int(payload["total_chunks"]),
            chunk_size=int(payload["chunk_size"]),
            content_type=str(payload.get("content_type") or "application/octet-stream"),
        )


def new_file_id() -> str:
    return uuid.uuid4().hex


def guess_content_type(path: Path) -> str:
    content_type, _ = mimetypes.guess_type(str(path))
    if content_type:
        return content_type
    suffix = path.suffix.lower()
    extras = {
        ".h5": "application/x-hdf5",
        ".hdf5": "application/x-hdf5",
        ".hdf": "application/x-hdf",
        ".zip": "application/zip",
        ".7z": "application/x-7z-compressed",
        ".rar": "application/vnd.rar",
        ".parquet": "application/vnd.apache.parquet",
        ".npz": "application/octet-stream",
        ".pt": "application/octet-stream",
        ".pkl": "application/octet-stream",
        ".pickle": "application/octet-stream",
    }
    return extras.get(suffix, "application/octet-stream")


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            block = fh.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_headers(
    msg_type: str,
    file_id: str,
    *,
    chunk_index: int | None = None,
    total_chunks: int | None = None,
) -> list[tuple[str, bytes]]:
    headers: list[tuple[str, bytes]] = [
        (HEADER_TYPE, msg_type.encode("utf-8")),
        (HEADER_FILE_ID, file_id.encode("utf-8")),
    ]
    if chunk_index is not None:
        headers.append((HEADER_CHUNK_INDEX, str(chunk_index).encode("utf-8")))
    if total_chunks is not None:
        headers.append((HEADER_TOTAL_CHUNKS, str(total_chunks).encode("utf-8")))
    return headers


def parse_headers(raw_headers: list[tuple[str, bytes]] | None) -> dict[str, str]:
    if not raw_headers:
        return {}
    parsed: dict[str, str] = {}
    for key, value in raw_headers:
        if value is None:
            continue
        parsed[key] = value.decode("utf-8")
    return parsed

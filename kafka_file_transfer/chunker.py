"""文件分片与重组。"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Iterator

from .protocol import FileMeta, guess_content_type, new_file_id, sha256_file

DEFAULT_CHUNK_SIZE = 512 * 1024  # 512 KiB，适配 Kafka 默认消息上限


def calc_total_chunks(size: int, chunk_size: int) -> int:
    if size < 0:
        raise ValueError("文件大小不能为负数")
    if chunk_size <= 0:
        raise ValueError("分片大小必须大于 0")
    if size == 0:
        return 1
    return math.ceil(size / chunk_size)


def build_file_meta(path: Path, chunk_size: int = DEFAULT_CHUNK_SIZE) -> FileMeta:
    if not path.is_file():
        raise FileNotFoundError(f"文件不存在: {path}")
    size = path.stat().st_size
    return FileMeta(
        file_id=new_file_id(),
        filename=path.name,
        size=size,
        sha256=sha256_file(path),
        total_chunks=calc_total_chunks(size, chunk_size),
        chunk_size=chunk_size,
        content_type=guess_content_type(path),
    )


def iter_file_chunks(path: Path, chunk_size: int = DEFAULT_CHUNK_SIZE) -> Iterator[tuple[int, bytes]]:
    if chunk_size <= 0:
        raise ValueError("分片大小必须大于 0")
    size = path.stat().st_size
    if size == 0:
        yield 0, b""
        return
    with path.open("rb") as fh:
        index = 0
        while True:
            data = fh.read(chunk_size)
            if not data:
                break
            yield index, data
            index += 1


class FileAssembler:
    """按分片序号重组文件。"""

    def __init__(self, meta: FileMeta, output_dir: Path) -> None:
        self.meta = meta
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._part_path = self.output_dir / f".{meta.file_id}.part"
        self._received: set[int] = set()
        self._bytes_written = 0
        # 预分配或按序追加：采用按序写入，要求 chunk 按序到达（同 key 同分区可保证）
        self._next_index = 0
        self._buffer: dict[int, bytes] = {}
        self._fh = self._part_path.open("wb")

    @property
    def is_complete(self) -> bool:
        return len(self._received) >= self.meta.total_chunks

    def add_chunk(self, index: int, data: bytes) -> None:
        if index < 0 or index >= self.meta.total_chunks:
            raise ValueError(f"非法分片序号: {index}")
        if index in self._received:
            return
        self._buffer[index] = data
        self._received.add(index)
        self._flush_ready()

    def _flush_ready(self) -> None:
        while self._next_index in self._buffer:
            data = self._buffer.pop(self._next_index)
            self._fh.write(data)
            self._bytes_written += len(data)
            self._next_index += 1

    def finalize(self) -> Path:
        self._flush_ready()
        self._fh.flush()
        self._fh.close()

        if not self.is_complete:
            missing = sorted(set(range(self.meta.total_chunks)) - self._received)
            self._cleanup_part()
            raise ValueError(f"文件分片不完整，缺失: {missing[:20]}")

        if self._bytes_written != self.meta.size:
            self._cleanup_part()
            raise ValueError(
                f"文件大小不匹配: 期望 {self.meta.size}, 实际 {self._bytes_written}"
            )

        from .protocol import sha256_file

        digest = sha256_file(self._part_path)
        if digest != self.meta.sha256:
            self._cleanup_part()
            raise ValueError(
                f"SHA256 校验失败: 期望 {self.meta.sha256}, 实际 {digest}"
            )

        final_path = self._unique_path(self.output_dir / self.meta.filename)
        self._part_path.replace(final_path)
        return final_path

    def abort(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass
        self._cleanup_part()

    def _cleanup_part(self) -> None:
        if self._part_path.exists():
            self._part_path.unlink()

    @staticmethod
    def _unique_path(path: Path) -> Path:
        if not path.exists():
            return path
        stem = path.stem
        suffix = path.suffix
        parent = path.parent
        counter = 1
        while True:
            candidate = parent / f"{stem}_{counter}{suffix}"
            if not candidate.exists():
                return candidate
            counter += 1

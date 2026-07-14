"""Kafka 文件发送端。"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable

from kafka import KafkaProducer

from .chunker import DEFAULT_CHUNK_SIZE, build_file_meta, iter_file_chunks
from .protocol import MSG_CHUNK, MSG_COMPLETE, MSG_META, FileMeta, build_headers

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int, int], None]


class FileSender:
    def __init__(
        self,
        brokers: str,
        topic: str,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        acks: str | int = "all",
        retries: int = 3,
        max_request_size: int | None = None,
        extra_producer_config: dict | None = None,
    ) -> None:
        self.brokers = brokers
        self.topic = topic
        self.chunk_size = chunk_size
        request_size = max_request_size or max(chunk_size * 2, 1024 * 1024)
        config = {
            "bootstrap_servers": brokers.split(","),
            "acks": acks,
            "retries": retries,
            "max_request_size": request_size,
            "linger_ms": 5,
            "value_serializer": None,
            "key_serializer": lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        }
        if extra_producer_config:
            config.update(extra_producer_config)
        self._producer = KafkaProducer(**config)

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()

    def __enter__(self) -> "FileSender":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def send_file(
        self,
        file_path: str | Path,
        *,
        progress: ProgressCallback | None = None,
    ) -> FileMeta:
        path = Path(file_path).expanduser().resolve()
        meta = build_file_meta(path, self.chunk_size)
        key = meta.file_id

        logger.info(
            "开始发送: %s (%s bytes, %s chunks, sha256=%s)",
            meta.filename,
            meta.size,
            meta.total_chunks,
            meta.sha256,
        )

        future = self._producer.send(
            self.topic,
            key=key,
            value=meta.to_bytes(),
            headers=build_headers(MSG_META, meta.file_id, total_chunks=meta.total_chunks),
        )
        future.get(timeout=60)

        for index, data in iter_file_chunks(path, self.chunk_size):
            future = self._producer.send(
                self.topic,
                key=key,
                value=data,
                headers=build_headers(
                    MSG_CHUNK,
                    meta.file_id,
                    chunk_index=index,
                    total_chunks=meta.total_chunks,
                ),
            )
            future.get(timeout=60)
            if progress:
                progress(index + 1, meta.total_chunks, len(data))
            logger.debug("已发送分片 %s/%s (%s bytes)", index + 1, meta.total_chunks, len(data))

        future = self._producer.send(
            self.topic,
            key=key,
            value=b"",
            headers=build_headers(
                MSG_COMPLETE,
                meta.file_id,
                total_chunks=meta.total_chunks,
            ),
        )
        future.get(timeout=60)
        self._producer.flush()

        logger.info("发送完成: %s -> topic=%s file_id=%s", meta.filename, self.topic, meta.file_id)
        return meta

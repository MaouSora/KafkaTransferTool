"""Kafka 文件接收端。"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Callable

from kafka import KafkaConsumer

from .chunker import FileAssembler
from .protocol import (
    HEADER_CHUNK_INDEX,
    HEADER_FILE_ID,
    HEADER_TYPE,
    MSG_CHUNK,
    MSG_COMPLETE,
    MSG_META,
    FileMeta,
    parse_headers,
)

logger = logging.getLogger(__name__)

FileReceivedCallback = Callable[[Path, FileMeta], None]


class FileReceiver:
    def __init__(
        self,
        brokers: str,
        topic: str,
        output_dir: str | Path,
        *,
        group_id: str = "kafka-file-transfer-receiver",
        auto_offset_reset: str = "earliest",
        idle_timeout: float = 30.0,
        poll_timeout_ms: int = 1000,
        extra_consumer_config: dict | None = None,
    ) -> None:
        self.brokers = brokers
        self.topic = topic
        self.output_dir = Path(output_dir).expanduser().resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.group_id = group_id
        self.idle_timeout = idle_timeout
        self.poll_timeout_ms = poll_timeout_ms

        config = {
            "bootstrap_servers": [b.strip() for b in brokers.split(",") if b.strip()],
            "group_id": group_id,
            "auto_offset_reset": auto_offset_reset,
            "enable_auto_commit": False,
            "max_partition_fetch_bytes": 2 * 1024 * 1024,
            "fetch_max_bytes": 10 * 1024 * 1024,
            "key_deserializer": lambda k: k.decode("utf-8") if k else None,
            "value_deserializer": None,
        }
        if extra_consumer_config:
            config.update(extra_consumer_config)
        logger.info(
            "初始化 Consumer brokers=%s topic=%s group_id=%s auto_offset_reset=%s output_dir=%s",
            self.brokers,
            self.topic,
            self.group_id,
            auto_offset_reset,
            self.output_dir,
        )
        self._consumer = KafkaConsumer(topic, **config)
        self._assemblers: dict[str, FileAssembler] = {}
        self._metas: dict[str, FileMeta] = {}
        self._completed: set[str] = set()

    def close(self) -> None:
        logger.info("关闭 Consumer，清理未完成组装任务 count=%s", len(self._assemblers))
        for assembler in self._assemblers.values():
            assembler.abort()
        self._assemblers.clear()
        self._consumer.close()

    def __enter__(self) -> "FileReceiver":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def receive_forever(
        self,
        *,
        on_file: FileReceivedCallback | None = None,
        max_files: int | None = None,
    ) -> list[Path]:
        """持续接收文件。

        idle_timeout > 0 时，若连续空闲超过该秒数则退出。
        idle_timeout <= 0 时持续监听，直到收到 KeyboardInterrupt。
        """
        saved: list[Path] = []
        last_message_at = time.monotonic()
        files_done = 0

        logger.info(
            "开始监听 topic=%s group=%s output=%s",
            self.topic,
            self.group_id,
            self.output_dir,
        )

        try:
            while True:
                records = self._consumer.poll(timeout_ms=self.poll_timeout_ms)
                if not records:
                    if self.idle_timeout > 0 and (time.monotonic() - last_message_at) >= self.idle_timeout:
                        logger.info("空闲超时 %.1fs，结束接收", self.idle_timeout)
                        break
                    continue

                last_message_at = time.monotonic()
                for _tp, messages in records.items():
                    for message in messages:
                        path = self._handle_message(message)
                        self._consumer.commit()
                        if path is not None:
                            saved.append(path)
                            files_done += 1
                            if on_file:
                                on_file(path, self._metas[message.key])
                            if max_files is not None and files_done >= max_files:
                                logger.info("已达 max_files=%s，结束接收", max_files)
                                return saved
        except KeyboardInterrupt:
            logger.info("收到中断信号，停止接收")
        return saved

    def _handle_message(self, message) -> Path | None:
        headers = parse_headers(message.headers)
        msg_type = headers.get(HEADER_TYPE)
        file_id = headers.get(HEADER_FILE_ID) or message.key
        if not file_id or not msg_type:
            logger.warning("忽略无效消息 offset=%s", message.offset)
            return None

        if msg_type == MSG_META:
            meta = FileMeta.from_bytes(message.value or b"{}")
            self._metas[file_id] = meta
            if file_id in self._assemblers:
                self._assemblers[file_id].abort()
            self._assemblers[file_id] = FileAssembler(meta, self.output_dir)
            logger.info(
                "收到元信息: %s size=%s chunks=%s content_type=%s",
                meta.filename,
                meta.size,
                meta.total_chunks,
                meta.content_type,
            )
            return None

        if msg_type == MSG_CHUNK:
            assembler = self._assemblers.get(file_id)
            if assembler is None:
                logger.warning("收到未知 file_id 的分片，已忽略: %s", file_id)
                return None
            index = int(headers.get(HEADER_CHUNK_INDEX, "-1"))
            payload = message.value or b""
            assembler.add_chunk(index, payload)
            total = assembler.meta.total_chunks
            done = index + 1
            if done == total or done == 1 or done % max(1, total // 10) == 0:
                logger.info(
                    "接收进度 %s/%s (%.1f%%) file=%s bytes=%s",
                    done,
                    total,
                    (done / total) * 100 if total else 100.0,
                    assembler.meta.filename,
                    len(payload),
                )
            else:
                logger.debug(
                    "收到分片 %s/%s file_id=%s bytes=%s",
                    done,
                    total,
                    file_id,
                    len(payload),
                )
            return None

        if msg_type == MSG_COMPLETE:
            if file_id in self._completed:
                return None
            assembler = self._assemblers.pop(file_id, None)
            meta = self._metas.get(file_id)
            if assembler is None or meta is None:
                logger.warning("收到 complete 但缺少组装上下文: %s", file_id)
                return None
            try:
                path = assembler.finalize()
            except Exception:
                logger.exception("文件组装失败: %s", getattr(meta, "filename", file_id))
                raise
            self._completed.add(file_id)
            logger.info("文件接收完成: %s", path)
            return path

        logger.warning("未知消息类型: %s", msg_type)
        return None

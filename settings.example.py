"""KafkaTransferTool 配置示例（Python 模块）。

用法:
  cp settings.example.py settings.py
  # 编辑 settings.py
  python -m kafka_file_transfer -c settings.py send
  python -m kafka_file_transfer -c settings.py receive
  python -m kafka_file_transfer --version

也可写成 class 形式，见文件末尾注释。
"""

# 配置版本（建议与程序版本一致）
VERSION = "1.0.0"

KAFKA = {
    "brokers": "localhost:9092",
    "topic": "file-transfer",
    # Producer 确认级别： "all" / 1 / 0
    "acks": "all",
    "retries": 3,
    # 单次发送等待超时（秒）
    "send_timeout": 60,
    # Consumer poll 超时（毫秒）
    "poll_timeout_ms": 1000,
    # 可选：覆盖 Producer max.request.size
    # "max_request_size": 2097152,
    # 可选：透传给 kafka-python 的额外参数
    # "producer_extra": {},
    # "consumer_extra": {},
}

TRANSFER = {
    # 分片大小（字节），建议小于 Kafka message.max.bytes（默认约 1MB）
    "chunk_size": 524288,
}

# 发送模式所需
SEND = {
    "file": "./examples/sample-data/sample.zip",
}

# 接收模式所需
RECEIVE = {
    "output_dir": "./received",
    "group_id": "kafka-file-transfer-receiver",
    # 空闲超时（秒）：0 表示持续监听
    "idle_timeout": 30,
    # 最多接收文件数；None 表示不限制
    "max_files": None,
    # "earliest" | "latest"
    "auto_offset_reset": "earliest",
}

LOGGING = {
    "level": "INFO",  # DEBUG / INFO / WARNING / ERROR / CRITICAL
    "format": "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    "datefmt": "%Y-%m-%d %H:%M:%S",
    "console": True,
    # 日志文件路径；None 关闭文件日志
    "file": "./logs/kafka-file-transfer.log",
    "max_bytes": 10 * 1024 * 1024,
    "backup_count": 5,
    "quiet_loggers": ("kafka", "urllib3"),
}


# ---------------------------------------------------------------------------
# 也可以改用 class 写法（二选一即可，不要混用同名段）：
#
# class Kafka:
#     brokers = "localhost:9092"
#     topic = "file-transfer"
#
# class Transfer:
#     chunk_size = 524288
#
# class Send:
#     file = "./data/sample.zip"
#
# class Receive:
#     output_dir = "./received"
#     group_id = "kafka-file-transfer-receiver"
#     idle_timeout = 0
#     max_files = None
#     auto_offset_reset = "earliest"
#
# class Logging:
#     level = "INFO"
#     console = True
#     file = "./logs/kafka-file-transfer.log"
# ---------------------------------------------------------------------------

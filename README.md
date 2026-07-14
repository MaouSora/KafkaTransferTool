# KafkaTransferTool

通过 Kafka **指定 Topic** 进行文件传输的命令行工具：一端发送、一端接收，支持 `zip`、`h5`/`hdf5`、以及任意常见二进制/文本文件。

**当前版本：`1.0.0`**

## 特性

- **Python 配置模块**：全部运行参数写在 `settings.py`（不用 YAML）
- 单 Topic 收发：发送端与接收端共用同一个 Topic
- 自动分片：大文件按块传输，适配 Kafka 消息大小限制（默认 512KiB/片）
- 完整性校验：SHA256 校验，接收完成后校验大小与哈希
- 完善日志：控制台 + 轮转文件日志，可配置级别与格式
- 版本号：`--version` 查看；启动日志输出程序版本与配置版本

## 安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# 或
pip install -e .
```

查看版本：

```bash
python -m kafka_file_transfer --version
```

## 配置

复制示例配置并按需修改：

```bash
cp settings.example.py settings.py
```

支持两种写法（见 `settings.example.py`）：

**dict 风格：**

```python
VERSION = "1.0.0"
KAFKA = {"brokers": "localhost:9092", "topic": "file-transfer"}
TRANSFER = {"chunk_size": 524288}
SEND = {"file": "./data/sample.zip"}
RECEIVE = {"output_dir": "./received", "idle_timeout": 0}
LOGGING = {"level": "INFO", "console": True, "file": "./logs/app.log"}
```

**class 风格：**

```python
class Kafka:
    brokers = "localhost:9092"
    topic = "file-transfer"

class Send:
    file = "./data/sample.zip"
```

主要配置段：

| 段 | 说明 |
|----|------|
| `VERSION` | 配置版本（建议与程序版本一致） |
| `KAFKA` / `Kafka` | brokers、topic、超时、acks 等 |
| `TRANSFER` / `Transfer` | `chunk_size` 分片大小 |
| `SEND` / `Send` | 发送文件路径 `file` |
| `RECEIVE` / `Receive` | 输出目录、消费组、空闲超时等 |
| `LOGGING` / `Logging` | 日志级别、格式、控制台/文件输出 |

## 快速开始

### 1. 准备 Kafka

```bash
kafka-topics.sh --create --topic file-transfer \
  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

或使用仓库内 Docker Compose：

```bash
cd examples && docker compose up -d
```

### 2. 编辑 `settings.py`

设置 `KAFKA`，以及发送用的 `SEND` / 接收用的 `RECEIVE`。

### 3. 接收 / 发送

```bash
python -m kafka_file_transfer -c settings.py receive
python -m kafka_file_transfer -c settings.py send
```

默认即读取当前目录 `settings.py`：

```bash
python -m kafka_file_transfer receive
python -m kafka_file_transfer send
```

命令行只接受：

- `send` / `receive`
- `-c/--config`（默认 `settings.py`）
- `--version`

### 4. 日志

默认同时输出到控制台与 `./logs/kafka-file-transfer.log`（轮转）。启动时会打印类似：

```text
kafka-file-transfer v1.0.0 启动 | command=send | config=.../settings.py | config.version=1.0.0
```

## 协议概览

| 消息类型 | 说明 |
|---------|------|
| `meta` | 文件名、大小、SHA256、分片数、Content-Type |
| `chunk` | 分片二进制内容 |
| `complete` | 发送完成标记，触发落盘与校验 |

## 本地演示

```bash
bash examples/demo.sh
```

## 测试

```bash
pip install -r requirements.txt
pytest -q
```

## 项目结构

```text
kafka_file_transfer/
  version.py        # 版本号（单一来源）
  config.py         # 加载/校验 Python 配置模块
  logging_setup.py
  cli.py
  protocol.py
  chunker.py
  sender.py
  receiver.py
settings.example.py
examples/docker-compose.yml
tests/
```

## 注意事项

1. **消息大小**：默认分片 512KiB。过大时减小 `TRANSFER["chunk_size"]` 或调大 Kafka 限制。
2. **消费组**：同一 `group_id` 下消息只会被一个消费者处理。
3. **重名文件**：目标目录已存在同名文件时，自动保存为 `name_1.ext`…
4. **版本**：程序版本在 `kafka_file_transfer/version.py`；配置中的 `VERSION` 仅用于兼容提示。

# KafkaTransferTool

通过 Kafka **指定 Topic** 进行文件传输的命令行工具：一端发送、一端接收，支持 `zip`、`h5`/`hdf5`、以及任意常见二进制/文本文件。

## 特性

- 单 Topic 收发：发送端与接收端共用同一个 Topic
- 自动分片：大文件按块传输，适配 Kafka 消息大小限制（默认 512KiB/片）
- 完整性校验：SHA256 校验，接收完成后校验大小与哈希
- 同文件有序：使用 `file_id` 作为消息 key，保证落到同一分区并按序消费
- 常见格式：`zip`、`h5`、`hdf5`、`7z`、`tar`、`gz`、`parquet`、图片与模型权重等均可透传

## 协议概览

| 消息类型 | 说明 |
|---------|------|
| `meta` | 文件名、大小、SHA256、分片数、Content-Type |
| `chunk` | 分片二进制内容 |
| `complete` | 发送完成标记，触发落盘与校验 |

消息头字段：`kft-type`、`kft-file-id`、`kft-chunk-index`、`kft-total-chunks`。

## 安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# 或
pip install -e .
```

## 快速开始

### 1. 准备 Kafka

确保已有可用的 Kafka 集群，并创建 Topic（示例）：

```bash
kafka-topics.sh --create --topic file-transfer --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

> 建议 Topic 至少保证同一 `file_id` 的消息有序（本工具用 key 路由到同一分区）。若单分区即可满足需求，可直接使用 1 个分区。

如 Broker 默认 `message.max.bytes` 较小，请同步调大 Broker / Producer / Consumer 的相关限制，或减小 `--chunk-size`。

### 2. 发送文件

```bash
python -m kafka_file_transfer send \
  --brokers localhost:9092 \
  --topic file-transfer \
  --file ./data/sample.zip
```

发送 HDF5：

```bash
python -m kafka_file_transfer send \
  -b localhost:9092 \
  -t file-transfer \
  -f ./data/model.h5
```

### 3. 接收文件

另开终端：

```bash
python -m kafka_file_transfer receive \
  --brokers localhost:9092 \
  --topic file-transfer \
  --output-dir ./received \
  --group-id file-receiver \
  --from-beginning
```

- `--idle-timeout 30`：连续 30 秒无新消息后退出（默认）
- `--idle-timeout 0`：持续监听
- `--max-files 1`：收满 N 个文件后退出

### 使用配置文件

复制并修改示例配置：

```bash
cp config.example.yaml config.yaml
```

```bash
python -m kafka_file_transfer send -c config.yaml -f ./data/sample.zip
python -m kafka_file_transfer receive -c config.yaml --from-beginning
```

安装后也可直接使用入口命令：

```bash
kafka-file-transfer send -b localhost:9092 -t file-transfer -f ./a.zip
kafka-file-transfer receive -b localhost:9092 -t file-transfer -o ./received
```

## 本地演示（Docker Compose）

仓库提供 `examples/docker-compose.yml`，可一键启动单节点 Kafka：

```bash
cd examples
docker compose up -d
```

然后按上面的 send / receive 命令操作（`brokers=localhost:9092`）。

## 测试

协议与分片重组不依赖真实 Kafka，可直接运行：

```bash
pip install -r requirements.txt
pytest -q
```

## 项目结构

```text
kafka_file_transfer/
  cli.py          # 命令行
  protocol.py     # 消息协议与校验
  chunker.py      # 分片 / 重组
  sender.py       # 发送端
  receiver.py     # 接收端
config.example.yaml
examples/docker-compose.yml
tests/
```

## 注意事项

1. **消息大小**：默认分片 512KiB。若发送失败并提示消息过大，减小 `--chunk-size` 或增大 Kafka `message.max.bytes` / `max.request.size` / `fetch.max.bytes`。
2. **消费组**：同一 `group_id` 下消息只会被一个消费者处理；多实例并行接收请使用不同 group，或依赖分区扩展。
3. **重名文件**：目标目录已存在同名文件时，会自动保存为 `name_1.ext`、`name_2.ext`…
4. **安全性**：本工具不做加密与鉴权封装；生产环境请配合 Kafka SASL/SSL 与网络隔离。

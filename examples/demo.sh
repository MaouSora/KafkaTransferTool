#!/usr/bin/env bash
# 本地快速演示：生成样例文件与可运行配置
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="$ROOT/examples/sample-data"
mkdir -p "$DATA"

python3 - "$DATA" <<'PY'
from pathlib import Path
import sys
import zipfile

data = Path(sys.argv[1])
data.mkdir(parents=True, exist_ok=True)

zip_path = data / "sample.zip"
with zipfile.ZipFile(zip_path, "w") as zf:
    zf.writestr("readme.txt", "hello from KafkaTransferTool\n")

h5_path = data / "sample.h5"
h5_path.write_bytes(b"\x89HDF\r\n\x1a\n" + b"demo-payload-" + bytes(range(64)))
print(f"created: {zip_path}")
print(f"created: {h5_path}")
PY

SEND_CFG="$ROOT/examples/config.send.yaml"
RECV_CFG="$ROOT/examples/config.receive.yaml"

cat > "$SEND_CFG" <<EOF
version: "1.0.0"
kafka:
  brokers: "localhost:9092"
  topic: "file-transfer"
transfer:
  chunk_size: 524288
send:
  file: "$DATA/sample.zip"
logging:
  level: INFO
  console: true
  file: "$ROOT/logs/send.log"
EOF

cat > "$RECV_CFG" <<EOF
version: "1.0.0"
kafka:
  brokers: "localhost:9092"
  topic: "file-transfer"
receive:
  output_dir: "$ROOT/received"
  group_id: "kafka-file-transfer-demo"
  idle_timeout: 0
  auto_offset_reset: earliest
logging:
  level: INFO
  console: true
  file: "$ROOT/logs/receive.log"
EOF

cat <<EOF

样例文件与配置已生成：
  $DATA/sample.zip
  $DATA/sample.h5
  $SEND_CFG
  $RECV_CFG

查看版本：
  python -m kafka_file_transfer --version

终端 A（接收）：
  python -m kafka_file_transfer -c $RECV_CFG receive

终端 B（发送）：
  python -m kafka_file_transfer -c $SEND_CFG send
EOF

#!/usr/bin/env bash
# 本地快速演示：生成样例文件并提示收发命令
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
# 用伪 HDF5 魔数 + 载荷演示二进制透传（真实 .h5 同样适用）
h5_path.write_bytes(b"\x89HDF\r\n\x1a\n" + b"demo-payload-" + bytes(range(64)))
print(f"created: {zip_path}")
print(f"created: {h5_path}")
PY

cat <<EOF

样例文件已生成：
  $DATA/sample.zip
  $DATA/sample.h5

终端 A（接收）：
  python -m kafka_file_transfer receive -b localhost:9092 -t file-transfer -o $ROOT/received --from-beginning --idle-timeout 0

终端 B（发送）：
  python -m kafka_file_transfer send -b localhost:9092 -t file-transfer -f $DATA/sample.zip
  python -m kafka_file_transfer send -b localhost:9092 -t file-transfer -f $DATA/sample.h5
EOF

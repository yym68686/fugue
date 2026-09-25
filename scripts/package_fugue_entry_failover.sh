#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
output="${1:?output directory required}"
mkdir -p "$output"
output="$(cd "$output" && pwd)"
task_dir="$(mktemp -d)"
trap 'rm -rf "$task_dir"' EXIT
cd "$root"
version="$(git describe --tags --always --dirty)"
for arch in amd64 arm64; do
  mkdir -p "$task_dir/$arch"
  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -ldflags="-s -w -X main.version=$version" -o "$task_dir/$arch/fugue-entry-failover" ./cmd/fugue-entry-failover
  tar -czf "$output/fugue_entry_failover_linux_$arch.tar.gz" -C "$task_dir/$arch" fugue-entry-failover
done
python3 - "$output" <<'PY'
import hashlib, pathlib, sys
root = pathlib.Path(sys.argv[1])
(root / 'fugue_entry_failover_checksums.txt').write_text(''.join(
    f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n'
    for p in sorted(root.glob('fugue_entry_failover_linux_*.tar.gz'))
))
PY

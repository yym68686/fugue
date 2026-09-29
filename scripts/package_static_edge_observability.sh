#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
output="${1:?output directory required}"
mkdir -p "$output"
output="$(cd "$output" && pwd)"
cd "$root"
revision="$(git rev-parse HEAD)"
export GOTOOLCHAIN=go1.26.3
python3 scripts/prepare_static_caddy.py
stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
for arch in amd64 arm64; do
  mkdir -p "$stage/$arch"
  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -ldflags='-s -w' -o "$stage/$arch/fugue-static-edge-observer" ./cmd/fugue-static-edge-observer
  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -ldflags='-s -w' -o "$stage/$arch/fugue-static-edge-manager" ./cmd/fugue-static-edge-manager
  (cd static-caddy && CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -tags nobadger,nomysql,nopgx -ldflags="-s -w -X github.com/caddyserver/caddy/v2.CustomVersion=v2.11.4-fugue-observe.${revision:0:12}" -o "$stage/$arch/caddy" .)
  python3 - "$stage/$arch" "$revision" <<'PY'
import hashlib,json,pathlib,sys
p=pathlib.Path(sys.argv[1])
facts={"schema":"fugue.static-edge.package/v1","source_commit":sys.argv[2],"go":"go1.26.3","caddy":"v2.11.4","x_net":"v0.55.0","capabilities":["body-read","httptrace","http2-receive-consume","http2-send-credit-wait","tcp-info-connection-scope","http3-body-only","direct-query"],"files":{f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(p.iterdir()) if f.is_file()},"dependencies":json.loads(pathlib.Path("static-caddy/.build/provenance.json").read_text()),"patches":{str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(pathlib.Path("static-caddy/patches").rglob("*.patch"))}}
(p/"provenance.json").write_text(json.dumps(facts,indent=2)+"\n")
PY
  tar -czf "$output/fugue_static_edge_observability_linux_$arch.tar.gz" -C "$stage/$arch" caddy fugue-static-edge-observer fugue-static-edge-manager provenance.json
done
python3 - "$output" <<'PY'
import hashlib,pathlib,sys
p=pathlib.Path(sys.argv[1]);(p/"fugue_static_edge_observability_checksums.txt").write_text("".join(f"{hashlib.sha256(f.read_bytes()).hexdigest()}  {f.name}\n" for f in sorted(p.glob("fugue_static_edge_observability_linux_*.tar.gz"))))
PY

#!/usr/bin/env bash
# Isolated local fixture only; no production endpoints or workload credentials.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
if [[ -z "${FUGUE_TEST_CADDY:-}" ]]; then
  case "$(uname -s)/$(uname -m)" in
    Linux/x86_64) target=linux_amd64 ;;
    Linux/aarch64) target=linux_arm64 ;;
    Darwin/arm64) target=mac_arm64 ;;
    Darwin/x86_64) target=mac_amd64 ;;
    *) echo 'set FUGUE_TEST_CADDY for this platform' >&2; exit 1 ;;
  esac
  task_dir="$(mktemp -d)"
  trap 'rm -rf "$task_dir"' EXIT
  gh release download v2.10.2 --repo caddyserver/caddy --pattern "caddy_2.10.2_$target.tar.gz" --pattern caddy_2.10.2_checksums.txt --dir "$task_dir"
  python3 - "$task_dir" "$target" <<'PY'
import hashlib, pathlib, sys, tarfile
root=pathlib.Path(sys.argv[1]);name=f'caddy_2.10.2_{sys.argv[2]}.tar.gz'
expected=next(line.split()[0] for line in (root/'caddy_2.10.2_checksums.txt').read_text().splitlines() if line.split()[-1]==name)
if hashlib.sha512((root/name).read_bytes()).hexdigest()!=expected: raise SystemExit('Caddy release checksum mismatch')
with tarfile.open(root/name) as archive:
    (root/'caddy').write_bytes(archive.extractfile('caddy').read())
(root/'caddy').chmod(0o755)
PY
  export FUGUE_TEST_CADDY="$task_dir/caddy"
fi
go test -race ./internal/staticedgecontract ./internal/staticedgemanager ./internal/cli -run 'StaticEdge|BundleDigest|StrictJSON|Manager|VerifiedActivation|FailedApply|InterruptedWrite|DrainActually|CorruptState|ReadGrant|RealCaddy' -count=1

#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
export GOTOOLCHAIN=go1.26.3
python3 -m unittest scripts.test_static_generation_installer
python3 scripts/prepare_static_caddy.py
go test -race ./internal/staticedgeobserve ./internal/staticedgecontract ./internal/staticedgemanager ./pkg/staticedgeclient -timeout 120s
(cd static-caddy && go test -race -tags nobadger,nomysql,nopgx ./... -timeout 120s)
(cd static-caddy && go test -race -tags nobadger,nomysql,nopgx github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy -run 'Test(Active|HealthMetrics|ActualProbe|DownstreamBody)' -timeout 120s)
# Compile foreign platforms too: the remote Windows CLI must not inherit a
# Unix-only collector implementation through the shared query types.
GOOS=windows GOARCH=amd64 go build -o /tmp/fugue-observe-windows.exe ./cmd/fugue
rm /tmp/fugue-observe-windows.exe

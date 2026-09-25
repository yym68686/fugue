#!/usr/bin/env sh
# Render only. Never install a service, replace Caddy or change production state.
set -eu
if [ "$#" -ne 3 ]; then
  printf 'usage: %s <absolute-manager-binary> <absolute-policy-json> <service-user>\n' "$0" >&2
  exit 1
fi
binary=$1
policy=$2
user=$3
case "$binary:$policy:$user" in *[!a-zA-Z0-9_./:-]*|'') printf 'invalid unit argument\n' >&2; exit 1;; esac
case "$binary" in /*) ;; *) exit 1;; esac
case "$policy" in /*) ;; *) exit 1;; esac
case "$user" in ''|*/*|*:*|.*|-*) exit 1;; esac
cat <<UNIT
[Unit]
Description=Independent static edge management (no business process dependency)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$user
Group=$user
ExecStart=$binary --config $policy
Restart=on-failure
RestartSec=5s
TimeoutStopSec=120s
UMask=0077
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
RuntimeDirectory=fugue-static-edge-manager
RuntimeDirectoryMode=0700
StateDirectory=fugue-static-edge-manager
StateDirectoryMode=0700
ReadWritePaths=/etc/fugue-static-edge
MemoryMax=256M
TasksMax=64
LimitNOFILE=1024

[Install]
WantedBy=multi-user.target
UNIT

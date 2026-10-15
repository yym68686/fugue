#!/usr/bin/env bash
set -euo pipefail

revision="$1"
output="$2"
[[ "$revision" =~ ^[0-9a-f]{40}$ ]]
[[ "$output" == /* ]]
git merge-base --is-ancestor "$revision" HEAD

# Configuration recovery uses a declared verified reader, independently of the
# current candidate's buildability. No serving artifact or workload is changed.
source_dir="$(mktemp -d)"
trap 'rm -rf "$source_dir"' EXIT
git archive "$revision" | tar -x -C "$source_dir"
(
  cd "$source_dir"
  go build -trimpath -o "$output" ./cmd/fugue-dns-observation-verify
)

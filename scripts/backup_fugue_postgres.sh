#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: backup_fugue_postgres.sh [--dsn <postgres-dsn>] [--output <file>] [--output-dir <dir>] [--label <name>]

Creates a custom-format PostgreSQL backup with pg_dump.

Environment:
  FUGUE_DATABASE_URL   Default PostgreSQL DSN when --dsn is omitted.
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    printf '[fugue-pg-backup] ERROR: missing required command: %s\n' "$1" >&2
    exit 1
  }
}

sha256_file() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${path}" | awk '{print $1}'
    return
  fi
  shasum -a 256 "${path}" | awk '{print $1}'
}

DATABASE_URL="${FUGUE_DATABASE_URL:-}"
OUTPUT_FILE=""
OUTPUT_DIR=".dist/postgres-backups"
LABEL=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dsn)
      DATABASE_URL="${2:-}"
      shift 2
      ;;
    --output)
      OUTPUT_FILE="${2:-}"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="${2:-}"
      shift 2
      ;;
    --label)
      LABEL="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf '[fugue-pg-backup] ERROR: unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

[[ -n "${DATABASE_URL}" ]] || {
  printf '[fugue-pg-backup] ERROR: PostgreSQL DSN is required via --dsn or FUGUE_DATABASE_URL\n' >&2
  exit 1
}

require_cmd pg_dump
require_cmd date
require_cmd mkdir
require_cmd mv

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -z "${OUTPUT_FILE}" ]]; then
  mkdir -p "${OUTPUT_DIR}"
  base="fugue-postgres-${timestamp}"
  if [[ -n "${LABEL}" ]]; then
    base="fugue-postgres-${LABEL}-${timestamp}"
  fi
  OUTPUT_FILE="${OUTPUT_DIR}/${base}.dump"
else
  mkdir -p "$(dirname "${OUTPUT_FILE}")"
fi

tmp_file="${OUTPUT_FILE}.tmp"
rm -f "${tmp_file}"

printf '[fugue-pg-backup] writing custom-format backup to %s\n' "${OUTPUT_FILE}"
pg_dump \
  --dbname="${DATABASE_URL}" \
  --format=custom \
  --no-owner \
  --no-privileges \
  --file="${tmp_file}"

mv "${tmp_file}" "${OUTPUT_FILE}"
checksum="$(sha256_file "${OUTPUT_FILE}")"
size_bytes="$(wc -c < "${OUTPUT_FILE}" | tr -d '[:space:]')"

printf '[fugue-pg-backup] backup complete\n'
printf '[fugue-pg-backup] path=%s\n' "${OUTPUT_FILE}"
printf '[fugue-pg-backup] size_bytes=%s\n' "${size_bytes}"
printf '[fugue-pg-backup] sha256=%s\n' "${checksum}"

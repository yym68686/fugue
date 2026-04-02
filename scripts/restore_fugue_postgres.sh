#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: restore_fugue_postgres.sh --input <file> [--dsn <postgres-dsn>] [--jobs <n>] [--no-clean]

Restores a custom-format PostgreSQL backup created by backup_fugue_postgres.sh.

Environment:
  FUGUE_DATABASE_URL   Default PostgreSQL DSN when --dsn is omitted.
EOF
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    printf '[fugue-pg-restore] ERROR: missing required command: %s\n' "$1" >&2
    exit 1
  }
}

DATABASE_URL="${FUGUE_DATABASE_URL:-}"
INPUT_FILE=""
JOBS="1"
USE_CLEAN="true"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dsn)
      DATABASE_URL="${2:-}"
      shift 2
      ;;
    --input)
      INPUT_FILE="${2:-}"
      shift 2
      ;;
    --jobs)
      JOBS="${2:-}"
      shift 2
      ;;
    --no-clean)
      USE_CLEAN="false"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf '[fugue-pg-restore] ERROR: unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

[[ -n "${DATABASE_URL}" ]] || {
  printf '[fugue-pg-restore] ERROR: PostgreSQL DSN is required via --dsn or FUGUE_DATABASE_URL\n' >&2
  exit 1
}
[[ -n "${INPUT_FILE}" ]] || {
  printf '[fugue-pg-restore] ERROR: backup input file is required\n' >&2
  exit 1
}
[[ -r "${INPUT_FILE}" ]] || {
  printf '[fugue-pg-restore] ERROR: backup file is not readable: %s\n' "${INPUT_FILE}" >&2
  exit 1
}

require_cmd pg_restore

args=(
  --dbname="${DATABASE_URL}"
  --format=custom
  --no-owner
  --no-privileges
  --jobs="${JOBS}"
  --verbose
)
if [[ "${USE_CLEAN}" == "true" ]]; then
  args+=(--clean --if-exists)
fi

printf '[fugue-pg-restore] restoring backup from %s\n' "${INPUT_FILE}"
pg_restore "${args[@]}" "${INPUT_FILE}"
printf '[fugue-pg-restore] restore complete\n'

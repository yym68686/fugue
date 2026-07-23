#!/usr/bin/env bash

set -euo pipefail

: "${FUGUE_SCHEMA_TEST_ADMIN_URL:?set FUGUE_SCHEMA_TEST_ADMIN_URL to a disposable PostgreSQL admin database URL}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
database="fugue_schema_expand_${$}_${RANDOM}"

cleanup() {
  psql "${FUGUE_SCHEMA_TEST_ADMIN_URL}" -X --no-psqlrc --set ON_ERROR_STOP=1 \
    --set database_name="${database}" >/dev/null 2>&1 <<'SQL' || true
DROP DATABASE IF EXISTS :"database_name" WITH (FORCE);
SQL
}
trap cleanup EXIT

psql "${FUGUE_SCHEMA_TEST_ADMIN_URL}" -X --no-psqlrc --set ON_ERROR_STOP=1 \
  --set database_name="${database}" >/dev/null <<'SQL'
CREATE DATABASE :"database_name";
SQL

admin_prefix="${FUGUE_SCHEMA_TEST_ADMIN_URL%/*}"
database_url="${admin_prefix}/${database}?sslmode=disable"
psql "${database_url}" -X --no-psqlrc --set ON_ERROR_STOP=1 >/dev/null <<'SQL'
CREATE SCHEMA fugue_custom;
CREATE TABLE fugue_custom.fugue_platform_consumer_instances (
  consumer_id TEXT PRIMARY KEY
);
INSERT INTO fugue_custom.fugue_platform_consumer_instances (consumer_id) VALUES ('legacy-consumer');
SQL

database_dsn="  ${database_url}&options=-csearch_path%3Dfugue_custom  "
(
  cd "${REPO_ROOT}"
  FUGUE_SCHEMA_TEST_DATABASE_URL="${database_dsn}" \
    go test ./cmd/fugue-api -run '^TestPlatformStateSchemaExpandLive$' -count=1
)

columns="$(psql "${database_url}" -X --no-psqlrc --tuples-only --no-align --set ON_ERROR_STOP=1 <<'SQL'
SELECT string_agg(column_name || ':' || is_nullable || ':' || COALESCE(column_default, ''), ',' ORDER BY ordinal_position)
FROM information_schema.columns
WHERE table_schema = 'fugue_custom'
  AND table_name = 'fugue_platform_consumer_instances'
  AND column_name LIKE 'observation_%';
SQL
)"
[[ "${columns}" == *"observation_evidence_hash:NO:''::text"* ]]
[[ "${columns}" == *"observation_window_started_at:YES:"* ]]
[[ "${columns}" == *"observation_window_heartbeat_count:NO:0"* ]]

legacy_defaults="$(psql "${database_url}" -X --no-psqlrc --tuples-only --no-align --set ON_ERROR_STOP=1 <<'SQL'
SELECT observation_evidence_hash = ''
  AND observation_window_started_at IS NULL
  AND observation_window_heartbeat_count = 0
FROM fugue_custom.fugue_platform_consumer_instances
WHERE consumer_id = 'legacy-consumer';
SQL
)"
[[ "${legacy_defaults}" == "t" ]]

printf 'platform-state schema expand live PostgreSQL checks passed\n'

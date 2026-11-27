#!/usr/bin/env bash
# Run against a new loopback-only PostgreSQL cluster, never a supplied DSN.
set -euo pipefail
fixture=$(mktemp -d /tmp/fugue-rollup-fixture.XXXXXXXX)
port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
trap 'pg_ctl -D "$fixture/db" -m fast -w stop >/dev/null 2>&1 || true' EXIT
initdb -D "$fixture/db" -U postgres -A trust --auth-host=scram-sha-256 --no-locale >/dev/null
pg_ctl -D "$fixture/db" -l "$fixture/postgres.log" -o "-h 127.0.0.1 -k $fixture -p $port" -w start >/dev/null
psql -Xqwv ON_ERROR_STOP=1 -h "$fixture" -p "$port" -U postgres -d postgres \
  -c "ALTER ROLE postgres PASSWORD 'synthetic-local-provision-fixture';" \
  -c 'CREATE DATABASE observation_test;' -c 'CREATE DATABASE quality_evidence_test;' -c 'CREATE DATABASE sample_retention_test;'
export FUGUE_OBSERVATION_PROVISION_TEST_DSN="postgresql://postgres:synthetic-local-provision-fixture@127.0.0.1:$port/observation_test?sslmode=disable"
export FUGUE_QUALITY_EVIDENCE_REHEARSAL_DSN="postgresql://postgres:synthetic-local-provision-fixture@127.0.0.1:$port/quality_evidence_test?sslmode=disable"
export FUGUE_SAMPLE_RETENTION_REHEARSAL_DSN="postgresql://postgres:synthetic-local-provision-fixture@127.0.0.1:$port/sample_retention_test?sslmode=disable"
export PYTHONDONTWRITEBYTECODE=1
pg_ctl --version
python3 -m unittest -v scripts.test_provision_observation_worker_database scripts.test_provision_quality_evidence_retention scripts.test_provision_observation_sample_retention

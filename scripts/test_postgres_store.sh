#!/usr/bin/env bash
set -euo pipefail

: "${FUGUE_STORE_TEST_POSTGRES:?set to a disposable, empty, loopback PostgreSQL database}"
# Match the Go fixture's URL restrictions before executing any schema writes.
python3 - "$FUGUE_STORE_TEST_POSTGRES" <<'PY'
import sys
from urllib.parse import urlsplit

url = urlsplit(sys.argv[1])
if not (url.scheme in ('postgres', 'postgresql') and url.hostname in ('127.0.0.1', 'localhost')
        and url.path.startswith('/fugue_test') and url.query == 'sslmode=disable' and not url.fragment):
    sys.exit('test PostgreSQL URL must use loopback, a fugue_test database and sslmode=disable')
PY

if [[ "$(psql "$FUGUE_STORE_TEST_POSTGRES" -XAtqc "select count(*) from pg_class where relnamespace = 'public'::regnamespace and relkind in ('r','p')")" != 0 ]]; then
  echo 'refusing to initialize a populated PostgreSQL test template; create a fresh empty database' >&2
  exit 1
fi
FUGUE_DATABASE_URL="$FUGUE_STORE_TEST_POSTGRES" go run ./cmd/fugue-store-setup
psql "$FUGUE_STORE_TEST_POSTGRES" -Xq -v ON_ERROR_STOP=1 -f internal/schemamigrate/compiler_input_zero_temp.sql
export FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY=local-disposable-postgres-fixture-key
go test -count=1 -timeout 35m -p 2 -parallel 4 ./internal/{store,api,controller,cli,releaseflow,sshcontrol,qualityrollup,operationqueue} "$@"

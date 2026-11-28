"""Run the exact handoff SQL against a fresh private socket-only PostgreSQL."""

from pathlib import Path
import re
import subprocess as sp
from tempfile import TemporaryDirectory


def fixture_sql():
    base = Path("internal/store/postgres.go").read_text()
    schedule = Path("internal/store/app_mail_reconcile.go").read_text()
    statements = [
        re.search(r"postgresMetaSchemaStatement = `([^`]+)", base)[1],
        re.search(r"`(CREATE TABLE IF NOT EXISTS fugue_app_mail_bindings [^`]+)", base)[1],
    ]
    for name in ("appMailReconcileTableSQL", "appMailReconcileIndexSQL",
                 "appMailReconcileEnqueueSQL", "appMailReconcileTriggerSQL"):
        statements.append(re.search(name + r" = `([^`]+)", schedule)[1])
    return ";\n".join(statements) + ";\n"


def rehearse(socket):
    command = ["psql", "-XAtqw", "-v", "ON_ERROR_STOP=1", "-h", socket, "-p", "5432", "-U", "postgres", "-d", "postgres"]
    source = Path("internal/schemamigrate/app_mail_reconcile_activate.sql").read_text()

    def sql(body, refused=None):
        result = sp.run(command, input=body, text=True, capture_output=True, timeout=8)
        if refused is not None:
            if result.returncode == 0 or refused not in result.stderr:
                raise RuntimeError(f"expected refusal: {refused}; got {result.stderr}")
        else:
            result.check_returncode()
        return result.stdout.strip()

    sql(fixture_sql())
    sql("""INSERT INTO fugue_app_mail_bindings VALUES ('synthetic',1,'{"state":"active"}');""")
    sql(source, "work does not cover")
    sql("""INSERT INTO fugue_meta(key,value) VALUES('app-mail-reconcile/seed/v1','{"complete":true}');
    DELETE FROM fugue_app_mail_reconcile;""")
    sql(source, "work does not cover")
    sql("UPDATE fugue_app_mail_bindings SET generation=2; ALTER TABLE fugue_app_mail_bindings DISABLE TRIGGER fugue_app_mail_reconcile_enqueue;")
    sql(source, "trigger is not enabled")
    sql("ALTER TABLE fugue_app_mail_bindings ENABLE TRIGGER fugue_app_mail_reconcile_enqueue;")
    for func, key in (("pg_advisory_xact_lock", "app-mail-reconcile"),
                      ("pg_advisory_xact_lock_shared", "app-mail-reconcile/owner/v1")):
        with sp.Popen(command, stdin=sp.PIPE, stdout=sp.PIPE, text=True) as lock:
            try:
                lock.stdin.write(f"BEGIN; SELECT {func}(hashtextextended('{key}',0)); SELECT 'held';\n")
                lock.stdin.flush()
                if lock.stdout.readline().strip() or lock.stdout.readline().strip() != "held":
                    raise RuntimeError("old writer fixture did not acquire its lock")
                sql(source, "API writer has not drained")
            finally:
                lock.stdin.close()
                if lock.wait(timeout=5) != 0:
                    raise RuntimeError("old writer fixture failed")
    marker = "SELECT value || '|' || updated_at FROM fugue_meta WHERE key='app-mail-reconcile/owner/v1';"
    sql(source)
    first = sql(marker)
    sql(source)
    if not first.startswith("controller|") or sql(marker) != first:
        raise RuntimeError("handoff did not retain its original durable owner and timestamp")
    if sql("UPDATE fugue_app_mail_bindings SET generation=3; SELECT generation FROM fugue_app_mail_reconcile;") != "3":
        raise RuntimeError("front-door intent was not enqueued after handoff")
    print("verified app-mail handoff: old-writer locks, exact intent coverage, one-way owner, idempotent replay")


def main():
    with TemporaryDirectory(prefix="fugue-app-mail-fixture.") as socket:
        data = socket + "/db"
        sp.run(["initdb", "-D", data, "-U", "postgres", "-A", "trust", "--no-locale"],
               check=True, stdout=sp.DEVNULL, timeout=20)
        sp.run(["pg_ctl", "-D", data, "-l", socket + "/pg.log", "-o",
                f"-h '' -k {socket} -p 5432 -c statement_timeout=4000", "-w", "start"],
               check=True, stdout=sp.DEVNULL, timeout=15)
        try:
            rehearse(socket)
        finally:
            sp.run(["pg_ctl", "-D", data, "-m", "fast", "-w", "stop"],
                   check=True, stdout=sp.DEVNULL, timeout=15)


if __name__ == "__main__":
    main()

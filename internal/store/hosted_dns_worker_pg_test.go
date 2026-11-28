package store

import (
	"encoding/json"
	"net/netip"
	"net/url"
	"os"
	"testing"

	"fugue/internal/hosteddns"
	"fugue/internal/model"
	"fugue/internal/platformconfig"

	"github.com/jackc/pgx/v5"
)

func TestHostedDNSWorkerUsesBoundedDatabaseRole(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	api := newHostedDNSTestStore(t)
	script, err := os.ReadFile("../schemamigrate/hosted_dns_flatten_worker.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := api.db.ExecContext(ctx, string(script)); err != nil {
		t.Fatalf("install hosted DNS capability: %v", err)
	}
	dsn, err := url.Parse(api.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	roleName := "dns_refresh_" + dsn.Path[1:]
	role := pgx.Identifier{roleName}.Sanitize()
	if _, err := api.db.ExecContext(ctx, "CREATE ROLE "+role+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD 'disposable-testing-only'"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := api.db.Exec("DROP OWNED BY " + role + "; DROP ROLE " + role); err != nil {
			t.Error(err)
		}
	})
	if _, err := api.db.ExecContext(ctx, "GRANT USAGE ON SCHEMA fugue_hosted_dns_maintenance TO "+role+
		"; GRANT EXECUTE ON FUNCTION fugue_hosted_dns_maintenance.progress_v1(integer,text), "+
		"fugue_hosted_dns_maintenance.records_v1(text), "+
		"fugue_hosted_dns_maintenance.commit_v1(text,text,text,text,timestamptz,timestamptz,jsonb,jsonb,text,text,timestamptz,text,text) TO "+role); err != nil {
		t.Fatal(err)
	}
	dsn.User = url.UserPassword(roleName, "disposable-testing-only")
	restricted, err := openPostgresDatabase(dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	defer restricted.Close()
	for _, query := range []string{
		"SELECT id FROM public.fugue_dns_records LIMIT 1",
		"UPDATE public.fugue_dns_records SET ttl=60 WHERE false",
		"SELECT key FROM public.fugue_meta LIMIT 1",
		"SELECT id FROM public.fugue_tenants LIMIT 1",
		"SELECT fugue_hosted_dns_maintenance.public_answers_v1('[]',4)",
	} {
		if _, err := restricted.ExecContext(ctx, query); err == nil {
			t.Fatalf("worker had general database privileges: %q", query)
		}
	}
	for _, raw := range []string{
		"8.8.8.8", "1.1.1.1", "0.0.0.1", "10.0.1.1", "100.127.0.1", "127.8.9.1", "169.254.169.254",
		"172.31.0.1", "192.0.0.9", "192.0.2.1", "192.168.0.1", "198.19.0.1", "198.51.100.1",
		"203.0.113.1", "224.0.0.1", "255.255.255.255", "2606:4700:4700::1111",
		"::", "::1", "::ffff:8.8.8.8", "64:ff9b:1::1", "100::1", "2001:db8::1", "fd00::1", "fe80::1", "ff02::1",
		"8.8.8.0/24", "999.1.2.3", "::abcd::1",
	} {
		ip, parseErr := netip.ParseAddr(raw)
		expected := parseErr == nil && platformconfig.PublicDNSFlattenIP(ip)
		family := 4
		if ip.Is6() {
			family = 6
		}
		answer, err := json.Marshal([]string{raw})
		if err != nil {
			t.Fatal(err)
		}
		var allowed bool
		if err := api.db.QueryRowContext(ctx, "SELECT fugue_hosted_dns_maintenance.public_answers_v1($1::jsonb,$2)", string(answer), family).Scan(&allowed); err != nil || allowed != expected {
			t.Fatalf("SQL address boundary disagrees with resolver for %q: allowed=%t expected=%t err=%v", raw, allowed, expected, err)
		}
	}
	zone := putHostedDNSTestZone(t, api, createHostedDNSTestTenant(t, api, "dns-worker"), "example.com")
	record := putHostedDNSTestRecord(t, api, zone, model.DNSRecord{
		Name: "@", Type: model.DNSRecordTypeALIAS, Values: []string{"valid.example.net"},
		Source: model.DNSRecordSourceUser, Status: model.DNSRecordStatusActive,
	})
	if _, err := api.db.ExecContext(ctx, `UPDATE fugue_dns_records SET flatten_target='',values_json='[]' WHERE id=$1`, record.ID); err != nil {
		t.Fatal(err)
	}
	worker, err := hosteddns.OpenPostgresWorker(ctx, dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	if more, err := worker.Reconcile(ctx); err != nil || more {
		t.Fatalf("standby advanced the cursor: more=%t err=%v", more, err)
	}
	progress, err := worker.Progress(ctx)
	if err != nil || progress.Owner != "api" || progress.CompletedAt.Valid {
		t.Fatalf("standby recorded work: %+v %v", progress, err)
	}
	var zones int
	if err := restricted.QueryRowContext(ctx, "SELECT count(*) FROM fugue_hosted_dns_maintenance.records_v1('')").Scan(&zones); err != nil || zones != 0 {
		t.Fatalf("standby read production input: %d %v", zones, err)
	}
	if _, err := api.db.ExecContext(ctx, "INSERT INTO fugue_meta (key,value) VALUES('hosted-dns-flatten/v1/writer','worker')"); err != nil {
		t.Fatal(err)
	}
	for _, unsafe := range []string{"[false]", "[null]", "[\"fd00::1\"]", "[\"169.254.169.254\"]", "[\"2606:4700:4700::1111\"]"} {
		const attempt = "SELECT fugue_hosted_dns_maintenance.commit_v1(" +
			"$1,$2,$3,$4,$5,$6,$7::jsonb,'[]','resolved','active',now(),'','')"
		if _, err := restricted.ExecContext(ctx, attempt, zone.ID, zone.TenantID, zone.ProjectID, record.ID, record.CreatedAt, record.UpdatedAt, unsafe); err == nil {
			t.Fatalf("worker accepted invalid A answers %s", unsafe)
		}
	}
	// Fail the cursor write after the worker computes and writes an answer.
	// This models a page interrupted before its durable checkpoint.
	if _, err := api.db.ExecContext(ctx, `
CREATE FUNCTION hosted_dns_test_reject_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fixture rejected hosted DNS checkpoint';
END $$;
CREATE TRIGGER hosted_dns_test_reject_checkpoint BEFORE INSERT OR UPDATE ON fugue_meta
FOR EACH ROW WHEN (NEW.key = 'hosted-dns-flatten/v1/progress')
EXECUTE FUNCTION hosted_dns_test_reject_checkpoint();
`); err != nil {
		t.Fatal(err)
	}
	if more, err := worker.Reconcile(ctx); err == nil || more {
		t.Fatalf("failed checkpoint unexpectedly committed: more=%t err=%v", more, err)
	}
	unchanged, err := api.GetDNSRecord(zone.ID, record.ID)
	if err != nil || unchanged.ResolveError != "" {
		t.Fatalf("answer was written without its checkpoint: %+v %v", unchanged, err)
	}
	progress, err = worker.Progress(ctx)
	if err != nil || progress.Owner != "worker" || progress.CompletedAt.Valid || progress.Cursor != "" {
		t.Fatalf("failed checkpoint changed progress: %+v %v", progress, err)
	}
	if _, err := api.db.ExecContext(ctx, `DROP TRIGGER hosted_dns_test_reject_checkpoint ON fugue_meta`); err != nil {
		t.Fatal(err)
	}
	if more, err := worker.Reconcile(ctx); err != nil || !more {
		t.Fatalf("processed page did not advance the cursor: more=%t err=%v", more, err)
	}
	observed, err := api.GetDNSRecord(zone.ID, record.ID)
	if err != nil || observed.FlattenStatus != model.DNSRecordFlattenStatusError || observed.ResolveError != "flatten target is empty" {
		t.Fatalf("worker did not publish the bounded DNS result: %+v %v", observed, err)
	}
	replacement, err := hosteddns.OpenPostgresWorker(ctx, dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	progress, err = replacement.Progress(ctx)
	if err != nil || progress.Owner != "worker" || !progress.CompletedAt.Valid || !progress.Updated.Valid || progress.Updated.Int64 != 1 || progress.Cursor != record.ID {
		t.Fatalf("worker restart lost the committed DNS pass: %+v %v", progress, err)
	}
	if more, err := replacement.Reconcile(ctx); err != nil || more {
		t.Fatalf("last page did not finish the sweep: more=%t err=%v", more, err)
	}
	progress, err = replacement.Progress(ctx)
	if err != nil || progress.Cursor != "" || progress.Updated.Int64 != 0 {
		t.Fatalf("worker did not start its next traversal: %+v %v", progress, err)
	}
}

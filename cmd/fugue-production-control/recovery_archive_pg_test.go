package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
)

func TestPostgresCreateJournalRecovery(t *testing.T) {
	sourceURL := os.Getenv("FUGUE_TEST_RECOVERY_ARCHIVE_SOURCE_URL")
	targetURL := os.Getenv("FUGUE_TEST_RECOVERY_ARCHIVE_RESTORE_URL")
	if sourceURL == "" || targetURL == "" {
		t.Skip("set recovery archive source and restore URLs to separate empty disposable PostgreSQL databases with verified TLS on source")
	}
	if sourceURL == targetURL {
		t.Fatal("source and restore databases must differ")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	openEmpty := func(dsn string) *sql.DB {
		t.Helper()
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		var empty bool
		if err := db.QueryRowContext(ctx, `SELECT NOT EXISTS (
			SELECT FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
			WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%')`).Scan(&empty); err != nil || !empty {
			t.Fatalf("test requires an empty disposable database: empty=%v err=%v", empty, err)
		}
		return db
	}
	source, restored := openEmpty(sourceURL), openEmpty(targetURL)
	if err := productioncontrol.ApplySchema(ctx, source); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, `
		CREATE ROLE fugue_archive_fixture_reader LOGIN;
		CREATE TABLE public.archive_marker (id int PRIMARY KEY);
		INSERT INTO public.archive_marker VALUES (1);
		GRANT SELECT ON public.archive_marker TO fugue_archive_fixture_reader;
		GRANT USAGE ON SCHEMA fugue_control TO fugue_archive_fixture_reader;
		GRANT SELECT ON ALL TABLES IN SCHEMA fugue_control TO fugue_archive_fixture_reader;
	`); err != nil {
		t.Fatal(err)
	}
	serviceEndpoint, err := url.Parse(sourceURL)
	if err != nil {
		t.Fatal(err)
	}
	serviceEndpoint.User = url.User("fugue_archive_fixture_reader")
	ca := serviceEndpoint.Query().Get("sslrootcert")
	if ca == "" {
		t.Fatal("recovery archive test requires a local root CA path")
	}
	dir := t.TempDir()
	serviceFile := filepath.Join(dir, "pg-service.conf")
	serviceConfig := "[fixture]\nuser=fugue_archive_fixture_reader\nsslrootcert=" + ca + "\n"
	if err := os.WriteFile(serviceFile, []byte(serviceConfig), 0600); err != nil {
		t.Fatal(err)
	}
	key, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	const cluster = "fixture-cluster"
	raw, err := json.Marshal(map[string]any{"cluster": cluster, "database": serviceEndpoint.String(),
		"ownerPublicKeys": map[string][]byte{"owner-a": key}})
	if err != nil {
		t.Fatal(err)
	}
	journalFile := filepath.Join(dir, "journal.json")
	if err := os.WriteFile(journalFile, raw, 0600); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	archiveDir := filepath.Join(dir, "archive")
	args := []string{"journal-recovery-create", journalFile, serviceFile, "fixture", archiveDir}
	if err := run(ctx, args, &out, time.Now); err != nil {
		t.Fatal(err)
	}
	var record recoveryArchiveRecord
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record.Checkpoint.Cluster != cluster || len(record.Checkpoint.Tables) == 0 || record.Archive != recoveryDumpFile {
		t.Fatalf("incorrect recovery record: %+v", record)
	}
	for name, want := range map[string]os.FileMode{
		archiveDir: 0700, filepath.Join(archiveDir, recoveryDumpFile): 0600, filepath.Join(archiveDir, recoveryRecordFile): 0600,
	} {
		fi, err := os.Stat(name)
		if err != nil || fi.Mode().Perm() != want {
			t.Fatalf("non-private archive file %s: %v", filepath.Base(name), err)
		}
	}
	archivePath := filepath.Join(archiveDir, recoveryDumpFile)
	dump, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(dump)
	if record.Bytes != int64(len(dump)) || record.SHA256 != "sha256:"+hex.EncodeToString(sum[:]) {
		t.Fatal("recovery record does not pin exact archive bytes")
	}
	if output, err := exec.CommandContext(ctx, "pg_restore", "--exit-on-error", "--single-transaction",
		"--no-owner", "--no-privileges", "--dbname="+targetURL, archivePath).CombinedOutput(); err != nil {
		t.Fatalf("pg_restore: %v: %s", err, output)
	}
	restoredJournal, err := productioncontrol.NewJournal(restored, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restoredJournal.VerifyRecoveryCheckpoint(ctx, cluster, record.Checkpoint.Digest); err != nil {
		t.Fatal(err)
	}
	var marker int
	if err := restored.QueryRowContext(ctx, `SELECT id FROM public.archive_marker`).Scan(&marker); err != nil || marker != 1 {
		t.Fatalf("backup did not restore non-journal database contents: marker=%d err=%v", marker, err)
	}
	if err := run(ctx, args, &out, time.Now); err == nil {
		t.Fatal("existing archive location was accepted")
	}
	if _, err := os.Stat(archivePath); err != nil {
		t.Fatal("retry modified pre-existing archive")
	}

	out.Reset()
	failedDir := filepath.Join(dir, "failed")
	if _, err := source.ExecContext(ctx, `REVOKE SELECT ON public.archive_marker FROM fugue_archive_fixture_reader`); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, []string{args[0], journalFile, serviceFile, "fixture", failedDir}, &out, time.Now); err == nil || out.Len() != 0 {
		t.Fatal("partial or unreadable database archive was reported as committed")
	}
	for _, file := range []string{recoveryRecordFile, recoveryRecordTemp, recoveryDumpFile} {
		if _, err := os.Stat(filepath.Join(failedDir, file)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("incomplete archive file not cleaned up: %s: %v", file, err)
		}
	}
}

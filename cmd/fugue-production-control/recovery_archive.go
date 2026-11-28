package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"fugue/internal/productioncontrol"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

const (
	recoveryDumpFile   = "control-plane.dump"
	recoveryRecordFile = "recovery.json"
	recoveryRecordTemp = "recovery.json.partial"
)

var (
	pgServicePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
	pgDatabaseTarget = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
)

type recoveryArchiveRecord struct {
	Format     string                                      `json:"format"`
	Archive    string                                      `json:"archive"`
	Bytes      int64                                       `json:"bytes"`
	SHA256     string                                      `json:"sha256"`
	Checkpoint productioncontrol.JournalRecoveryCheckpoint `json:"checkpoint"`
}

func createJournalRecovery(ctx context.Context, source, serviceFile, service, dir string, out io.Writer) error {
	info, err := os.Lstat(serviceFile)
	if !filepath.IsAbs(serviceFile) || err != nil || !info.Mode().IsRegular() ||
		info.Mode().Perm()&0077 != 0 || !pgServicePattern.MatchString(service) {
		return errors.New("recovery requires a local private PostgreSQL service file and an explicit service name")
	}
	cfg, keys, cluster, err := productioncontrol.LoadJournalEndpoint(source)
	if err != nil {
		return err
	}
	if !pgDatabaseTarget.MatchString(cfg.Host) || !pgDatabaseTarget.MatchString(cfg.Database) {
		return errors.New("recovery requires an explicit supported PostgreSQL host and database")
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = make(map[string]string)
	}
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	db.SetMaxOpenConns(1)
	j, err := productioncontrol.NewJournal(db, keys)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	record, err := stageJournalRecovery(ctx, j, cluster, dir, func(ctx context.Context, snapshot string, out io.Writer) error {
		return dumpRecoverySnapshot(ctx, cfg, serviceFile, service, snapshot, out)
	})
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(record)
}

func dumpRecoverySnapshot(ctx context.Context, cfg *pgx.ConnConfig, serviceFile, service, snapshot string, out io.Writer) error {
	// Explicit host/database and verified TLS override service defaults.
	// Disallow hostaddr in the connection environment by using only the
	// mounted service file, which is trusted operator configuration.
	conn := "service=" + service + " host=" + cfg.Host + " port=" + strconv.Itoa(int(cfg.Port)) +
		" dbname=" + cfg.Database + " sslmode=verify-full"
	cmd := exec.CommandContext(ctx, "pg_dump", "--dbname="+conn, "--snapshot="+snapshot, "--format=custom",
		"--no-owner", "--no-privileges", "--lock-wait-timeout=10s")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "PG") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "PGSERVICEFILE="+serviceFile)
	cmd.Stdout = out
	cmd.WaitDelay = 5 * time.Second
	if err := cmd.Run(); err != nil {
		// libpq errors can include addresses or connection credentials.
		return errors.New("pg_dump failed; check the independent service credential, source identity and read permissions")
	}
	return nil
}

func stageJournalRecovery(ctx context.Context, j *productioncontrol.Journal, cluster, dir string,
	dump func(context.Context, string, io.Writer) error) (recoveryArchiveRecord, error) {
	var record recoveryArchiveRecord
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return record, errors.New("recovery requires a new absolute private directory")
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return record, errors.New("cannot create a new private recovery directory")
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return record, err
	}
	defer root.Close()
	complete := false
	defer func() {
		if !complete {
			_ = root.Remove(recoveryRecordFile)
			_ = root.Remove(recoveryRecordTemp)
			_ = root.Remove(recoveryDumpFile)
		}
	}()
	archive, err := root.OpenFile(recoveryDumpFile, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return record, err
	}
	defer archive.Close()
	hash := sha256.New()
	checkpoint, err := j.CaptureRecovery(ctx, cluster, func(ctx context.Context, snapshot string) error {
		return dump(ctx, snapshot, io.MultiWriter(archive, hash))
	})
	if err != nil {
		return record, err
	}
	size, err := archive.Seek(0, io.SeekCurrent)
	if err != nil {
		return record, err
	}
	header := make([]byte, 5)
	if _, err := archive.ReadAt(header, 0); err != nil || string(header) != "PGDMP" {
		return record, errors.New("backup reader did not produce a PostgreSQL custom archive")
	}
	if err := archive.Sync(); err != nil {
		return record, err
	}
	if err := archive.Close(); err != nil {
		return record, err
	}
	record = recoveryArchiveRecord{"fugue.production.recovery-archive.v1", recoveryDumpFile, size,
		"sha256:" + hex.EncodeToString(hash.Sum(nil)), checkpoint}
	manifest, err := root.OpenFile(recoveryRecordTemp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return recoveryArchiveRecord{}, err
	}
	err = json.NewEncoder(manifest).Encode(record)
	if err == nil {
		err = manifest.Sync()
	}
	if err := errors.Join(err, manifest.Close()); err != nil {
		return recoveryArchiveRecord{}, err
	}
	if err := root.Rename(recoveryRecordTemp, recoveryRecordFile); err != nil {
		return recoveryArchiveRecord{}, err
	}
	// The manifest is the readiness marker. Persist the directory entries
	// before returning a record that can be pinned in external custody.
	top, err := root.Open(".")
	if err != nil {
		return recoveryArchiveRecord{}, err
	}
	if err := errors.Join(top.Sync(), top.Close()); err != nil {
		return recoveryArchiveRecord{}, err
	}
	complete = true
	return record, nil
}

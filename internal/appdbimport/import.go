// Package appdbimport advances persisted app database imports.
package appdbimport

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"fugue/internal/appmanageddb"
	"fugue/internal/model"
)

const (
	MaxDumpBytes     = 128 << 20
	maxExpandedBytes = 256 << 20
	pollInterval     = 5 * time.Second
	batchSize        = 5
	renewInterval    = 15 * time.Second
	storeTimeout     = 5 * time.Second
	maxCommandOutput = 2000
)

// ErrUnclaimable is an ordinary race: another instance took the work, a
// lifecycle gate closed, or this process is not the selected writer.
var ErrUnclaimable = errors.New("database import is not available to this executor")

type Archive struct {
	Filename string
	Bytes    []byte
}

// ExecutionStore grants only import execution capabilities. The adapter
// binds the executor identity; the runner cannot request a different one.
type ExecutionStore interface {
	ListPending(context.Context, int) ([]string, error)
	Claim(context.Context, string, string) (model.AppDatabaseImportJob, error)
	AppendLog(context.Context, string, string, string) error
	Complete(context.Context, string, string, string, string, string) error
	BeginEffect(context.Context, string, string) error
	Renew(context.Context, string, string) error
	RecoverExpired(context.Context, int) error
	ReadConnection(context.Context, string, string) (appmanageddb.Connection, error)
	ReadArchive(context.Context, string, string, int) (Archive, error)
}

func Run(ctx context.Context, db ExecutionStore, runner func(context.Context, model.AppDatabaseImportJob) (string, error), logf func(string, ...any)) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		processPending(ctx, db, runner, logf)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func processPending(ctx context.Context, db ExecutionStore, runner func(context.Context, model.AppDatabaseImportJob) (string, error), logf func(string, ...any)) {
	if ctx.Err() != nil {
		return
	}
	recoveryCtx, stopRecovery := context.WithTimeout(ctx, storeTimeout)
	err := db.RecoverExpired(recoveryCtx, batchSize)
	stopRecovery()
	if err != nil {
		if !errors.Is(err, ErrUnclaimable) && ctx.Err() == nil && logf != nil {
			logf("database import recovery failed: %v", err)
		}
		return
	}
	listCtx, stopList := context.WithTimeout(ctx, storeTimeout)
	jobs, err := db.ListPending(listCtx, batchSize)
	stopList()
	if err != nil {
		if !errors.Is(err, ErrUnclaimable) && ctx.Err() == nil && logf != nil {
			logf("list pending database import jobs failed: %v", err)
		}
		return
	}
	for _, id := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
		}
		attempt := model.NewID("dbimport-attempt")
		claimCtx, stopClaim := context.WithTimeout(ctx, storeTimeout)
		job, err := db.Claim(claimCtx, id, attempt)
		stopClaim()
		if err != nil {
			if !errors.Is(err, ErrUnclaimable) && ctx.Err() == nil && logf != nil {
				logf("claim database import job %s failed: %v", id, err)
			}
			continue
		}
		runClaimed(ctx, db, job, attempt, runner, logf)
	}
}

func runClaimed(parent context.Context, db ExecutionStore, job model.AppDatabaseImportJob, attempt string, runner func(context.Context, model.AppDatabaseImportJob) (string, error), logf func(string, ...any)) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				probe, stop := context.WithTimeout(ctx, storeTimeout)
				err := db.Renew(probe, job.ID, attempt)
				stop()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	appendLog := func(message string) {
		probe, stop := context.WithTimeout(ctx, storeTimeout)
		defer stop()
		if err := db.AppendLog(probe, job.ID, attempt, message); err != nil && logf != nil {
			logf("append database import job log %s failed: %v", job.ID, err)
		}
	}
	beginEffect := func() error {
		probe, stop := context.WithTimeout(ctx, storeTimeout)
		defer stop()
		return db.BeginEffect(probe, job.ID, attempt)
	}
	appendLog("starting database import")
	var result string
	var runErr error
	if runner == nil {
		result, runErr = execute(ctx, db, job, attempt, appendLog, beginEffect)
	} else if runErr = beginEffect(); runErr == nil {
		result, runErr = runner(ctx, job)
	}
	if ctx.Err() == nil {
		status, errorMessage := model.OperationStatusCompleted, ""
		if runErr != nil {
			status, result, errorMessage = model.OperationStatusFailed, "", runErr.Error()
		}
		finalize, stop := context.WithTimeout(ctx, storeTimeout)
		err := db.Complete(finalize, job.ID, attempt, status, result, errorMessage)
		stop()
		if err != nil && logf != nil {
			logf("complete database import job %s failed: %v", job.ID, err)
		}
	}
	cancel()
	<-heartbeatDone
}

func execute(ctx context.Context, db ExecutionStore, job model.AppDatabaseImportJob, attempt string, appendLog func(string), beginEffect func() error) (string, error) {
	resolve, stopResolve := context.WithTimeout(ctx, storeTimeout)
	connection, err := db.ReadConnection(resolve, job.ID, attempt)
	stopResolve()
	if err != nil {
		return "", err
	}
	read, stopRead := context.WithTimeout(ctx, 15*time.Second)
	archive, err := db.ReadArchive(read, job.ID, attempt, MaxDumpBytes)
	stopRead()
	if err != nil {
		return "", err
	}
	if len(archive.Bytes) == 0 {
		return "", fmt.Errorf("database import dump is empty")
	}
	data, err := maybeGunzipDatabaseDump(archive.Bytes, archive.Filename)
	if err != nil {
		return "", err
	}
	format := detectDatabaseImportFormat(job.Format, data, archive.Filename)
	if format == "" {
		return "", fmt.Errorf("could not detect database import format")
	}
	if appendLog != nil {
		appendLog(fmt.Sprintf("running %s import into %s/%s", format, connection.Host, connection.Database))
	}
	output, err := runPostgresImportCommand(ctx, connection, format, job.Clean, data, beginEffect)
	if err != nil {
		return "", err
	}
	message := fmt.Sprintf("imported %d bytes into database %s", len(data), connection.Database)
	if strings.TrimSpace(output) != "" {
		message += ": " + output
	}
	return message, nil
}

func NormalizeFormat(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.AppDatabaseImportFormatAuto:
		return model.AppDatabaseImportFormatAuto
	case model.AppDatabaseImportFormatSQL:
		return model.AppDatabaseImportFormatSQL
	case model.AppDatabaseImportFormatCustom:
		return model.AppDatabaseImportFormatCustom
	default:
		return ""
	}
}

func maybeGunzipDatabaseDump(data []byte, filename string) ([]byte, error) {
	return ExpandDump(data, filename, maxExpandedBytes)
}

func ExpandDump(data []byte, filename string, maxExpandedBytes int64) ([]byte, error) {
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		if !strings.HasSuffix(strings.ToLower(strings.TrimSpace(filename)), ".gz") {
			return data, nil
		}
	}
	if maxExpandedBytes < 0 {
		return nil, errors.New("invalid gzip database import expansion boundary")
	}
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("open gzip dump: %w", err)
	}
	defer reader.Close()
	out, err := io.ReadAll(io.LimitReader(reader, maxExpandedBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read gzip dump: %w", err)
	}
	if int64(len(out)) > maxExpandedBytes {
		return nil, fmt.Errorf("gzip database import expands beyond %d bytes", maxExpandedBytes)
	}
	return out, nil
}

func detectDatabaseImportFormat(requested string, data []byte, filename string) string {
	requested = NormalizeFormat(requested)
	if requested != model.AppDatabaseImportFormatAuto {
		return requested
	}
	if bytes.HasPrefix(data, []byte("PGDMP")) {
		return model.AppDatabaseImportFormatCustom
	}
	lowerName := strings.ToLower(strings.TrimSpace(filename))
	switch {
	case strings.HasSuffix(lowerName, ".dump"), strings.HasSuffix(lowerName, ".backup"), strings.HasSuffix(lowerName, ".custom"):
		return model.AppDatabaseImportFormatCustom
	default:
		return model.AppDatabaseImportFormatSQL
	}
}

func runPostgresImportCommand(ctx context.Context, connection appmanageddb.Connection, format string, clean bool, data []byte, beginEffect func() error) (string, error) {
	pgEnv, commonArgs, err := postgresCommandEnvAndArgs(connection)
	if err != nil {
		return "", err
	}
	tempFile, err := os.CreateTemp("", "fugue-db-import-*")
	if err != nil {
		return "", fmt.Errorf("create temporary database import file: %w", err)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)
	if format == model.AppDatabaseImportFormatSQL {
		data = unwrapPostgresDumpRestrictions(data)
	}
	if _, err := tempFile.Write(data); err != nil {
		tempFile.Close()
		return "", fmt.Errorf("write temporary database import file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return "", fmt.Errorf("close temporary database import file: %w", err)
	}

	var name string
	var args []string
	switch format {
	case model.AppDatabaseImportFormatCustom:
		name = "pg_restore"
		args = append([]string{"--no-owner", "--no-privileges"}, commonArgs...)
		if clean {
			args = append(args, "--clean", "--if-exists")
		}
		args = append(args, tempPath)
	case model.AppDatabaseImportFormatSQL:
		name = "psql"
		args = append([]string{"--no-psqlrc", "--no-password", "--set", "ON_ERROR_STOP=on"}, commonArgs...)
		// Restrict client commands for the entire input using a fresh key
		// independent of the archive. Older clients fail before reading it.
		args = append(args, "--command", "\\restrict "+rand.Text())
		if clean {
			args = append(args, "--command", sqlCleanPublicSchema(connection.User))
		}
		args = append(args, "--file", tempPath)
	default:
		return "", fmt.Errorf("unsupported database import format %q", format)
	}

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = pgEnv
	var output commandOutput
	cmd.Stdout = &output
	cmd.Stderr = &output
	if beginEffect != nil {
		if err := beginEffect(); err != nil {
			return "", fmt.Errorf("record database import execution: %w", err)
		}
	}
	if err := cmd.Run(); err != nil {
		return output.String(), fmt.Errorf("%s failed: %w: %s", name, err, output.String())
	}
	return output.String(), nil
}

var dumpRestrictionStart = regexp.MustCompile(`(?m)^(?:--[^\r\n]*\r?\n|[ \t]*\r?\n)*\\restrict ([A-Za-z0-9]+)[ \t]*\r?$\n`)
var dumpRestrictionEnd = regexp.MustCompile(`(?m)^\\unrestrict ([A-Za-z0-9]+)[ \t]*\r?(?:\n[ \t\r\n]*)?\z`)

// Standard pg_dump plain archives have their own outer guard. Remove only a
// matching pair surrounding the complete dump; psql supplies our guard instead.
func unwrapPostgresDumpRestrictions(data []byte) []byte {
	start := dumpRestrictionStart.FindSubmatchIndex(data)
	if start == nil || start[0] != 0 {
		return data
	}
	end := dumpRestrictionEnd.FindSubmatchIndex(data[start[1]:])
	if end == nil || !bytes.Equal(data[start[2]:start[3]], data[start[1]+end[2]:start[1]+end[3]]) {
		return data
	}
	return data[start[1] : start[1]+end[0]]
}

func postgresCommandEnvAndArgs(connection appmanageddb.Connection) ([]string, []string, error) {
	parsed, err := url.Parse(connection.DSN)
	if err != nil {
		return nil, nil, fmt.Errorf("parse database dsn: %w", err)
	}
	host := firstNonEmptyString(connection.Host, parsed.Hostname())
	port := firstNonEmptyString(connection.Port, parsed.Port(), "5432")
	database := firstNonEmptyString(connection.Database, strings.TrimPrefix(parsed.Path, "/"))
	user := firstNonEmptyString(connection.User, parsed.User.Username())
	if host == "" || database == "" || user == "" {
		return nil, nil, fmt.Errorf("database connection is missing host, database, or user")
	}
	password, _ := parsed.User.Password()
	env := []string{
		"PGHOST=" + host,
		"PGPORT=" + port,
		"PGDATABASE=" + database,
		"PGUSER=" + user,
		"PGSSLMODE=" + firstNonEmptyString(parsed.Query().Get("sslmode"), "disable"),
	}
	if password != "" {
		env = append(env, "PGPASSWORD="+password)
	}
	args := []string{
		"--host", host,
		"--port", port,
		"--username", user,
		"--dbname", (&url.URL{Scheme: "postgresql", Host: net.JoinHostPort(host, port), Path: "/" + database, User: url.User(user)}).String(),
	}
	return env, args, nil
}

func sqlCleanPublicSchema(owner string) string {
	quotedOwner := quotePostgresIdentifier(strings.TrimSpace(owner))
	if quotedOwner == "" {
		quotedOwner = "CURRENT_USER"
	}
	return "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO " + quotedOwner + "; GRANT ALL ON SCHEMA public TO public;"
}

func quotePostgresIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

// commandOutput drains the subprocess even after reaching the log limit.
// Returning a short write could interrupt a valid import or block its pipes.
type commandOutput struct {
	prefix    [maxCommandOutput]byte
	n         int
	truncated bool
}

func (out *commandOutput) Write(p []byte) (int, error) {
	n := copy(out.prefix[out.n:], p)
	out.n += n
	out.truncated = out.truncated || n < len(p)
	return len(p), nil
}

func (out *commandOutput) String() string {
	raw := out.prefix[:out.n]
	if out.truncated {
		// Don't emit a partial trailing rune at the output boundary.
		for len(raw) > 0 {
			r, size := utf8.DecodeLastRune(raw)
			if r != utf8.RuneError || size != 1 {
				break
			}
			raw = raw[:len(raw)-1]
		}
		return strings.TrimSpace(string(raw)) + "...[truncated]"
	}
	return strings.TrimSpace(string(raw))
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

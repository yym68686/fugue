package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) CreateProtectiveBackup(targetStore, generation string) (string, error) {
	if s == nil {
		return "", ErrInvalidInput
	}
	targetStore = strings.TrimSpace(targetStore)
	generation = strings.TrimSpace(generation)
	if targetStore == "" || generation == "" {
		return "", ErrInvalidInput
	}
	backupDir := strings.TrimSpace(os.Getenv("FUGUE_STORE_PROMOTION_BACKUP_DIR"))
	if backupDir == "" {
		backupDir = filepath.Join(os.TempDir(), "fugue-store-backups")
	}
	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		return "", err
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	name := safeBackupName(targetStore + "-" + generation + "-" + stamp)
	if s.usingDatabase() {
		if strings.TrimSpace(s.databaseURL) == "" {
			return "", ErrInvalidInput
		}
		path := filepath.Join(backupDir, name+".dump")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "pg_dump", "--format=custom", "--file", path, s.databaseURL)
		if output, err := cmd.CombinedOutput(); err != nil {
			_ = os.Remove(path)
			return "", fmt.Errorf("pg_dump protective backup failed: %w: %s", err, strings.TrimSpace(string(output)))
		}
		return path, nil
	}
	if strings.TrimSpace(s.path) == "" {
		return "", ErrInvalidInput
	}
	path := filepath.Join(backupDir, name+".json")
	if err := copyFile(path, s.path, 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func (s *Store) VerifyControlPlanePermissions(requiredGrants []string) ([]model.StoreInvariantCheck, error) {
	if s == nil {
		return nil, ErrInvalidInput
	}
	if !s.usingDatabase() {
		return []model.StoreInvariantCheck{{
			Name:    "permission_verification",
			Pass:    true,
			Message: "json store verified through file-backed API service identity",
		}}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.CheckReadiness(ctx); err != nil {
		return []model.StoreInvariantCheck{{
			Name:    "permission_verification",
			Pass:    false,
			Message: err.Error(),
		}}, nil
	}
	if len(requiredGrants) == 0 {
		requiredGrants = []string{
			"fugue_tenants:select",
			"fugue_projects:select",
			"fugue_apps:select",
			"fugue_runtimes:select",
			"fugue_edge_nodes:select",
			"fugue_dns_nodes:select",
			"fugue_store_promotions:insert",
			"fugue_store_promotions:update",
		}
	}
	checks := make([]model.StoreInvariantCheck, 0, len(requiredGrants)+1)
	for _, grant := range requiredGrants {
		tableName, privilege, ok := parseRequiredGrant(grant)
		if !ok {
			checks = append(checks, model.StoreInvariantCheck{Name: "grant_" + safeBackupName(grant), Pass: false, Message: "required grant must be table:privilege"})
			continue
		}
		var pass bool
		if err := s.db.QueryRowContext(ctx, `SELECT has_table_privilege(current_user, $1, $2)`, tableName, privilege).Scan(&pass); err != nil {
			checks = append(checks, model.StoreInvariantCheck{Name: "grant_" + tableName + "_" + privilege, Pass: false, Message: err.Error()})
			continue
		}
		checks = append(checks, model.StoreInvariantCheck{Name: "grant_" + tableName + "_" + privilege, Pass: pass, Message: "current service account " + privilege + " on " + tableName})
	}
	sequenceCheck := model.StoreInvariantCheck{Name: "grant_sequences_usage", Pass: true, Message: "no database sequences require nextval grants"}
	rows, err := s.db.QueryContext(ctx, `SELECT sequence_schema || '.' || sequence_name FROM information_schema.sequences WHERE sequence_schema = 'public' ORDER BY sequence_name`)
	if err != nil {
		sequenceCheck.Pass = false
		sequenceCheck.Message = err.Error()
		checks = append(checks, sequenceCheck)
		return checks, nil
	}
	defer rows.Close()
	for rows.Next() {
		var sequenceName string
		if err := rows.Scan(&sequenceName); err != nil {
			sequenceCheck.Pass = false
			sequenceCheck.Message = err.Error()
			continue
		}
		var pass bool
		if err := s.db.QueryRowContext(ctx, `SELECT has_sequence_privilege(current_user, $1, 'USAGE')`, sequenceName).Scan(&pass); err != nil || !pass {
			sequenceCheck.Pass = false
			if err != nil {
				sequenceCheck.Message = err.Error()
			} else {
				sequenceCheck.Message = "current service account lacks nextval usage on " + sequenceName
			}
			break
		}
		sequenceCheck.Count++
		sequenceCheck.Message = "current service account can use sequences for nextval"
	}
	if err := rows.Err(); err != nil {
		sequenceCheck.Pass = false
		sequenceCheck.Message = err.Error()
	}
	checks = append(checks, sequenceCheck)
	return checks, nil
}

func copyFile(target, source string, mode os.FileMode) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := target + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, target)
}

func parseRequiredGrant(raw string) (string, string, bool) {
	tableName, privilege, ok := strings.Cut(strings.TrimSpace(raw), ":")
	tableName = strings.TrimSpace(tableName)
	privilege = strings.ToLower(strings.TrimSpace(privilege))
	switch privilege {
	case "select", "insert", "update", "delete":
	default:
		return "", "", false
	}
	if tableName == "" || strings.ContainsAny(tableName, " ;'\"") {
		return "", "", false
	}
	return tableName, privilege, ok
}

func safeBackupName(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		sum := sha256.Sum256([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
		return hex.EncodeToString(sum[:])[:16]
	}
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-.")
	if out == "" {
		sum := sha256.Sum256([]byte(raw))
		return hex.EncodeToString(sum[:])[:16]
	}
	return out
}

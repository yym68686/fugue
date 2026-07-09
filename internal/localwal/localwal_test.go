package localwal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAppendAndReadAll(t *testing.T) {
	now := time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC)
	expires := now.Add(time.Minute)
	record, err := NewRecord("edge-worker", "edge-1", "serve_lkg", map[string]string{
		"route_generation": "routegen_1",
		"reason":           "control_plane_read_failed",
	}, "routegen_1", &expires, now)
	if err != nil {
		t.Fatalf("new record: %v", err)
	}
	record.SafetyClass = "observe_only"
	path := filepath.Join(t.TempDir(), "autonomy.wal")
	if err := Append(path, record); err != nil {
		t.Fatalf("append record: %v", err)
	}
	records, err := ReadAll(path)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].EvidenceHash == "" || records[0].EvidenceHash != EvidenceHash(records[0].Evidence) {
		t.Fatalf("expected stable evidence hash, got %+v", records[0])
	}
	if records[0].ExpiresAt == nil || !records[0].ExpiresAt.Equal(expires) {
		t.Fatalf("expected expires_at, got %+v", records[0].ExpiresAt)
	}
}

func TestReadAllRejectsEvidenceHashMismatch(t *testing.T) {
	now := time.Date(2026, 7, 9, 10, 0, 0, 0, time.UTC)
	record, err := NewRecord("dns-server", "dns-1", "temporary_filter", map[string]string{"edge_id": "edge-a"}, "dnsgen_1", nil, now)
	if err != nil {
		t.Fatalf("new record: %v", err)
	}
	path := filepath.Join(t.TempDir(), "autonomy.wal")
	if err := Append(path, record); err != nil {
		t.Fatalf("append record: %v", err)
	}
	body := mustReadFile(t, path)
	body = strings.Replace(body, `"edge-a"`, `"edge-b"`, 1)
	if err := writeFile(path, body); err != nil {
		t.Fatalf("write corrupt wal: %v", err)
	}
	if _, err := ReadAll(path); err == nil || !strings.Contains(err.Error(), "evidence hash mismatch") {
		t.Fatalf("expected evidence hash mismatch, got %v", err)
	}
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	return string(data)
}

func writeFile(path, body string) error {
	return os.WriteFile(path, []byte(body), 0o600)
}

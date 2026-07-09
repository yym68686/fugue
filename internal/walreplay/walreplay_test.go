package walreplay

import (
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/localwal"
)

func TestReplayMergesIncidentsAndExpiresTemporaryActions(t *testing.T) {
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "autonomy.wal")
	first, err := localwal.NewRecord("dns-server", "dns-a", "temporary_filter", map[string]string{"edge_id": "edge-a", "reason": "probe_fail"}, "dnsgen_1", ptr(now.Add(time.Minute)), now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("new first: %v", err)
	}
	first.Subject = "edge-a"
	second := first
	second.ID = ""
	second.RecordedAt = now.Add(-30 * time.Second)
	expired, err := localwal.NewRecord("edge-worker", "edge-b", "endpoint_lkg_fallback", map[string]string{"hostname": "api.example.com"}, "routegen_old", ptr(now.Add(-time.Second)), now.Add(-2*time.Minute))
	if err != nil {
		t.Fatalf("new expired: %v", err)
	}
	for _, record := range []localwal.Record{first, second, expired} {
		if err := localwal.Append(path, record); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	result, err := Replay([]string{path}, now)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if result.Summary.RecordsRead != 3 || result.Summary.RecordsAccepted != 2 || result.Summary.RecordsExpired != 1 || result.Summary.TemporaryActionsGC != 1 {
		t.Fatalf("unexpected summary: %+v", result.Summary)
	}
	if len(result.Incidents) != 1 || result.Incidents[0].RecordCount != 2 || result.Incidents[0].Subject != "edge-a" {
		t.Fatalf("unexpected incidents: %+v", result.Incidents)
	}
}

func TestReplayVerifiesWALSignerWhenSecretsConfigured(t *testing.T) {
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "signed.wal")
	record, err := localwal.NewRecord("edge-worker", "edge-a", "serve_lkg", map[string]string{"generation": "routegen_1"}, "routegen_1", ptr(now.Add(time.Minute)), now)
	if err != nil {
		t.Fatalf("new record: %v", err)
	}
	signed, err := localwal.SignRecord(record, "node:edge-a", []byte("secret"))
	if err != nil {
		t.Fatalf("sign record: %v", err)
	}
	if err := localwal.Append(path, signed); err != nil {
		t.Fatalf("append: %v", err)
	}
	accepted, err := ReplayWithSignerSecrets([]string{path}, now, map[string][]byte{"node:edge-a": []byte("secret")})
	if err != nil {
		t.Fatalf("replay accepted: %v", err)
	}
	if accepted.Summary.RecordsAccepted != 1 || accepted.Summary.RecordsRejected != 0 {
		t.Fatalf("expected signed record accepted, got %+v", accepted.Summary)
	}
	rejected, err := ReplayWithSignerSecrets([]string{path}, now, map[string][]byte{"node:edge-a": []byte("wrong")})
	if err != nil {
		t.Fatalf("replay rejected: %v", err)
	}
	if rejected.Summary.RecordsAccepted != 0 || rejected.Summary.RecordsRejected != 1 {
		t.Fatalf("expected signed record rejected, got %+v", rejected.Summary)
	}
}

func ptr(t time.Time) *time.Time {
	return &t
}

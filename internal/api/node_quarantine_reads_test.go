package api

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestActiveNodeQuarantinePreservesFreshnessExpiryAndAliases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := store.New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	expired := now.Add(-time.Hour)
	rows := []model.NodeDeepHealthResult{
		{NodeUpdaterID: "observed", ObservedOnly: true, QuarantineState: model.NodeQuarantineStateQuarantined, ReportedAt: now, UpdatedAt: now},
		{NodeUpdaterID: "clear", QuarantineState: model.NodeQuarantineStateClear, ReportedAt: now, UpdatedAt: now},
		{NodeUpdaterID: "expired", QuarantineState: model.NodeQuarantineStateQuarantined, QuarantineExpiresAt: &expired, ReportedAt: now, UpdatedAt: now},
		{NodeUpdaterID: "stale", ClusterNodeName: "node-stale", QuarantineState: model.NodeQuarantineStateClear, ReportedAt: now.Add(-time.Hour), UpdatedAt: now},
		{NodeUpdaterID: "future", QuarantineState: model.NodeQuarantineStateClear, ReportedAt: now.Add(time.Hour), UpdatedAt: now},
		{NodeUpdaterID: "enforced", ClusterNodeName: " node-a ", RuntimeID: "runtime-a", MachineID: "machine-a", QuarantineState: model.NodeQuarantineStateQuarantined, QuarantineReason: "fresh failure", ReportedAt: now, UpdatedAt: now},
	}
	persist := func() {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var state model.State
		if err = json.Unmarshal(raw, &state); err != nil {
			t.Fatal(err)
		}
		state.NodeDeepHealthResults = rows
		raw, err = json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	persist()
	server := &Server{store: s}
	got := server.activeNodeQuarantineByName()
	for _, key := range []string{"observed", "clear", "expired"} {
		if _, ok := got[key]; ok {
			t.Fatal("non-enforcing report quarantined", key)
		}
	}
	for _, key := range []string{"stale", "node-stale", "future"} {
		if got[key].QuarantineState != model.NodeQuarantineStateDegraded || got[key].QuarantineReason != nodeDeepHealthReportStaleReason {
			t.Fatal("freshness gate lost", key)
		}
	}
	for _, key := range []string{"enforced", "node-a", "runtime-a", "machine-a"} {
		if got[key].NodeUpdaterID != "enforced" || got[key].QuarantineReason != "fresh failure" {
			t.Fatal("enforcement alias lost", key)
		}
	}
	if len(got) != 7 {
		t.Fatal("unexpected quarantine membership", got)
	}
	rows[5].ObservedOnly = true
	rows[0].ObservedOnly = false
	persist()
	got = server.activeNodeQuarantineByName()
	if _, ok := got["enforced"]; ok {
		t.Fatal("released authority remained cached")
	}
	if got["observed"].NodeUpdaterID != "observed" {
		t.Fatal("newly enforced report was not read")
	}
	// A malformed store must still take the existing read-failure path.
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if got = server.activeNodeQuarantineByName(); len(got) != 0 {
		t.Fatal("failed inventory reused prior authority", got)
	}
}

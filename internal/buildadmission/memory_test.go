package buildadmission

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

type memoryCheckpoints map[string][]byte

func (m memoryCheckpoints) Read(_ context.Context, key string) ([]byte, error) {
	return m[key], nil
}

func (m memoryCheckpoints) Update(_ context.Context, key string, update func([]byte) ([]byte, error)) (bool, error) {
	data, err := update(m[key])
	if err == nil && data != nil {
		m[key] = data
	}
	return true, err
}

func TestBuildMemoryHistoryExpiresAndRemainsBounded(t *testing.T) {
	now := time.Now().UTC()
	profile := strings.Repeat("b", 64)
	raw, err := json.Marshal(buildMemoryLedger{Version: 1, Profiles: map[string][]buildMemorySample{
		profile: {{"expired", 1 << 30, now.Add(-31 * 24 * time.Hour)}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := decodeBuildMemoryLedger(raw, now)
	if err != nil || len(ledger.Profiles) != 0 {
		t.Fatal("expired history learned", ledger, err)
	}
	app := model.App{TenantID: "fixture-tenant", ID: "fixture"}
	db := memoryCheckpoints{memoryLedgerKey(app): raw}
	s := New(db, nil)
	for i := 0; i < 20; i++ {
		if err := s.RecordMemoryPeak(t.Context(), app, fmt.Sprintf("%064x", i), "uid", 1<<30); err != nil {
			t.Fatal(err)
		}
	}
	ledger, err = decodeBuildMemoryLedger(db[memoryLedgerKey(app)], time.Now().UTC())
	if err != nil || len(ledger.Profiles) != buildMemoryProfileLimit {
		t.Fatal("unbounded ledger", len(ledger.Profiles), err)
	}
	for i := 0; i < 10; i++ {
		if err := s.RecordMemoryPeak(t.Context(), app, profile, fmt.Sprint(i), 1<<30); err != nil {
			t.Fatal(err)
		}
	}
	ledger, err = decodeBuildMemoryLedger(db[memoryLedgerKey(app)], time.Now().UTC())
	if err != nil || len(ledger.Profiles[profile]) != buildMemorySampleLimit {
		t.Fatal("unbounded samples", ledger, err)
	}
	if _, err := decodeBuildMemoryLedger([]byte("corrupt"), now); err == nil {
		t.Fatal("corrupt history accepted")
	}
}

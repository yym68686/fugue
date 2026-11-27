package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestBuildMemoryHistorySurvivesRestartAndConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "postgres-fixture")
	s := &Service{Store: newStoreTestDatabase(t, path)}
	if err := s.Store.Init(); err != nil {
		t.Fatal(err)
	}
	app := model.App{TenantID: "tenant-fixture", ID: "app-fixture"}
	source := model.AppSource{Type: "github_public", RepoURL: "https://github.com/example/service", CommitSHA: "old"}
	key := buildMemoryLedgerKey(app)
	scope := buildMemoryScope(source, model.AppSpec{})
	profile := strings.Repeat("a", 64)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			if err := s.recordBuildMemoryPeak(ctx, key, profile, fmt.Sprint(i), int64(i+1)<<20); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	s = &Service{Store: newStoreTestDatabase(t, path)}
	target, err := s.readBuildMemoryTarget(t.Context(), key, profile)
	if err != nil || target != 139<<20 {
		t.Fatal(target, err)
	}
	if err := s.recordBuildMemoryPeak(t.Context(), key, profile, "7", 1<<20); err != nil {
		t.Fatal(err)
	}
	raw, _ := s.Store.ReadObservationCheckpoint(t.Context(), key)
	ledger, err := decodeBuildMemoryLedger(raw, time.Now())
	if err != nil || len(ledger.Profiles[profile]) != 8 {
		t.Fatal("duplicate UID recorded", ledger, err)
	}
	source.CommitSHA = "new"
	source.RepoAuthToken = "never-persist"
	if buildMemoryScope(source, model.AppSpec{}) != scope {
		t.Fatal("ordinary release lost history")
	}
	for _, other := range []model.App{{TenantID: "other", ID: app.ID}, {TenantID: app.TenantID, ID: "other"}} {
		if buildMemoryLedgerKey(other) == key {
			t.Fatal("tenant/app history shared")
		}
	}
	if buildMemoryScope(source, model.AppSpec{Env: map[string]string{"MODE": "changed"}}) == scope {
		t.Fatal("config change retained history")
	}
	if strings.Contains(string(raw), "never-persist") {
		t.Fatal("credentials persisted")
	}
}

func TestBuildMemoryHistoryExpiresAndRemainsBounded(t *testing.T) {
	now := time.Now().UTC()
	profile := strings.Repeat("b", 64)
	raw, _ := json.Marshal(buildMemoryLedger{Version: 1, Profiles: map[string][]buildMemorySample{profile: {{"expired", 1 << 30, now.Add(-31 * 24 * time.Hour)}}}})
	ledger, err := decodeBuildMemoryLedger(raw, now)
	if err != nil || len(ledger.Profiles) != 0 {
		t.Fatal("expired history learned", ledger, err)
	}
	s := &Service{Store: newStoreTestDatabase(t, "")}
	if err := s.Store.Init(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := s.recordBuildMemoryPeak(t.Context(), "fixture", fmt.Sprintf("%064x", i), "uid", 1<<30); err != nil {
			t.Fatal(err)
		}
	}
	raw, _ = s.Store.ReadObservationCheckpoint(t.Context(), "fixture")
	ledger, err = decodeBuildMemoryLedger(raw, now)
	if err != nil || len(ledger.Profiles) != buildMemoryProfileLimit {
		t.Fatal("unbounded ledger", len(ledger.Profiles), err)
	}
	for i := 0; i < 10; i++ {
		if err := s.recordBuildMemoryPeak(t.Context(), "fixture", profile, fmt.Sprint(i), 1<<30); err != nil {
			t.Fatal(err)
		}
	}
	raw, _ = s.Store.ReadObservationCheckpoint(t.Context(), "fixture")
	ledger, err = decodeBuildMemoryLedger(raw, time.Now())
	if err != nil || len(ledger.Profiles[profile]) != buildMemorySampleLimit {
		t.Fatal("unbounded samples", ledger, err)
	}
	if _, err := decodeBuildMemoryLedger([]byte("corrupt"), now); err == nil {
		t.Fatal("corrupt history accepted")
	}
}

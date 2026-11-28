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

	"fugue/internal/buildadmission"
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
	key := "builder-memory/v1/" + app.TenantID + "/" + app.ID
	scope := buildadmission.MemoryScope(source, model.AppSpec{})
	profile := strings.Repeat("a", 64)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			if err := s.buildAdmission().RecordMemoryPeak(ctx, app, profile, fmt.Sprint(i), int64(i+1)<<20); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	s = &Service{Store: newStoreTestDatabase(t, path)}
	target, err := s.buildAdmission().MemoryTarget(t.Context(), app, profile)
	if err != nil || target != 139<<20 {
		t.Fatal(target, err)
	}
	if err := s.buildAdmission().RecordMemoryPeak(t.Context(), app, profile, "7", 1<<20); err != nil {
		t.Fatal(err)
	}
	raw, err := s.Store.ReadObservationCheckpoint(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	var ledger struct {
		Profiles map[string][]json.RawMessage
	}
	err = json.Unmarshal(raw, &ledger)
	if err != nil || len(ledger.Profiles[profile]) != 8 {
		t.Fatal("duplicate UID recorded", ledger, err)
	}
	source.CommitSHA = "new"
	source.RepoAuthToken = "never-persist"
	if buildadmission.MemoryScope(source, model.AppSpec{}) != scope {
		t.Fatal("ordinary release lost history")
	}
	for _, other := range []model.App{{TenantID: "other", ID: app.ID}, {TenantID: app.TenantID, ID: "other"}} {
		if target, err := s.buildAdmission().MemoryTarget(t.Context(), other, profile); target != 0 || err != nil {
			t.Fatalf("tenant/app history shared: %v %v", target, err)
		}
	}
	if buildadmission.MemoryScope(source, model.AppSpec{Env: map[string]string{"MODE": "changed"}}) == scope {
		t.Fatal("config change retained history")
	}
	if strings.Contains(string(raw), "never-persist") {
		t.Fatal("credentials persisted")
	}
}

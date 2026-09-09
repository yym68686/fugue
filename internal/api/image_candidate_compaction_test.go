package api

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestImageCandidateCompactionPreservesInterleavedHistory(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := &Server{registryPushBase: "registry.example", registryPullBase: "pull.example"}
	for seed := int64(0); seed < 100; seed++ {
		rng := rand.New(rand.NewSource(seed))
		sources := make([]*model.AppSource, 12)
		for i := range sources {
			sources[i] = &model.AppSource{Type: model.AppSourceTypeGitHubPublic, RepoURL: "https://github.com/example/app", ResolvedImageRef: fmt.Sprintf("registry.example/fugue-apps/app:v%d", i%3)}
			if i%2 == 0 {
				sources[i].CommitSHA = "abc"
			}
			if i%4 == 0 {
				sources[i].RepoBranch = "main"
			}
		}
		app := model.App{ID: "app", Name: "app", Source: sources[0], Spec: model.AppSpec{Image: "pull.example/fugue-apps/app:v0"}, UpdatedAt: now}
		ops := make([]model.Operation, 600)
		for i := range ops {
			observed := now.Add(time.Duration(rng.Intn(30)) * time.Second)
			op := model.Operation{ID: fmt.Sprint(i), AppID: app.ID, DesiredSource: sources[rng.Intn(len(sources))], CreatedAt: now.Add(time.Duration(i) * time.Millisecond), UpdatedAt: observed}
			if rng.Intn(3) != 0 {
				op.DesiredSpec = &model.AppSpec{Image: fmt.Sprintf("pull.example/fugue-apps/app:v%d", rng.Intn(3))}
			}
			if rng.Intn(3) == 0 {
				op.CompletedAt = &observed
			} else if rng.Intn(3) == 0 {
				op.StartedAt = &observed
			}
			if rng.Intn(15) == 0 {
				op.DesiredSource = nil
			}
			ops[i] = op
		}
		compact := store.CompactImageCandidateOperations(ops)
		want := s.collectAppImageCandidates(app, ops)
		got := s.collectAppImageCandidates(app, compact)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("seed %d changed candidates after compaction", seed)
		}
		if len(compact) >= len(ops)/2 {
			t.Fatalf("seed %d did not compact repeated inputs: %d", seed, len(compact))
		}
	}
}

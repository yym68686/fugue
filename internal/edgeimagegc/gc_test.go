package edgeimagegc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type fakeRuntime struct {
	evidence []Evidence
	calls    int
	removed  []string
	err      error
}

func (f *fakeRuntime) Observe(context.Context) (Evidence, error) {
	if f.err != nil {
		return Evidence{}, f.err
	}
	i := f.calls
	if i >= len(f.evidence) {
		i = len(f.evidence) - 1
	}
	f.calls++
	return f.evidence[i], nil
}
func (f *fakeRuntime) Remove(_ context.Context, id string) error {
	f.removed = append(f.removed, id)
	return nil
}
func testImage(char string) Image {
	return Image{ID: "sha256:" + strings.Repeat(char, 64), RepoDigests: []string{"registry.example/system@sha256:" + strings.Repeat(char, 64)}}
}
func testPolicy() Policy {
	return Policy{Repositories: []string{"registry.example/system"}, MinimumUnusedAge: 24 * time.Hour, MaximumObservationGap: 2 * time.Hour, MaximumDeletes: 4, Apply: true}
}
func matureState(now time.Time, images ...Image) State {
	s := State{Version: 1, LastObservedAt: now.Add(-time.Hour), UnusedSince: map[string]time.Time{}}
	for _, im := range images {
		s.UnusedSince[im.ID] = now.Add(-25 * time.Hour)
	}
	return s
}

func TestSweepProtectsContainersRollbackAliasesAndPinned(t *testing.T) {
	images := []Image{testImage("a"), testImage("b"), testImage("c"), testImage("d"), testImage("e")}
	images[2].Pinned = true
	images[3].RepoDigests = append(images[3].RepoDigests, "another.example/tenant@"+images[3].ID)
	e := Evidence{Images: images, Protected: map[string]bool{}}
	if err := ProtectJSON([]byte(fmt.Sprintf(`{"containers":[{"imageRef":%q}],"data":{"record.json":%q}}`, images[0].ID, `{"rollback":{"image":"`+images[1].RepoDigests[0]+`"}}`)), e.Protected); err != nil {
		t.Fatal(err)
	}
	f := &fakeRuntime{evidence: []Evidence{e}}
	now := time.Now()
	state := matureState(now, images...)
	result, err := Sweep(context.Background(), f, testPolicy(), &state, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Protected != 4 || len(f.removed) != 1 || f.removed[0] != images[4].ID {
		t.Fatalf("unsafe plan: %+v removed=%v", result, f.removed)
	}
}
func TestSweepRechecksReferencesAndFailsClosed(t *testing.T) {
	im := testImage("a")
	e := Evidence{Images: []Image{im}, Protected: map[string]bool{}}
	fresh := Evidence{Images: e.Images, Protected: map[string]bool{im.ID: true}}
	f := &fakeRuntime{evidence: []Evidence{e, fresh}}
	now := time.Now()
	state := matureState(now, im)
	result, err := Sweep(context.Background(), f, testPolicy(), &state, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.removed) != 0 || len(result.Deferred) != 1 {
		t.Fatal("newly protected image deleted")
	}
	f.err = errors.New("API unavailable")
	if _, err := Sweep(context.Background(), f, testPolicy(), &state, now); err == nil || len(f.removed) != 0 {
		t.Fatal("failed-open inventory")
	}
}
func TestSweepAgeGapDryRunAndBatchBound(t *testing.T) {
	now := time.Now()
	images := []Image{testImage("a"), testImage("b"), testImage("c"), testImage("d"), testImage("e")}
	f := &fakeRuntime{evidence: []Evidence{{Images: images, Protected: map[string]bool{}}}}
	policy := testPolicy()
	state := State{}
	result, err := Sweep(context.Background(), f, policy, &state, now)
	if err != nil || result.Waiting != 5 || len(f.removed) != 0 {
		t.Fatalf("new images not aged: %+v %v", result, err)
	}
	state = matureState(now, images...)
	state.LastObservedAt = now.Add(-3 * time.Hour)
	result, err = Sweep(context.Background(), f, policy, &state, now)
	if err != nil || result.Waiting != 5 {
		t.Fatal("unobserved time counted")
	}
	state = matureState(now, images...)
	policy.Apply = false
	result, err = Sweep(context.Background(), f, policy, &state, now)
	if err != nil || len(result.Candidates) != 5 || len(f.removed) != 0 {
		t.Fatal("dry run mutated runtime")
	}
	policy.Apply = true
	result, err = Sweep(context.Background(), f, policy, &state, now)
	if err != nil || len(f.removed) != 4 {
		t.Fatal("batch limit violated")
	}
}
func TestGCStateRoundTripAndCorruption(t *testing.T) {
	p := t.TempDir() + "/state.json"
	now := time.Now().UTC()
	s := matureState(now, testImage("a"))
	if err := SaveState(p, s); err != nil {
		t.Fatal(err)
	}
	got, err := LoadState(p)
	if err != nil || len(got.UnusedSince) != 1 || !got.LastObservedAt.Equal(s.LastObservedAt) {
		t.Fatalf("lost state: %+v %v", got, err)
	}
}

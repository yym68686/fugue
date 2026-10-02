// Package edgeimagegc reclaims only explicitly allowed node-local image caches.
// It never removes containerd content/snapshot directories or registry copies.
package edgeimagegc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type Image struct {
	ID          string   `json:"id"`
	RepoTags    []string `json:"repoTags"`
	RepoDigests []string `json:"repoDigests"`
	Pinned      bool     `json:"pinned"`
}

type Evidence struct {
	Images    []Image
	Protected map[string]bool
}
type Runtime interface {
	Observe(context.Context) (Evidence, error)
	Remove(context.Context, string) error
}

type Policy struct {
	Repositories          []string
	MinimumUnusedAge      time.Duration
	MaximumObservationGap time.Duration
	MaximumDeletes        int
	Apply                 bool
}

type State struct {
	LastObservedAt time.Time            `json:"last_observed_at"`
	Version        int                  `json:"version"`
	UnusedSince    map[string]time.Time `json:"unused_since"`
}
type Result struct {
	ObservedAt time.Time `json:"observed_at"`
	Inventory  int       `json:"inventory"`
	Protected  int       `json:"protected"`
	Waiting    int       `json:"waiting"`
	Candidates []string  `json:"candidates"`
	Deleted    []string  `json:"deleted"`
	Deferred   []string  `json:"deferred"`
	Failures   []string  `json:"failures"`
	Apply      bool      `json:"apply"`
}

var digestPattern = regexp.MustCompile(`sha256:[a-f0-9]{64}`)

// Include digests from nested JSON strings in immutable release records as well
// as ordinary PodSpecs. All retained history is protected, not only live Pods.
func ProtectJSON(raw []byte, protected map[string]bool) error {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	var visit func(any, int)
	visit = func(v any, depth int) {
		if depth > 32 {
			return
		}
		switch x := v.(type) {
		case string:
			if len(x) <= 512 && !strings.ContainsAny(x, " \t\r\n{}") {
				protected[x] = true
			}
			for _, d := range digestPattern.FindAllString(x, -1) {
				protected[d] = true
			}
			if len(x) > 0 && len(x) < 1<<20 && (x[0] == '{' || x[0] == '[') {
				var nested any
				if json.Unmarshal([]byte(x), &nested) == nil {
					visit(nested, depth+1)
				}
			}
		case map[string]any:
			for _, child := range x {
				visit(child, depth+1)
			}
		case []any:
			for _, child := range x {
				visit(child, depth+1)
			}
		}
	}
	visit(value, 0)
	return nil
}

func eligible(image Image, evidence Evidence, policy Policy) bool {
	if image.Pinned || len(image.ID) != 71 || digestPattern.FindString(image.ID) != image.ID || len(image.RepoDigests) == 0 {
		return false
	}
	refs := append(append([]string{image.ID}, image.RepoTags...), image.RepoDigests...)
	allowed := map[string]bool{}
	for _, repo := range policy.Repositories {
		allowed[repo] = true
	}
	for _, ref := range refs {
		if evidence.Protected[ref] {
			return false
		}
		for _, d := range digestPattern.FindAllString(ref, -1) {
			if evidence.Protected[d] {
				return false
			}
		}
	}
	// One shared image ID can have multiple repository aliases. An alias outside
	// the configured ownership boundary prevents deletion of the entire image.
	for _, ref := range append(append([]string{}, image.RepoTags...), image.RepoDigests...) {
		repo := strings.SplitN(ref, "@", 2)[0]
		if colon := strings.LastIndex(repo, ":"); colon > strings.LastIndex(repo, "/") {
			repo = repo[:colon]
		}
		if !allowed[repo] {
			return false
		}
	}
	return true
}

func Sweep(ctx context.Context, runtime Runtime, policy Policy, state *State, now time.Time) (Result, error) {
	result := Result{ObservedAt: now, Apply: policy.Apply}
	if policy.MinimumUnusedAge < time.Hour || policy.MaximumObservationGap < time.Minute || policy.MaximumDeletes < 1 || policy.MaximumDeletes > 32 || len(policy.Repositories) == 0 || state == nil {
		return result, errors.New("invalid image GC policy")
	}
	evidence, err := runtime.Observe(ctx)
	if err != nil {
		return result, fmt.Errorf("inventory incomplete; no deletion: %w", err)
	}
	if state.LastObservedAt.IsZero() || now.Sub(state.LastObservedAt) > policy.MaximumObservationGap || now.Before(state.LastObservedAt) {
		state.UnusedSince = nil
	}
	state.LastObservedAt = now
	if state.UnusedSince == nil {
		state.UnusedSince = map[string]time.Time{}
	}
	state.Version = 1
	result.Inventory = len(evidence.Images)
	next := map[string]time.Time{}
	for _, image := range evidence.Images {
		if !eligible(image, evidence, policy) {
			result.Protected++
			continue
		}
		since, ok := state.UnusedSince[image.ID]
		if !ok || since.After(now) {
			since = now
		}
		next[image.ID] = since
		if now.Sub(since) < policy.MinimumUnusedAge {
			result.Waiting++
			continue
		}
		result.Candidates = append(result.Candidates, image.ID)
	}
	state.UnusedSince = next
	sort.Strings(result.Candidates)
	if !policy.Apply {
		return result, nil
	}
	for _, id := range result.Candidates {
		if len(result.Deleted)+len(result.Failures) >= policy.MaximumDeletes {
			break
		}
		// Re-read runtime references, workload history and release pins immediately
		// before each deletion. Any failed read stops the batch.
		fresh, err := runtime.Observe(ctx)
		if err != nil {
			return result, fmt.Errorf("pre-delete evidence unavailable: %w", err)
		}
		safe := false
		for _, im := range fresh.Images {
			if im.ID == id {
				safe = eligible(im, fresh, policy)
				break
			}
		}
		if !safe {
			delete(state.UnusedSince, id)
			result.Deferred = append(result.Deferred, id)
			continue
		}
		if err := runtime.Remove(ctx, id); err != nil {
			result.Failures = append(result.Failures, fmt.Sprintf("%s: %v", id, err))
			continue
		}
		result.Deleted = append(result.Deleted, id)
		delete(state.UnusedSince, id)
	}
	return result, nil
}

func LoadState(path string) (State, error) {
	state := State{Version: 1, UnusedSince: map[string]time.Time{}}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return State{}, err
	}
	if state.Version != 1 {
		return State{}, errors.New("unsupported image GC state version")
	}
	return state, nil
}

func SaveState(path string, state State) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".image-gc-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

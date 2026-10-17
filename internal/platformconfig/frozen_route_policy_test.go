package platformconfig

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"fugue/internal/model"
)

func frozenRouteFixture() (model.PlatformArtifact, PolicySnapshot) {
	return model.PlatformArtifact{Generation: "route-generation", Content: map[string]any{
		"schema_version": SchemaVersion,
		"routes":         []any{map[string]any{"hostname": "app.example.test", "enabled": true, "upstream_url": "http://origin:8080"}},
	}}, NormalizePolicySnapshot(PolicySnapshot{Generation: "policy"})
}

func TestFrozenRoutePolicyDecodesOnceAcrossConcurrentHostnames(t *testing.T) {
	artifact, policy := frozenRouteFixture()
	var calls atomic.Int32
	project := newFrozenPolicyRouteProjector(policy, func(raw []byte) (routeProjectionPolicy, error) {
		calls.Add(1)
		return decodeRouteProjectionPolicy(raw)
	})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := project(artifact)
			if err != nil || len(out.Routes) != 1 {
				t.Error("valid route rejected", err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("complete policy decoded %d times, want 1", calls.Load())
	}
}

func TestFrozenRoutePolicyMatchesOrdinaryValidationAndErrorOrder(t *testing.T) {
	for _, mode := range []string{"valid", "null-policy", "bad-policy", "bad-schema", "missing-routes", "bad-route-field", "bad-cache", "bad-tls", "invalid-policy-and-route", "alias-policy", "marshal-error"} {
		t.Run(mode, func(t *testing.T) {
			artifact, typed := frozenRouteFixture()
			var policy any = typed
			switch mode {
			case "null-policy":
				policy = nil
			case "bad-policy":
				typed.CanaryWeights = []int{101}
				policy = typed
			case "bad-schema":
				artifact.Content["schema_version"] = "unsupported"
			case "missing-routes":
				delete(artifact.Content, "routes")
			case "bad-route-field":
				artifact.Content["routes"].([]any)[0].(map[string]any)["unknown"] = true
			case "bad-cache":
				artifact.Content["cache_policies"] = "invalid"
			case "bad-tls":
				artifact.Content["tls_allowlist"] = "invalid"
			case "invalid-policy-and-route":
				policy = map[string]any{"generation": "policy", "canary_weights": []int{101}}
				artifact.Content["routes"] = "invalid"
			case "alias-policy":
				policy = map[string]any{"generation": "policy", "minimum_healthy_edges": 2, "MinimumHealthyEdges": 3}
			case "marshal-error":
				policy = make(chan int)
			}
			ordinary := artifact
			ordinary.Content = map[string]any{}
			for k, v := range artifact.Content {
				ordinary.Content[k] = v
			}
			ordinary.Content["policy"] = policy
			want, wantErr := ProjectRouteArtifact(ordinary)
			project := NewFrozenPolicyRouteProjector(policy)
			for range 2 {
				got, err := project(artifact)
				if !reflect.DeepEqual(got, want) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
					t.Fatal("frozen policy changed validation", got, err, want, wantErr)
				}
			}
		})
	}
}

func TestFrozenRoutePolicyOwnsSnapshotAndRejectsMixedPolicy(t *testing.T) {
	artifact, policy := frozenRouteFixture()
	policy.MinimumHealthyEdges = 2
	policy.CanaryWeights = []int{10}
	project := NewFrozenPolicyRouteProjector(policy)
	policy.CanaryWeights[0] = 101
	policy.MinimumHealthyEdges = 3
	first, err := project(artifact)
	if err != nil || first.Routes[0].MinHealthyEdgeNodes != 2 {
		t.Fatal("original policy was not detached", first, err)
	}
	if _, err := NewFrozenPolicyRouteProjector(policy)(artifact); err == nil {
		t.Fatal("new invalid policy reused prior validation")
	}
	first.Routes[0].Hostname = "changed.example.test"
	second, err := project(artifact)
	if err != nil || second.Routes[0].Hostname != "app.example.test" {
		t.Fatal("returned route aliases subsequent result", second, err)
	}
	artifact.Content["policy"] = nil
	if _, err := project(artifact); err == nil {
		t.Fatal("artifact policy silently overridden")
	}
}

func TestFrozenRoutePolicySnapshotsCustomJSONOnce(t *testing.T) {
	artifact, policy := frozenRouteFixture()
	raw, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	project := NewFrozenPolicyRouteProjector(json.RawMessage(raw))
	for i := range raw {
		raw[i] = ' '
	}
	if _, err := project(artifact); err != nil {
		t.Fatal("mutable raw input changed captured policy", err)
	}
}

type policyChangingRouteJSON struct {
	content map[string]any
	policy  any
	remove  bool
	raw     []byte
}

func (r policyChangingRouteJSON) MarshalJSON() ([]byte, error) {
	if r.remove {
		delete(r.content, "policy")
	} else {
		r.content["policy"] = r.policy
	}
	return r.raw, nil
}

func TestOrdinaryRouteProjectionPreservesPolicyLookupAfterRouteEncoding(t *testing.T) {
	for _, mode := range []string{"replace", "add", "remove"} {
		t.Run(mode, func(t *testing.T) {
			artifact, policy := frozenRouteFixture()
			raw, err := json.Marshal(artifact.Content["routes"])
			if err != nil {
				t.Fatal(err)
			}
			invalid := policy
			invalid.CanaryWeights = []int{101}
			if mode == "replace" {
				artifact.Content["policy"] = policy
			} else if mode == "remove" {
				artifact.Content["policy"] = invalid
			}
			artifact.Content["routes"] = policyChangingRouteJSON{content: artifact.Content, policy: invalid, remove: mode == "remove", raw: raw}
			_, err = ProjectRouteArtifact(artifact)
			if (err == nil) != (mode == "remove") {
				t.Fatal("ordinary entry point read policy before route encoding", mode, err)
			}
		})
	}
}

func TestFrozenRoutePolicyRejectsPolicyAddedDuringRouteEncoding(t *testing.T) {
	artifact, policy := frozenRouteFixture()
	raw, err := json.Marshal(artifact.Content["routes"])
	if err != nil {
		t.Fatal(err)
	}
	artifact.Content["routes"] = policyChangingRouteJSON{content: artifact.Content, policy: policy, raw: raw}
	if _, err := NewFrozenPolicyRouteProjector(policy)(artifact); err == nil {
		t.Fatal("custom route encoder introduced a second policy")
	}
}

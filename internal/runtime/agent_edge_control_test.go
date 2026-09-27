package runtime

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/agentedge"
	"fugue/internal/config"
)

type agentEdgeCountingTransport struct{ calls int }

func (c *agentEdgeCountingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.calls++
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"ok":true}`)), Request: r}, nil
}

func TestActivatedAgentDoesNotSendControlRequestsThroughLegacyClientAfterRestart(t *testing.T) {
	now := time.Now().UTC()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	publication := agentedge.Publication{ServingGroupID: "edge-group-a", ReleaseSetID: "set-a", ReleaseSetDigest: digest, RouteArtifactID: "route-a", RouteArtifactDigest: digest, PolicyDigest: digest, IntentDigest: digest, InputSnapshotDigest: digest, TopologyDigest: digest, ScopeKey: "global", ReleaseID: "release-a", Channel: "full", FencingToken: 1, PublishedAt: now.Add(-time.Minute)}
	g := agentedge.Grant{Schema: agentedge.GrantSchema, Purpose: agentedge.GrantPurpose, Audience: "runtime-test", Origin: "https://api.example.test", Mode: "active", PolicyReference: agentedge.PolicyReference{ArtifactID: "policy-a", ArtifactDigest: digest, ReleaseID: "policy-release-a", Channel: "full", FencingToken: 1, PublishedAt: now.Add(-time.Minute)},
		Policy: agentedge.Policy{ProbeIntervalSeconds: 10, ProbeTimeoutMilliseconds: 500, FactMaxAgeSeconds: 120, FailureThreshold: 3, BetterSampleThreshold: 3, SwitchImprovementPercent: 15, SwitchCooldownSeconds: 60, StandbyCount: 1, DesiredDistinctCells: 2, MaxCandidates: 8}, MinimumCandidates: 1, MinDistinctCells: 1, MinDistinctDomains: map[string]int{"host": 1}, IssuedAt: now.Add(-time.Second), ValidUntil: now.Add(30 * time.Second),
		Candidates: []agentedge.Candidate{{Publication: publication, EdgeID: "edge-a", AuthorityCellID: "cell-a", Address: "8.8.8.8", RouteDigests: []string{digest}, FailureDomains: map[string]string{"host": "host-a"}, EvidenceDigest: digest, EvidenceObservedAt: now.Add(-time.Second), EvidenceValidUntil: now.Add(time.Minute)}}}
	signed, err := agentedge.Sign(g, "key-a", private)
	if err != nil {
		t.Fatal(err)
	}
	ring := agentedge.TrustKeyring{Schema: agentedge.TrustKeyringSchema, Generation: 1, Keys: []agentedge.PublicKeyConfig{{KeyID: "key-a", PublicKey: base64.RawURLEncoding.EncodeToString(public), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}}}
	dir := t.TempDir()
	trustPath := filepath.Join(dir, "trust.json")
	checkpoint := filepath.Join(dir, "checkpoint.json")
	for path, value := range map[string]any{trustPath: ring, checkpoint: map[string]any{"schema": "fugue.agent-edge-checkpoint/v1", "audience": g.Audience, "origin": g.Origin, "activated": true, "trust": ring, "last_grant": signed, "cell_watermarks": map[string]any{"cell-a": publication}}} {
		raw, _ := json.Marshal(value)
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	control, err := agentedge.NewControl(agentedge.ControlOptions{Audience: g.Audience, Origin: g.Origin, RuntimeKey: "runtime-token", TrustFile: trustPath, CheckpointFile: checkpoint})
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	legacy := &agentEdgeCountingTransport{}
	s := NewAgentService(config.AgentConfig{ServerURL: g.Origin, RuntimeID: g.Audience, RuntimeKey: "runtime-token"}, nil)
	s.HTTPClient = &http.Client{Transport: legacy}
	s.edgeControl = control
	for _, request := range []struct{ method, path string }{{http.MethodPost, "/v1/agent/heartbeat"}, {http.MethodGet, "/v1/agent/operations"}, {http.MethodPost, "/v1/agent/operations/operation-a/complete"}} {
		if _, err = s.doJSONRequest(context.Background(), request.method, request.path, "runtime-token", []byte(`{}`)); err == nil {
			t.Fatal("restart without fresh measurements sent a control request", request.path)
		}
	}
	if legacy.calls != 0 {
		t.Fatal("Agent bypassed selected transport through its legacy client")
	}
}

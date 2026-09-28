package agentedge

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

func controlFixture(t *testing.T) (*Control, Grant, ed25519.PrivateKey, TrustKeyring, *time.Time) {
	t.Helper()
	g, private, keys, now := grantFixture(t)
	delta := time.Now().UTC().Sub(now)
	now = now.Add(delta)
	g.IssuedAt = g.IssuedAt.Add(delta)
	g.ValidUntil = g.ValidUntil.Add(delta)
	g.PolicyReference.PublishedAt = g.PolicyReference.PublishedAt.Add(delta)
	for i := range g.Candidates {
		g.Candidates[i].Publication.PublishedAt = g.Candidates[i].Publication.PublishedAt.Add(delta)
		g.Candidates[i].EvidenceObservedAt = g.Candidates[i].EvidenceObservedAt.Add(delta)
		g.Candidates[i].EvidenceValidUntil = g.Candidates[i].EvidenceValidUntil.Add(delta)
	}
	g.MinDistinctCells = 1
	g.MinDistinctDomains = map[string]int{"host": 1}
	g.Mode = "shadow"
	anchor := keys["key-one"]
	anchor.NotBefore = anchor.NotBefore.Add(delta)
	anchor.NotAfter = anchor.NotAfter.Add(delta)
	ring := TrustKeyring{Schema: TrustKeyringSchema, Generation: 1, Keys: []PublicKeyConfig{{KeyID: "key-one", PublicKey: base64.RawURLEncoding.EncodeToString(anchor.PublicKey), NotBefore: anchor.NotBefore, NotAfter: anchor.NotAfter}}}
	dir := t.TempDir()
	trustPath := filepath.Join(dir, "trust.json")
	writeControlTrust(t, trustPath, ring)
	m, err := NewControl(ControlOptions{Audience: g.Audience, Origin: g.Origin, RuntimeKey: "synthetic-runtime-token", TrustFile: trustPath, CheckpointFile: filepath.Join(dir, "checkpoint.json")})
	if err != nil {
		t.Fatal(err)
	}
	m.now = func() time.Time { return now }
	m.fetch = func(context.Context, string) ([]byte, error) { return encodeGrant(t, g, private), nil }
	m.probe = func(_ context.Context, _ Grant, c Candidate) Measurement {
		return Measurement{EdgeID: c.EdgeID, Address: c.Address, Success: true, Latency: time.Millisecond}
	}
	t.Cleanup(m.Close)
	return m, g, private, ring, &now
}

func writeControlTrust(t *testing.T, path string, ring TrustKeyring) {
	t.Helper()
	raw, err := json.Marshal(ring)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func activateControl(t *testing.T, m *Control, g Grant, private ed25519.PrivateKey, now *time.Time) Grant {
	t.Helper()
	*now = now.Add(10 * time.Second)
	g.IssuedAt = *now
	g.Mode = "active"
	g.PolicyReference.ArtifactID = "policy-active"
	g.PolicyReference.ReleaseID = "policy-release-active"
	g.PolicyReference.PublishedAt = *now
	g.PolicyReference.FencingToken++
	m.fetch = func(context.Context, string) ([]byte, error) { return encodeGrant(t, g, private), nil }
	m.refreshAt = time.Time{}
	if err := m.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	return g
}

func TestControlShadowActivationRestartAndExpiredGrantNeverFallback(t *testing.T) {
	m, g, private, _, now := controlFixture(t)
	legacy := &http.Client{}
	if err := m.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.HTTPClient(legacy) != legacy || m.Status().Activated || m.Status().Primary == "" {
		t.Fatal("shadow mode changed request transport or failed to measure")
	}
	g = activateControl(t, m, g, private, now)
	if m.HTTPClient(legacy) == legacy || !m.Status().Activated || m.Status().Primary == "" {
		t.Fatal("active permission failed to select measured transport")
	}
	restarted, err := NewControl(m.options)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.now = func() time.Time { return *now }
	if restarted.HTTPClient(legacy) == legacy || restarted.Status().Primary != "" {
		t.Fatal("restart erased activation or revived old measurements")
	}
	restarted.fetch = func(context.Context, string) ([]byte, error) { return nil, errors.New("offline") }
	restarted.probe = m.probe
	if err = restarted.Step(context.Background()); err == nil || restarted.Status().Primary == "" {
		t.Fatal("original unexpired LKG did not permit fresh measurements during acquisition outage", err)
	}
	*now = g.ValidUntil
	if err = restarted.Step(context.Background()); err == nil || restarted.Status().Primary != "" || restarted.HTTPClient(legacy) == legacy {
		t.Fatal("expired cached grant regained native authority", err)
	}
}

func TestControlRevocationAndTrustRollbackSurviveRestart(t *testing.T) {
	m, g, private, ring, now := controlFixture(t)
	if err := m.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	activateControl(t, m, g, private, now)
	old := ring
	ring.Keys = append([]PublicKeyConfig(nil), ring.Keys...)
	ring.Generation = 2
	ring.Keys[0].Revoked = true
	writeControlTrust(t, m.options.TrustFile, ring)
	if err := m.Step(context.Background()); err == nil || m.Status().Primary != "" || m.Status().TrustGeneration != 2 {
		t.Fatal("revocation retained selected request authority", err)
	}
	writeControlTrust(t, m.options.TrustFile, old)
	restarted, err := NewControl(m.options)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.now = func() time.Time { return *now }
	restarted.fetch = m.fetch
	restarted.probe = m.probe
	if err = restarted.Step(context.Background()); err == nil || restarted.Status().TrustGeneration != 2 || restarted.Status().Primary != "" {
		t.Fatal("stale projected trust undid persisted revocation", err)
	}
	if err = os.WriteFile(m.options.TrustFile, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = restarted.Step(context.Background()); err == nil || restarted.Status().TrustGeneration != 2 {
		t.Fatal("corrupt trust displaced positive configuration LKG", err)
	}
}

func TestControlPersistsAbsentCellWatermarksAndRejectsCorruptCheckpoint(t *testing.T) {
	m, g, private, _, now := controlFixture(t)
	if err := m.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(10 * time.Second)
	g.IssuedAt = *now
	g.Candidates[0].Publication.ReleaseID = "cell-a-new"
	g.Candidates[0].Publication.PublishedAt = *now
	g.Candidates[0].Publication.FencingToken++
	if err := m.acceptGrant(encodeGrant(t, g, private)); err != nil {
		t.Fatal(err)
	}
	previousCell := g.Candidates[0]
	*now = now.Add(time.Second)
	g.IssuedAt = *now
	g.Candidates = g.Candidates[1:]
	if err := m.acceptGrant(encodeGrant(t, g, private)); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewControl(m.options)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	restarted.now = func() time.Time { return *now }
	*now = now.Add(time.Second)
	g.IssuedAt = *now
	previousCell.Publication.PublishedAt = previousCell.Publication.PublishedAt.Add(-time.Minute)
	previousCell.Publication.FencingToken--
	previousCell.Publication.ReleaseID = "old-cell-a"
	g.Candidates = append([]Candidate{previousCell}, g.Candidates...)
	if err = restarted.acceptGrant(encodeGrant(t, g, private)); err == nil {
		t.Fatal("absent cell replay became valid after restart")
	}
	if err = os.WriteFile(m.options.CheckpointFile, []byte(`{"activated":false}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = NewControl(m.options); err == nil {
		t.Fatal("corrupt checkpoint silently reset to native requests")
	}
}

func TestControlDoesNotActivateBeforeDurableCheckpoint(t *testing.T) {
	m, g, private, _, now := controlFixture(t)
	if err := m.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	g.Mode = "active"
	g.IssuedAt = now.Add(time.Second)
	*now = g.IssuedAt
	g.PolicyReference.PublishedAt = *now
	g.PolicyReference.FencingToken++
	g.PolicyReference.ReleaseID = "active-policy"
	m.options.CheckpointFile = filepath.Join(t.TempDir(), "missing", "checkpoint.json")
	if err := m.acceptGrant(encodeGrant(t, g, private)); err == nil || m.Status().Activated {
		t.Fatal("permission activated without durable replay protection", err)
	}
}

func TestAuthenticatedLocalDisqualificationSkipsTransientFailureThreshold(t *testing.T) {
	s, g, _, keys, now, digest := selectorFixture(t, false)
	if _, err := s.Observe(measurementRound(g, digest, now, time.Millisecond, 2*time.Millisecond), keys, now); err != nil {
		t.Fatal(err)
	}
	at := now.Add(10 * time.Second)
	round := measurementRound(g, digest, at, 0, 2*time.Millisecond)
	round.Measurements[0].Disqualified = true
	choice, err := s.Observe(round, keys, at)
	if err != nil || choice.Primary.EdgeID != "edge-b" || !choice.Degraded {
		t.Fatal("authenticated negative evidence retained old Edge for hysteresis", choice, err)
	}
}

func TestControlCheckpointNeverStoresRuntimeCredentials(t *testing.T) {
	m, _, _, _, _ := controlFixture(t)
	if err := m.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(m.options.CheckpointFile)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), m.options.RuntimeKey) || strings.Contains(string(raw), "private_key") {
		t.Fatal("selection checkpoint persisted secret authority")
	}
}

func TestControlMissingInitializedCheckpointFailsClosed(t *testing.T) {
	m, g, private, _, now := controlFixture(t)
	if err := m.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	activateControl(t, m, g, private, now)
	if err := os.Remove(m.options.CheckpointFile); err != nil {
		t.Fatal(err)
	}
	if _, err := NewControl(m.options); err == nil {
		t.Fatal("removed checkpoint reset an activated Agent to native requests")
	}
}

func TestControlStagesRenewalWithoutExposingAnUnmeasuredPermission(t *testing.T) {
	m, g, private, _, now := controlFixture(t)
	if err := m.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	g = activateControl(t, m, g, private, now)
	old, err := m.selector.Current(m.currentKeys(), *now)
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(11 * time.Second)
	next := cloneGrant(g)
	next.IssuedAt = *now
	for i := range next.Candidates {
		next.Candidates[i].RouteDigests = []string{"sha256:" + strings.Repeat("b", 64)}
		next.Candidates[i].Publication.ReleaseID = "next-release"
		next.Candidates[i].Publication.PublishedAt = *now
		next.Candidates[i].Publication.FencingToken++
	}
	raw := encodeGrant(t, next, private)
	m.fetch = func(context.Context, string) ([]byte, error) { return raw, nil }
	m.refreshAt = time.Time{}
	entered, release := make(chan struct{}, len(next.Candidates)), make(chan struct{})
	m.probe = func(_ context.Context, _ Grant, c Candidate) Measurement {
		entered <- struct{}{}
		<-release
		return Measurement{EdgeID: c.EdgeID, Address: c.Address, Success: true, Latency: time.Millisecond}
	}
	done := make(chan error, 1)
	go func() { done <- m.Step(context.Background()) }()
	<-entered
	during, err := m.selector.Current(m.currentKeys(), *now)
	if err != nil || during.GrantDigest != old.GrantDigest {
		close(release)
		<-done
		t.Fatal("probing replacement displaced positive measured permission", during, err)
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	after, err := m.selector.Current(m.currentKeys(), *now)
	if err != nil || after.GrantDigest == old.GrantDigest || after.Primary.EdgeID == "" {
		t.Fatal("atomic renewal did not publish fresh measurements with its grant", after, err)
	}
	checkpoint, _, err := readCheckpoint(m.options.CheckpointFile, g.Audience, g.Origin)
	if err != nil || checkpoint.LastGrant.Digest != after.GrantDigest {
		t.Fatal("measured activation outran durable grant", err)
	}
}

func TestFailedReplacementKeepsPositiveGrantOnlyToOriginalExpiry(t *testing.T) {
	m, g, private, _, now := controlFixture(t)
	if err := m.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	g = activateControl(t, m, g, private, now)
	old, err := m.selector.Current(m.currentKeys(), *now)
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	next := cloneGrant(g)
	next.IssuedAt = *now
	for i := range next.Candidates {
		next.Candidates[i].RouteDigests = []string{"sha256:" + strings.Repeat("b", 64)}
	}
	raw := encodeGrant(t, next, private)
	m.fetch = func(context.Context, string) ([]byte, error) { return raw, nil }
	m.refreshAt = time.Time{}
	m.probe = func(_ context.Context, _ Grant, c Candidate) Measurement {
		return Measurement{EdgeID: c.EdgeID, Address: c.Address}
	}
	if err = m.Step(context.Background()); err == nil {
		t.Fatal("failed replacement appeared successful")
	}
	current, err := m.selector.Current(m.currentKeys(), *now)
	if err != nil || current.GrantDigest != old.GrantDigest || current.ValidUntil != old.ValidUntil {
		t.Fatal("failed new probes revoked or renewed positive LKG", current, err)
	}
	*now = old.ValidUntil
	if _, err = m.selector.Current(m.currentKeys(), *now); err == nil {
		t.Fatal("failed replacement renewed old grant expiration")
	}
}

func TestControlAcquisitionPreservesOriginAuthAndDoesNotFollowRedirects(t *testing.T) {
	m, g, private, _, _ := controlFixture(t)
	raw := encodeGrant(t, g, private)
	var redirect atomic.Bool
	server, roots := edgeTLSServer(t, "api.example.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "api.example.test" || r.TLS.ServerName != "api.example.test" || r.Header.Get("Authorization") != "Bearer synthetic-runtime-token" || r.URL.Path != "/v1/agent/edge-candidates" || r.Header.Get("X-Fugue-Agent-Current-Edge") != "edge-a" {
			t.Error("acquisition identity or continuity hint changed")
		}
		if redirect.Load() {
			w.Header().Set("Location", "https://foreign.example.test/keyring")
			w.WriteHeader(307)
			return
		}
		w.Write(raw)
	}))
	transport := m.acquisition.Transport.(*http.Transport)
	transport.TLSClientConfig.RootCAs = roots
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "api.example.test:443" {
			return nil, errors.New("acquisition escaped origin")
		}
		return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(server.URL, "https://"))
	}
	got, err := m.fetchGrant(context.Background(), "edge-a")
	if err != nil || string(got) != string(raw) {
		t.Fatal("acquisition failed", err)
	}
	redirect.Store(true)
	if _, err = m.fetchGrant(context.Background(), "edge-a"); err == nil {
		t.Fatal("acquisition followed a redirect")
	}
}

func TestControlLocalProofRequiresExactAuthorityAndOriginalLease(t *testing.T) {
	m, g, _, _, now := controlFixture(t)
	c := g.Candidates[0]
	p := c.Publication
	base := routeprobe.Proof{Version: "proof-v1", Digest: c.RouteDigests[0], EdgeID: c.EdgeID, GroupID: p.ServingGroupID, CheckedAt: *now, ValidUntil: g.ValidUntil, TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: p.ReleaseSetID, ReleaseSetDigest: p.ReleaseSetDigest, RouteArtifactID: p.RouteArtifactID, RouteArtifactDigest: p.RouteArtifactDigest, PolicyDigest: p.PolicyDigest, IntentDigest: p.IntentDigest, InputSnapshotDigest: p.InputSnapshotDigest, ReleaseID: p.ReleaseID, ReleaseChannel: p.Channel, FencingToken: p.FencingToken, ScopeKey: p.ScopeKey}}
	for _, scenario := range []string{"valid", "timeout", "tls mismatch", "cell mismatch", "publication mismatch", "route mismatch", "negative state", "short lease", "stale observation"} {
		t.Run(scenario, func(t *testing.T) {
			m.routeProbe = func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
				if host != "api.example.test" || path != "/" || address != c.Address || state != "" {
					t.Error("wrong probe target")
				}
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > time.Duration(g.Policy.ProbeTimeoutMilliseconds)*time.Millisecond {
					t.Error("probe did not retain millisecond deadline")
				}
				proof := base
				binding := *base.TrafficRelease
				proof.TrafficRelease = &binding
				switch scenario {
				case "timeout":
					return routeprobe.Proof{}, routeprobe.ErrUnavailable
				case "tls mismatch":
					return routeprobe.Proof{}, errors.New("TLS verification failed")
				case "cell mismatch":
					proof.GroupID = "foreign"
				case "publication mismatch":
					proof.TrafficRelease.ReleaseID = "foreign"
				case "route mismatch":
					proof.Digest = "sha256:" + strings.Repeat("b", 64)
				case "negative state":
					proof.State = "disabled"
				case "short lease":
					proof.ValidUntil = g.ValidUntil.Add(-time.Second)
				case "stale observation":
					proof.CheckedAt = g.IssuedAt.Add(-time.Second)
				}
				return proof, nil
			}
			measurement := m.probeCandidate(context.Background(), g, c)
			if measurement.Success != (scenario == "valid") || measurement.Disqualified != (scenario != "valid" && scenario != "timeout") {
				t.Fatal("local proof classification unsafe", measurement)
			}
		})
	}
}

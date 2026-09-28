package agentedge

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"fugue/internal/routeprobe"
)

type ControlOptions struct {
	Audience       string
	Origin         string
	RuntimeKey     string
	TrustFile      string
	CheckpointFile string
}

// Control keeps artifact acquisition separate from control execution. Only
// acquisition uses ordinary hostname resolution after dynamic routing has
// activated; heartbeat, operation polling and completion never fall back to it.
type Control struct {
	stepMu      sync.Mutex
	mu          sync.RWMutex
	options     ControlOptions
	checkpoint  clientCheckpoint
	keys        map[string]TrustKey
	selector    *Selector
	selected    *http.Client
	acquisition *http.Client
	now         func() time.Time
	lastProbe   time.Time
	refreshAt   time.Time
	fetch       func(context.Context, string) ([]byte, error)
	probe       func(context.Context, Grant, Candidate) Measurement
	routeProbe  func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error)
}

type ControlStatus struct {
	Activated       bool      `json:"activated"`
	Mode            string    `json:"mode,omitempty"`
	Primary         string    `json:"primary,omitempty"`
	Cell            string    `json:"cell,omitempty"`
	Standbys        []string  `json:"standbys,omitempty"`
	Degraded        bool      `json:"degraded"`
	ValidUntil      time.Time `json:"valid_until,omitempty"`
	TrustGeneration uint64    `json:"trust_generation"`
	GrantDigest     string    `json:"grant_digest,omitempty"`
}

func NewControl(o ControlOptions) (*Control, error) {
	u, err := url.Parse(o.Origin)
	if err != nil || u.Scheme != "https" || o.Origin != "https://"+u.Hostname() || !hostnamePattern.MatchString(u.Hostname()) || !identifier.MatchString(o.Audience) || o.RuntimeKey == "" || !filepath.IsAbs(o.TrustFile) || !filepath.IsAbs(o.CheckpointFile) || o.TrustFile == o.CheckpointFile {
		return nil, errors.New("Agent Edge control requires a canonical HTTPS origin, runtime identity and independent absolute configuration paths")
	}
	c, found, err := readCheckpoint(o.CheckpointFile, o.Audience, o.Origin)
	if err != nil {
		return nil, err
	}
	if !found {
		trust, err := LoadTrustKeyring(o.TrustFile)
		if err != nil {
			return nil, err
		}
		c = clientCheckpoint{Schema: checkpointSchema, Audience: o.Audience, Origin: o.Origin, Trust: trust, Cells: map[string]Publication{}}
		if err = checkpointMarker(o.CheckpointFile, o.Audience, o.Origin, false); err != nil {
			return nil, err
		}
		if err = writeCheckpoint(o.CheckpointFile, c); err != nil {
			return nil, err
		}
	} else if err = checkpointMarker(o.CheckpointFile, o.Audience, o.Origin, true); err != nil {
		return nil, err
	}
	keys, err := c.Trust.PublicKeys()
	if err != nil {
		return nil, err
	}
	m := &Control{options: o, checkpoint: c, keys: keys, selector: &Selector{}, now: time.Now}
	m.selector.restoreCheckpoint(c)
	m.selected, err = NewHTTPClient(m.selector, m.currentKeys, 20*time.Second)
	if err != nil {
		return nil, err
	}
	m.acquisition = &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, ForceAttemptHTTP2: true, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 20 * time.Second, IdleConnTimeout: 30 * time.Second, MaxIdleConns: 4, MaxIdleConnsPerHost: 2}}
	m.fetch = m.fetchGrant
	m.probe = m.probeCandidate
	m.routeProbe = routeprobe.Probe
	return m, nil
}

func (m *Control) currentKeys() map[string]TrustKey {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.keys
}

func (m *Control) HTTPClient(legacy *http.Client) *http.Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.checkpoint.Activated {
		return m.selected
	}
	return legacy
}

func (m *Control) Close() { m.selected.CloseIdleConnections(); m.acquisition.CloseIdleConnections() }

func (m *Control) reloadTrust() error {
	ring, err := LoadTrustKeyring(m.options.TrustFile)
	if err != nil {
		return err
	} // Existing verified trust remains the positive LKG.
	m.mu.RLock()
	c := m.checkpoint
	m.mu.RUnlock()
	if ring.Generation < c.Trust.Generation || ring.Generation == c.Trust.Generation && !reflect.DeepEqual(ring, c.Trust) {
		return errors.New("Agent Edge trust replay or same-generation mutation rejected")
	}
	if reflect.DeepEqual(ring, c.Trust) {
		return nil
	}
	keys, err := ring.PublicKeys()
	if err != nil {
		return err
	}
	c.Trust = ring
	if err = writeCheckpoint(m.options.CheckpointFile, c); err != nil {
		return err
	}
	m.mu.Lock()
	m.checkpoint = c
	m.keys = keys
	m.mu.Unlock()
	m.selected.CloseIdleConnections()
	return nil
}

func (m *Control) acceptGrant(raw []byte, measured ...Round) error {
	m.mu.RLock()
	c := m.checkpoint
	keys := m.keys
	m.mu.RUnlock()
	var previous VerifiedGrant
	if c.LastGrant != nil {
		previous = VerifiedGrant{signed: *c.LastGrant, cellWatermarks: c.Cells}
	}
	v, err := Verify(raw, keys, m.options.Audience, m.options.Origin, &previous, m.now())
	if err != nil {
		return err
	}
	c.LastGrant = &v.signed
	c.Cells = maps.Clone(v.cellWatermarks)
	c.Activated = c.Activated || v.signed.Grant.Mode == "active"
	if len(measured) == 1 {
		err = m.selector.installObserved(raw, keys, m.options.Audience, m.options.Origin, measured[0], m.now(), func(VerifiedGrant) error {
			if err := writeCheckpoint(m.options.CheckpointFile, c); err != nil {
				return err
			}
			m.mu.Lock()
			m.checkpoint = c
			m.mu.Unlock()
			return nil
		})
		if err != nil {
			return err
		}
		m.lastProbe = measured[0].ObservedAt
		m.refreshAt = v.signed.Grant.IssuedAt.Add(v.signed.Grant.ValidUntil.Sub(v.signed.Grant.IssuedAt) / 2)
		m.refreshDegradedSooner(v.signed.Grant, keys)
		return nil
	}
	// Persist the replay floor and activation latch before changing transport.
	if err = writeCheckpoint(m.options.CheckpointFile, c); err != nil {
		return err
	}
	if err = m.selector.Install(raw, keys, m.options.Audience, m.options.Origin, m.now()); err != nil {
		// Even a permission expiring during persistence must not undo the
		// durable activation latch or permit an unsigned native fallback.
		m.mu.Lock()
		m.checkpoint = c
		m.mu.Unlock()
		return err
	}
	m.mu.Lock()
	m.checkpoint = c
	m.mu.Unlock()
	m.refreshAt = v.signed.Grant.IssuedAt.Add(v.signed.Grant.ValidUntil.Sub(v.signed.Grant.IssuedAt) / 2)
	return nil
}

func (m *Control) fetchGrant(ctx context.Context, hint string) ([]byte, error) {
	clients := []*http.Client{}
	if choice, err := m.selector.Current(m.currentKeys(), m.now()); err == nil && choice.Mode == "active" {
		clients = append(clients, m.selected)
	}
	clients = append(clients, m.acquisition)
	var err error
	for _, client := range clients {
		var raw []byte
		raw, err = m.fetchGrantThrough(ctx, hint, client)
		if err == nil {
			return raw, nil
		}
		if ctx.Err() != nil {
			break
		}
	}
	return nil, err
}

func (m *Control) fetchGrantThrough(ctx context.Context, hint string, client *http.Client) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.options.Origin+"/v1/agent/edge-candidates", nil)
	if err != nil {
		return nil, errors.New("Agent Edge acquisition request invalid")
	}
	req.Header.Set("Authorization", "Bearer "+m.options.RuntimeKey)
	if hint != "" {
		req.Header.Set("X-Fugue-Agent-Current-Edge", hint)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, errors.New("Agent Edge acquisition unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("Agent Edge acquisition returned HTTP %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, MaxGrantBytes+1))
	if err != nil || len(raw) > MaxGrantBytes {
		return nil, errors.New("Agent Edge acquisition body invalid")
	}
	return raw, nil
}

// Step is serialized independently from request execution. Failed refreshes
// retain the previous grant only to its original signed expiration.
func (m *Control) Step(ctx context.Context) error {
	m.stepMu.Lock()
	defer m.stepMu.Unlock()
	trustErr := m.reloadTrust()
	keys := m.currentKeys()
	v, permissionErr := m.selector.permission(keys, m.now())
	var fetchErr, probeErr error
	if permissionErr != nil || !m.now().Before(m.refreshAt) {
		hint := ""
		if choice, err := m.selector.Current(keys, m.now()); err == nil {
			hint = choice.Primary.EdgeID
		}
		raw, err := m.fetch(ctx, hint)
		if err == nil {
			// Verify before any candidate dial, then stage fresh measurements
			// while the selected client continues to use the positive grant.
			m.mu.RLock()
			c := m.checkpoint
			m.mu.RUnlock()
			var previous VerifiedGrant
			if c.LastGrant != nil {
				previous = VerifiedGrant{signed: *c.LastGrant, cellWatermarks: c.Cells}
			}
			next, verifyErr := Verify(raw, keys, m.options.Audience, m.options.Origin, &previous, m.now())
			err = verifyErr
			if err == nil {
				var round Round
				round, err = m.measure(ctx, next)
				if err == nil {
					err = m.acceptGrant(raw, round)
				}
			}
		}
		fetchErr = err
		if err != nil {
			m.refreshAt = m.now().Add(5 * time.Second)
		}
		v, permissionErr = m.selector.permission(keys, m.now())
	}
	if permissionErr == nil {
		g := v.View()
		if m.lastProbe.IsZero() || m.now().Sub(m.lastProbe) >= time.Duration(g.Policy.ProbeIntervalSeconds)*time.Second {
			var round Round
			round, probeErr = m.measure(ctx, v)
			if probeErr == nil {
				_, probeErr = m.selector.Observe(round, keys, m.now())
				m.lastProbe = round.ObservedAt
				m.refreshDegradedSooner(g, keys)
				for _, measurement := range round.Measurements {
					if measurement.Disqualified {
						m.refreshAt = time.Time{}
					}
				}
			}
		}
	}
	return errors.Join(trustErr, fetchErr, permissionErr, probeErr)
}

// A temporarily missing independent standby must not wait half the grant lease
// before it can rejoin. Retry on the signed probe cadence while preserving the
// current grant's absolute expiry and measured primary. Do not postpone an
// earlier retry or repeatedly reset the deadline on every Step.
func (m *Control) refreshDegradedSooner(g Grant, keys map[string]TrustKey) {
	choice, err := m.selector.Current(keys, m.now())
	if err == nil && choice.Degraded {
		next := m.now().Add(time.Duration(g.Policy.ProbeIntervalSeconds) * time.Second)
		if next.Before(m.refreshAt) {
			m.refreshAt = next
		}
	}
}

func (m *Control) measure(ctx context.Context, v VerifiedGrant) (Round, error) {
	g := v.View()
	round := Round{GrantDigest: v.Digest(), ObservedAt: m.now(), Measurements: make([]Measurement, len(g.Candidates))}
	ctx, cancel := context.WithDeadline(ctx, g.ValidUntil)
	defer cancel()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i, candidate := range g.Candidates {
		wg.Add(1)
		go func(i int, candidate Candidate) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			round.Measurements[i] = m.probe(ctx, g, candidate)
		}(i, candidate)
	}
	wg.Wait()
	return round, ctx.Err()
}

func (m *Control) probeCandidate(ctx context.Context, g Grant, c Candidate) Measurement {
	measurement := Measurement{EdgeID: c.EdgeID, Address: c.Address}
	timeout := time.Duration(g.Policy.ProbeTimeoutMilliseconds) * time.Millisecond
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	host := strings.TrimPrefix(g.Origin, "https://")
	started := time.Now()
	proof, err := m.routeProbe(ctx, host, "/", c.Address, "", time.Duration(max(1, (g.Policy.ProbeTimeoutMilliseconds+999)/1000))*time.Second)
	measurement.Latency = time.Since(started)
	if err != nil {
		measurement.Disqualified = !errors.Is(err, routeprobe.ErrUnavailable) && ctx.Err() == nil
		return measurement
	}
	b, p := proof.TrafficRelease, c.Publication
	if proof.Version == "" || proof.EdgeID != c.EdgeID || proof.GroupID != p.ServingGroupID || proof.State != "" || !slices.Contains(c.RouteDigests, proof.Digest) || proof.CheckedAt.IsZero() || proof.CheckedAt.Before(g.IssuedAt) || proof.CheckedAt.After(m.now()) || proof.ValidUntil.Before(g.ValidUntil) || b == nil ||
		b.ReleaseSetID != p.ReleaseSetID || b.ReleaseSetDigest != p.ReleaseSetDigest || b.RouteArtifactID != p.RouteArtifactID || b.RouteArtifactDigest != p.RouteArtifactDigest || b.PolicyDigest != p.PolicyDigest || b.IntentDigest != p.IntentDigest || b.InputSnapshotDigest != p.InputSnapshotDigest || b.ReleaseID != p.ReleaseID || b.ReleaseChannel != p.Channel || b.FencingToken != p.FencingToken || b.ScopeKey != p.ScopeKey {
		measurement.Disqualified = true
		return measurement
	}
	measurement.Success = measurement.Latency > 0 && measurement.Latency <= timeout
	return measurement
}

func (m *Control) Status() ControlStatus {
	m.mu.RLock()
	c := m.checkpoint
	keys := m.keys
	m.mu.RUnlock()
	status := ControlStatus{Activated: c.Activated, TrustGeneration: c.Trust.Generation, Degraded: true}
	if c.LastGrant != nil {
		status.Mode = c.LastGrant.Grant.Mode
		status.ValidUntil = c.LastGrant.Grant.ValidUntil
		status.GrantDigest = c.LastGrant.Digest
	}
	if choice, err := m.selector.Current(keys, m.now()); err == nil {
		status.Primary = choice.Primary.EdgeID
		status.Cell = choice.Primary.AuthorityCellID
		status.Degraded = choice.Degraded
		for _, standby := range choice.Standbys {
			status.Standbys = append(status.Standbys, standby.EdgeID)
		}
	}
	return status
}

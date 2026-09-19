package edge

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/proxyproto"
	"fugue/internal/routeartifact"
	"fugue/internal/routeprobe"
	"fugue/internal/routeproof"
	"fugue/internal/trafficbinding"
)

type PlatformServingStatus struct {
	State          string                       `json:"state"`
	LastError      string                       `json:"last_error,omitempty"`
	TrafficRelease *model.TrafficReleaseBinding `json:"traffic_release,omitempty"`
	BundleVersion  string                       `json:"bundle_version,omitempty"`
	RouteProbes    int                          `json:"route_probes"`
	TLSProbes      int                          `json:"tls_probes"`
	VerifiedAt     time.Time                    `json:"verified_at,omitempty"`
	ReportedAt     time.Time                    `json:"reported_at,omitempty"`
}

type platformServingReceipt struct {
	Schema        string                `json:"schema"`
	Route         edgePlatformCandidate `json:"route"`
	TLS           edgePlatformCandidate `json:"tls"`
	BundleVersion string                `json:"bundle_version"`
	Probes        []routeprobe.Proof    `json:"route_probes"`
	Sequence      int64                 `json:"sequence"`
	VerifiedAt    time.Time             `json:"verified_at"`
}

// Serving observation is deliberately read-only with respect to Group
// Authority, Caddy, and the existing bundle/LKG. Only that execution path applies
// configurations. This observer reports what actually reached the executor.
func (s *Service) SyncPlatformServingOnce(ctx context.Context) error {
	return s.syncPlatformServingOnce(ctx, s.probePlatformServingRoute, probePlatformTLS)
}

func (s *Service) syncPlatformServingOnce(ctx context.Context, routeProbe platformServingRouteProbe, tlsProbe platformTLSProbe) error {
	s.platformConsumerMu.Lock()
	defer s.platformConsumerMu.Unlock()
	selection, err := s.selectRouteBundleSource()
	if err != nil {
		return err
	}
	bundle, ok := s.Bundle()
	if selection.candidate || !ok || bundle.TrafficRelease == nil {
		s.mu.Lock()
		s.platformServing = PlatformServingStatus{State: "awaiting_release"}
		s.mu.Unlock()
		return nil
	}
	b := bundle.TrafficRelease
	if err = trafficbinding.ValidateGroup(b, s.Config.EdgeGroupID, true); err != nil {
		return err
	}
	client := platformconsumer.Client{BaseURL: s.Config.APIURL, TokenFile: s.PlatformTokenFile, HTTPClient: s.HTTPClient}
	id, a, artifact, release, err := client.SyncChannel(ctx, model.PlatformConsumerComponentEdgeWorker, s.Config.EdgeID, "global", model.PlatformArtifactKindEdgeRouteBundle, b.ReleaseChannel)
	if err != nil {
		return err
	}
	keys := bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs)
	parent, err := client.ReleaseSet(ctx, id, a, release)
	if err != nil {
		return err
	}
	projection, err := routeartifact.ProjectRelease(parent, artifact, a, release, keys)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(b, projection.TrafficRelease) {
		return errors.New("serving group bundle belongs to another traffic release")
	}
	if _, err = s.verifyPlatformRouteCandidate(artifact, a, release); err != nil {
		return err
	}
	_, ta, tlsArtifact, tr, err := client.SyncChannel(ctx, model.PlatformConsumerComponentEdgeWorker, s.Config.EdgeID, "global", model.PlatformArtifactKindCaddyRouteConfig, b.ReleaseChannel)
	if err != nil {
		return err
	}
	member, memberKind := false, false
	ids, idsOK := parent.Content["artifact_ids"].([]any)
	kinds, kindsOK := parent.Content["artifact_kinds"].([]any)
	if idsOK && kindsOK && len(ids) == len(kinds) {
		for i, id := range ids {
			if id == tlsArtifact.ID {
				member = true
				memberKind = kinds[i] == tlsArtifact.ArtifactKind
			}
		}
	}
	if !member || !memberKind {
		return errors.New("TLS serving artifact is not a ReleaseSet member")
	}
	routeCandidate := edgePlatformCandidate{Artifact: artifact, Assignment: a, Release: release, ReleaseSet: &parent, TrafficRelease: b}
	tlsCandidate := edgePlatformCandidate{Artifact: tlsArtifact, Assignment: ta, Release: tr}
	payload, err := s.verifyPlatformTLSCandidate(tlsCandidate, routeCandidate)
	if err != nil {
		return err
	}
	policy := payload.Policy.TLSReadiness
	if policy == nil || platformconfig.ValidateReadinessProbePolicy(policy) != nil {
		return errors.New("serving verification requires signed probe policy")
	}
	if err = s.validatePlatformServingBundle(bundle, projection); err != nil {
		return err
	}
	verifiedAt := time.Now().UTC()
	var probes []routeprobe.Proof
	var tlsEvidence *platformTLSReadinessReceipt
	previousEvidence := s.platformServingEvidence // guarded by platformConsumerMu
	reuse := previousEvidence != nil && previousEvidence.BundleVersion == bundle.Version && reflect.DeepEqual(previousEvidence.Route.Assignment, a) && reflect.DeepEqual(previousEvidence.TLS.Assignment, ta) && !previousEvidence.VerifiedAt.After(verifiedAt) && verifiedAt.Sub(previousEvidence.VerifiedAt) < time.Duration(policy.ProbeIntervalSeconds)*time.Second
	if reuse {
		summary := summarizePlatformTLSReadiness(previousEvidence.TLS.TLSReadiness, bundle.Version, verifiedAt)
		reuse = summary != nil && summary.Probes > 0 && summary.Probes == summary.ReadyProbes && len(previousEvidence.Probes) > 0
		for _, proof := range previousEvidence.Probes {
			reuse = reuse && proof.ValidUntil.After(verifiedAt)
		}
	}
	if reuse {
		probes, tlsEvidence, verifiedAt = previousEvidence.Probes, previousEvidence.TLS.TLSReadiness, previousEvidence.VerifiedAt
	} else {
		probes, err = s.observePlatformServingRoutes(ctx, bundle, policy, routeProbe)
		if err != nil {
			return err
		}
		tlsEvidence, err = s.observePlatformTLSReadinessWithProbe(ctx, tlsCandidate, artifact, payload, tlsProbe)
		if err != nil {
			return err
		}
	}
	readiness := summarizePlatformTLSReadiness(tlsEvidence, bundle.Version, time.Now().UTC())
	if readiness == nil || readiness.Probes == 0 || readiness.ReadyProbes != readiness.Probes {
		return errors.New("traffic TLS readiness is incomplete")
	}
	if err = client.CheckAssignment(ctx, id, a); err != nil {
		return err
	}
	if err = client.CheckAssignment(ctx, id, ta); err != nil {
		return err
	}
	currentSelection, err := s.selectRouteBundleSource()
	if err != nil || currentSelection != selection {
		return errors.New("traffic serving activation changed")
	}
	if err = s.validatePlatformServingBundle(bundle, projection); err != nil {
		return err
	}
	path := s.Config.CachePath + ".platform-serving.json"
	if strings.TrimSpace(s.Config.CachePath) == "" {
		return errors.New("traffic serving receipt path unavailable")
	}
	var previous platformServingReceipt
	if raw, e := platformconsumer.ReadFile(path, 16<<20); e == nil {
		if json.Unmarshal(raw, &previous) != nil || previous.Sequence <= 0 || previous.Sequence == math.MaxInt64 {
			return errors.New("traffic serving cursor corrupt")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	tlsCandidate.TLSReadiness = tlsEvidence
	receipt := platformServingReceipt{Schema: "fugue.edge.traffic-serving/v1", Route: routeCandidate, TLS: tlsCandidate, BundleVersion: bundle.Version, Probes: probes, Sequence: max(previous.Sequence+1, time.Now().UnixNano()), VerifiedAt: verifiedAt}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if err = lkgcache.AtomicWriteFile(path, raw, 0600); err != nil {
		return err
	}
	for _, assigned := range []model.PlatformConsumerAssignment{a, ta} {
		currentSelection, err := s.selectRouteBundleSource()
		if err != nil || currentSelection != selection {
			return errors.New("traffic serving activation changed before report")
		}
		if err = s.validatePlatformServingBundle(bundle, projection); err != nil {
			return err
		}
		if err = client.CheckAssignment(ctx, id, assigned); err != nil {
			return err
		}
		for _, proof := range probes {
			if !proof.ValidUntil.After(time.Now().UTC()) {
				return errors.New("traffic route evidence expired before report")
			}
		}
		fresh := summarizePlatformTLSReadiness(tlsEvidence, bundle.Version, time.Now().UTC())
		if fresh == nil || fresh.ReadyProbes != fresh.Probes {
			return errors.New("traffic TLS evidence expired before report")
		}
		nonce := make([]byte, 16)
		if _, err = rand.Read(nonce); err != nil {
			return err
		}
		h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: id.Component + ":" + id.NodeID, Component: id.Component, NodeID: id.NodeID, ArtifactKind: assigned.ArtifactKind, ScopeKey: assigned.ScopeKey, ReleaseSetID: assigned.ReleaseSetID, ExpectedConsumerSetID: assigned.ExpectedConsumerSetID, FencingToken: assigned.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", CompatibilityCapabilities: []string{"caddy_apply_probe"}, Sequence: receipt.Sequence, IssuedAt: time.Now().UTC(), Nonce: hex.EncodeToString(nonce), GenerationSequence: assigned.GenerationSequence, DesiredGeneration: assigned.ExpectedGeneration, ActualGeneration: assigned.ExpectedGeneration, LKGGeneration: s.Status().LKGGeneration, ApplyStatus: "applied", ProbeStatus: "passed"}
		h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
		if err != nil {
			return err
		}
		var accepted model.PlatformConsumerHeartbeatResponse
		if err = client.PostJSON(ctx, "/v1/platform-state/consumers/trusted-heartbeat", id.Token, h, &accepted); err != nil {
			return err
		}
		if !accepted.Consumer.IdentityVerified || accepted.Consumer.ConsumerID != h.ConsumerID || accepted.Consumer.Sequence != h.Sequence || accepted.Consumer.EvidenceHash != h.EvidenceHash || accepted.Consumer.ExpectedConsumerSetID != h.ExpectedConsumerSetID {
			return errors.New("traffic serving receipt acknowledgement mismatch")
		}
	}
	s.platformServingEvidence = &receipt
	s.mu.Lock()
	s.platformServing = PlatformServingStatus{State: "serving_verified", TrafficRelease: trafficbinding.Clone(b), BundleVersion: bundle.Version, RouteProbes: len(probes), TLSProbes: readiness.Probes, VerifiedAt: receipt.VerifiedAt, ReportedAt: time.Now().UTC()}
	s.mu.Unlock()
	return nil
}

func (s *Service) validatePlatformServingBundle(expected model.EdgeRouteBundle, projection model.EdgeRouteIntentSnapshot) error {
	bundle, ok := s.Bundle()
	status := s.Status()
	index := s.currentRouteIndex()
	if !ok || !s.Config.CaddyEnabled || !status.Healthy || status.StaleCache || status.MaxStaleExceeded || bundle.Version != expected.Version || !bundle.ValidUntil.After(time.Now()) || status.CaddyAppliedVersion != bundle.Version || status.CaddyLastError != "" || index == nil || index.publication.Candidate || index.bundleVersion != bundle.Version || !reflect.DeepEqual(bundle.TrafficRelease, projection.TrafficRelease) {
		return errors.New("traffic artifact is not the healthy applied Caddy bundle")
	}
	want, err := routeartifact.MaterializeSnapshotForGroup(projection, s.Config.EdgeGroupID)
	if err != nil {
		return err
	}
	if len(want.Routes) != len(bundle.Routes) || !reflect.DeepEqual(want.CachePolicies, bundle.CachePolicies) || !reflect.DeepEqual(want.TLSAllowlist, bundle.TLSAllowlist) {
		return errors.New("traffic serving projection differs")
	}
	digests := map[string]string{}
	for _, r := range want.Routes {
		d, e := routeproof.Digest(r)
		if e != nil {
			return e
		}
		digests[r.Hostname+"\x00"+model.NormalizeAppRoutePathPrefix(r.PathPrefix)] = d
	}
	for _, r := range bundle.Routes {
		key := r.Hostname + "\x00" + model.NormalizeAppRoutePathPrefix(r.PathPrefix)
		d, e := routeproof.Digest(r)
		if e != nil || digests[key] != d {
			return errors.New("traffic serving route differs")
		}
		delete(digests, key)
	}
	if len(digests) != 0 {
		return errors.New("traffic serving route missing")
	}
	if err = probePlatformCandidateIndex(bundle, index); err != nil {
		return err
	}
	raw, err := platformconsumer.ReadFile(s.Config.CachePath, 16<<20)
	if err != nil {
		return errors.New("traffic serving cache unavailable")
	}
	var cached cacheFile
	if json.Unmarshal(raw, &cached) != nil || cached.Version != cacheFileVersion || cached.Candidate || cached.Bundle.Version != bundle.Version || !reflect.DeepEqual(cached.Bundle.TrafficRelease, bundle.TrafficRelease) {
		return errors.New("traffic serving cache differs")
	}
	wantDigest, _ := platformconfig.Digest(bundle)
	gotDigest, _ := platformconfig.Digest(cached.Bundle)
	if wantDigest != gotDigest {
		return errors.New("traffic serving cache content differs")
	}
	return nil
}

type platformServingRouteProbe func(context.Context, model.EdgeRouteBinding, time.Duration) (routeprobe.Proof, error)

func (s *Service) observePlatformServingRoutes(ctx context.Context, bundle model.EdgeRouteBundle, policy *platformconfig.ReadinessProbePolicy, probe platformServingRouteProbe) ([]routeprobe.Proof, error) {
	if policy == nil || platformconfig.ValidateReadinessProbePolicy(policy) != nil || len(bundle.Routes) > policy.MaxProbes {
		return nil, errors.New("traffic route probes exceed signed bounds")
	}
	routes := []model.EdgeRouteBinding{}
	for _, r := range bundle.Routes {
		if model.EdgeRoutePolicyAllowsTraffic(r.RoutePolicy) {
			routes = append(routes, r)
		}
	}
	if len(routes) == 0 {
		return nil, errors.New("traffic route probes empty")
	}
	proofs := make([]routeprobe.Proof, len(routes))
	errs := make([]error, len(routes))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for n := 0; n < min(policy.MaxConcurrency, len(routes)); n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				p, e := probe(ctx, routes[i], time.Duration(policy.ProbeTimeoutSeconds)*time.Second)
				now := time.Now().UTC()
				d, _ := routeproof.Digest(routes[i])
				state := ""
				if routes[i].Status != model.EdgeRouteStatusActive {
					state = routes[i].Status
				}
				if e == nil && (p.Digest != d || p.Version != bundle.Version || p.EdgeID != s.Config.EdgeID || p.GroupID != s.Config.EdgeGroupID || p.State != state || p.CheckedAt.IsZero() || p.CheckedAt.After(now) || !p.ValidUntil.After(now)) {
					e = errors.New("traffic route proof mismatch")
				}
				if e == nil {
					until := p.CheckedAt.Add(time.Duration(policy.FactFreshnessSeconds) * time.Second)
					if until.Before(p.ValidUntil) {
						p.ValidUntil = until
					}
					proofs[i] = p
				}
				errs[i] = e
			}
		}()
	}
	for i := range routes {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return proofs, ctx.Err()
}

func (s *Service) probePlatformServingRoute(ctx context.Context, route model.EdgeRouteBinding, timeout time.Duration) (routeprobe.Proof, error) {
	return s.probePlatformServingRouteWithRoots(ctx, route, timeout, nil)
}

func (s *Service) probePlatformServingRouteWithRoots(ctx context.Context, route model.EdgeRouteBinding, timeout time.Duration, roots *x509.CertPool) (routeprobe.Proof, error) {
	address, err := localTLSProbeAddress(s.Config.CaddyListenAddr)
	if err != nil {
		return routeprobe.Proof{}, err
	}
	nonce := make([]byte, 16)
	if _, err = rand.Read(nonce); err != nil {
		return routeprobe.Proof{}, err
	}
	token := hex.EncodeToString(nonce)
	transport := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, ServerName: route.Hostname, RootCAs: roots}, MaxResponseHeaderBytes: 16 << 10, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		conn, err := (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if s.Config.CaddyProxyProtocolEnabled {
			_ = conn.SetWriteDeadline(time.Now().Add(timeout))
			if _, err = io.WriteString(conn, proxyproto.HeaderV1(conn.LocalAddr(), conn.RemoteAddr())); err != nil {
				conn.Close()
				return nil, err
			}
			_ = conn.SetWriteDeadline(time.Time{})
		}
		return conn, nil
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	u := url.URL{Scheme: "https", Host: route.Hostname, Path: model.NormalizeAppRoutePathPrefix(route.PathPrefix)}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u.String(), nil)
	if err != nil {
		return routeprobe.Proof{}, err
	}
	req.Header.Set(routeproof.RequestHeader, "1")
	req.Header.Set(routeproof.NonceHeader, token)
	if route.Status != model.EdgeRouteStatusActive {
		req.Header.Set(routeproof.StateHeader, route.Status)
	}
	response, err := client.Do(req)
	if err != nil {
		return routeprobe.Proof{}, errors.New("traffic local HTTPS route probe failed")
	}
	defer response.Body.Close()
	now := time.Now().UTC()
	proof, err := routeprobe.ParseResponse(response, token, now)
	if err != nil {
		return proof, err
	}
	if response.TLS == nil || len(response.TLS.VerifiedChains) == 0 {
		return routeprobe.Proof{}, errors.New("traffic route certificate not verified")
	}
	for _, cert := range response.TLS.VerifiedChains[0] {
		if cert.NotAfter.Before(proof.ValidUntil) {
			proof.ValidUntil = cert.NotAfter
		}
	}
	proof.CheckedAt = now
	return proof, nil
}

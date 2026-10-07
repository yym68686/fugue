package edge

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"slices"
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

// Keep only the facts needed during the signed probe interval. The complete
// signed artifacts remain in the durable receipt, but retaining their decoded
// maps here pins a second route/TLS configuration between observation cycles.
type platformServingEvidence struct {
	RouteAssignment model.PlatformConsumerAssignment
	TLSAssignment   model.PlatformConsumerAssignment
	TLSReadiness    *platformTLSReadinessReceipt
	BundleVersion   string
	Probes          []routeprobe.Proof
	VerifiedAt      time.Time
}

var errServingTrafficReleaseMismatch = errors.New("serving group bundle belongs to another traffic release")
var errServingBundleChanged = errors.New("applied serving bundle changed during observation")

func servingObservationChanged(err error) bool {
	return errors.Is(err, errServingTrafficReleaseMismatch) || errors.Is(err, platformconsumer.ErrAssignmentChanged) || errors.Is(err, errServingBundleChanged)
}

const platformServingBindingRetryAttempts = 3

// Serving observation is deliberately read-only with respect to Group
// Authority, Caddy, and the existing bundle/LKG. Only that execution path applies
// configurations. This observer reports what actually reached the executor.
func (s *Service) SyncPlatformServingOnce(ctx context.Context) error {
	return s.syncPlatformServing(ctx, s.probePlatformServingRoute, probePlatformTLS)
}

func (s *Service) syncPlatformServing(ctx context.Context, routeProbe platformServingRouteProbe, tlsProbe platformTLSProbe) error {
	var err error
	for attempt := 0; attempt < platformServingBindingRetryAttempts; attempt++ {
		err = s.syncPlatformServingOnce(ctx, routeProbe, tlsProbe)
		if !servingObservationChanged(err) || ctx.Err() != nil || attempt == platformServingBindingRetryAttempts-1 {
			return err
		}
		delay := time.Duration(attempt+1) * 100 * time.Millisecond
		if errors.Is(err, errServingTrafficReleaseMismatch) {
			// Give the independent route loop a chance to apply the publication.
			// This observer must never download or apply serving configuration.
			delay = min(s.syncInterval(), 30*time.Second)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func (s *Service) syncPlatformServingOnce(ctx context.Context, routeProbe platformServingRouteProbe, tlsProbe platformTLSProbe) (result error) {
	s.platformConsumerMu.Lock()
	defer s.platformConsumerMu.Unlock()
	defer func() {
		if result != nil {
			if ctx.Err() != nil {
				return
			}
			if servingObservationChanged(result) {
				s.mu.Lock()
				s.platformServing.State, s.platformServing.LastError = "awaiting_release", result.Error()
				s.mu.Unlock()
				return
			}
			s.platformServingEvidence = nil
			if ctx.Err() == nil {
				s.mu.Lock()
				s.platformServing.State, s.platformServing.LastError = "failed", result.Error()
				s.mu.Unlock()
			}
		}
	}()
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
	client := platformconsumer.Client{BaseURL: s.Config.APIURL, TokenFile: s.PlatformTokenFile, HTTPClient: s.HTTPClient, AuthorityID: platformcontrol.ConsumerAuthorityID(s.Config.EdgeGroupID)}
	id, a, artifact, release, err := client.SyncServing(ctx, model.PlatformConsumerComponentEdgeWorker, s.Config.EdgeID, platformcontrol.ConfiguredConsumerScope(s.Config.PlatformScopeKey), model.PlatformArtifactKindEdgeRouteBundle)
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
		// Route synchronization and platform assignment publication are
		// independent loops. The bundle can be replaced after the initial
		// snapshot but before the assignment read completes. Re-read the local
		// applied bundle once before treating that publication race as serving
		// evidence; validatePlatformServingBundle below still rechecks the live
		// Caddy state and durable cache before reporting anything.
		if refreshed, ok := s.Bundle(); ok && reflect.DeepEqual(refreshed.TrafficRelease, projection.TrafficRelease) {
			bundle = refreshed
			b = refreshed.TrafficRelease
		} else {
			return fmt.Errorf("%w: bundle_release_set=%s assignment_release_set=%s bundle_route_generation=%s assignment_route_generation=%s bundle_release=%s bundle_channel=%s bundle_fence=%d assignment_release=%s assignment_channel=%s assignment_fence=%d", errServingTrafficReleaseMismatch, b.ReleaseSetID, projection.TrafficRelease.ReleaseSetID, b.RouteArtifactGeneration, projection.TrafficRelease.RouteArtifactGeneration, b.ReleaseID, b.ReleaseChannel, b.FencingToken, projection.TrafficRelease.ReleaseID, projection.TrafficRelease.ReleaseChannel, projection.TrafficRelease.FencingToken)
		}
	}
	if _, err = s.verifyPlatformRouteCandidate(artifact, a, release); err != nil {
		return err
	}
	_, ta, tlsArtifact, tr, err := client.SyncServing(ctx, model.PlatformConsumerComponentEdgeWorker, s.Config.EdgeID, platformcontrol.ConfiguredConsumerScope(s.Config.PlatformScopeKey), model.PlatformArtifactKindCaddyRouteConfig)
	if err != nil {
		if errors.Is(err, platformconsumer.ErrNoServingAssignment) {
			// A selected route without its TLS assignment is an incomplete
			// serving release, not a member eligible to resume shadow reporting.
			return errors.New("traffic TLS serving assignment unavailable")
		}
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
		// Route and TLS assignments are fetched separately. A publication may
		// advance between them; only a fresh assignment read may classify that
		// mismatch as retryable. An unchanged inconsistent parent still fails.
		if err := client.CheckServingAssignment(ctx, id, a); err != nil {
			return err
		}
		return errors.New("TLS serving artifact is not a ReleaseSet member")
	}
	routeCandidate := edgePlatformCandidate{Artifact: artifact, Assignment: a, Release: release, ReleaseSet: &parent, TrafficRelease: b}
	tlsCandidate := edgePlatformCandidate{Artifact: tlsArtifact, Assignment: ta, Release: tr}
	payload, err := s.verifyPlatformTLSCandidate(tlsCandidate, routeCandidate)
	if err != nil {
		for _, selected := range []model.PlatformConsumerAssignment{a, ta} {
			if changed := client.CheckServingAssignment(ctx, id, selected); changed != nil {
				return changed
			}
		}
		return err
	}
	policy := payload.Policy.TLSReadiness
	if policy == nil || platformconfig.ValidateReadinessProbePolicy(policy) != nil {
		return errors.New("serving verification requires signed probe policy")
	}
	// Only verified immutable inputs authorize facts. Runtime failures below
	// revoke positive convergence promptly, without changing the executor or LKG.
	defer func() {
		if result == nil {
			return
		}
		if ctx.Err() != nil || servingObservationChanged(result) {
			return
		}
		if current, exists := s.Bundle(); exists && current.Version != bundle.Version {
			result = errServingBundleChanged
			return
		}
		if reportErr := s.reportPlatformServingFailure(ctx, client, id, bundle, selection, []model.PlatformConsumerAssignment{a, ta}); reportErr != nil {
			result = errors.Join(result, reportErr)
		}
	}()
	verifiedProjection, err := preparePlatformServingProjection(projection, s.Config.EdgeGroupID)
	if err != nil {
		return err
	}
	if err = s.validatePlatformServingBundle(bundle, verifiedProjection); err != nil {
		return err
	}
	verifiedAt := time.Now().UTC()
	var probes []routeprobe.Proof
	var tlsEvidence *platformTLSReadinessReceipt
	previousEvidence := s.platformServingEvidence // guarded by platformConsumerMu
	reuse := previousEvidence != nil && previousEvidence.BundleVersion == bundle.Version && reflect.DeepEqual(previousEvidence.RouteAssignment, a) && reflect.DeepEqual(previousEvidence.TLSAssignment, ta) && !previousEvidence.VerifiedAt.After(verifiedAt) && verifiedAt.Sub(previousEvidence.VerifiedAt) < time.Duration(policy.ProbeIntervalSeconds)*time.Second
	if reuse {
		summary := summarizePlatformTLSReadiness(previousEvidence.TLSReadiness, bundle.Version, verifiedAt)
		reuse = summary != nil && summary.Probes > 0 && summary.Probes == summary.ReadyProbes && len(previousEvidence.Probes) > 0
		for _, proof := range previousEvidence.Probes {
			reuse = reuse && proof.ValidUntil.After(verifiedAt)
		}
	}
	if reuse {
		probes, tlsEvidence, verifiedAt = previousEvidence.Probes, previousEvidence.TLSReadiness, previousEvidence.VerifiedAt
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
	if err = client.CheckServingAssignment(ctx, id, a); err != nil {
		return err
	}
	if err = client.CheckServingAssignment(ctx, id, ta); err != nil {
		return err
	}
	currentSelection, err := s.selectRouteBundleSource()
	if err != nil || currentSelection != selection {
		return errors.New("traffic serving activation changed")
	}
	if err = s.validatePlatformServingBundle(bundle, verifiedProjection); err != nil {
		return err
	}
	path := s.Config.CachePath + ".platform-serving.json"
	if strings.TrimSpace(s.Config.CachePath) == "" {
		return errors.New("traffic serving receipt path unavailable")
	}
	sequence, err := s.nextPlatformServingSequence()
	if err != nil {
		return err
	}
	tlsCandidate.TLSReadiness = tlsEvidence
	receipt := platformServingReceipt{Schema: "fugue.edge.traffic-serving/v1", Route: routeCandidate, TLS: tlsCandidate, BundleVersion: bundle.Version, Probes: probes, Sequence: sequence, VerifiedAt: verifiedAt}
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
		if err = s.validatePlatformServingBundle(bundle, verifiedProjection); err != nil {
			return err
		}
		if err = client.CheckServingAssignment(ctx, id, assigned); err != nil {
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
		if err = s.reportPlatformServingFact(ctx, client, id, assigned, receipt.Sequence, true); err != nil {
			return err
		}
	}
	s.platformServingEvidence = &platformServingEvidence{RouteAssignment: a, TLSAssignment: ta, TLSReadiness: tlsEvidence, BundleVersion: bundle.Version, Probes: probes, VerifiedAt: verifiedAt}
	s.mu.Lock()
	s.platformServing = PlatformServingStatus{State: "serving_verified", TrafficRelease: trafficbinding.Clone(b), BundleVersion: bundle.Version, RouteProbes: len(probes), TLSProbes: readiness.Probes, VerifiedAt: receipt.VerifiedAt, ReportedAt: time.Now().UTC()}
	s.mu.Unlock()
	return nil
}

// Success and failure facts share a cursor, independent of the positive receipt.
// The previous receipt supplies the migration floor but is never overwritten by
// a failure. The caller holds platformConsumerMu through persistence/reporting.
func (s *Service) nextPlatformServingSequence() (int64, error) {
	if strings.TrimSpace(s.Config.CachePath) == "" {
		return 0, errors.New("traffic serving cursor path unavailable")
	}
	var previous platformServingReceipt
	if raw, err := platformconsumer.ReadFile(s.Config.CachePath+".platform-serving.json", 16<<20); err == nil {
		if json.Unmarshal(raw, &previous) != nil || previous.Sequence <= 0 {
			return 0, errors.New("traffic serving receipt cursor corrupt")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	path := s.Config.CachePath + ".platform-serving-cursor.json"
	var persisted int64
	if raw, err := platformconsumer.ReadFile(path, 1024); err == nil {
		if json.Unmarshal(raw, &persisted) != nil || persisted <= 0 {
			return 0, errors.New("traffic serving cursor corrupt")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	last := max(previous.Sequence, persisted)
	if last == math.MaxInt64 {
		return 0, errors.New("traffic serving cursor exhausted")
	}
	sequence := max(last+1, time.Now().UnixNano())
	raw, _ := json.Marshal(sequence)
	if err := lkgcache.AtomicWriteFile(path, raw, 0600); err != nil {
		return 0, err
	}
	return sequence, nil
}

func (s *Service) reportPlatformServingFailure(ctx context.Context, client platformconsumer.Client, id platformconsumer.Identity, bundle model.EdgeRouteBundle, selection routeSourceSelection, assignments []model.PlatformConsumerAssignment) error {
	current := func() error {
		selected, err := s.selectRouteBundleSource()
		actual, ok := s.Bundle()
		if err != nil || selected != selection || selected.candidate || !ok || actual.Version != bundle.Version || !reflect.DeepEqual(actual.TrafficRelease, bundle.TrafficRelease) {
			return errors.New("traffic failure observation activation or bundle changed")
		}
		return nil
	}
	if err := current(); err != nil {
		return err
	}
	for _, a := range assignments {
		if err := client.CheckServingAssignment(ctx, id, a); err != nil {
			return err
		}
	}
	sequence, err := s.nextPlatformServingSequence()
	if err != nil {
		return err
	}
	var reports error
	for _, a := range assignments {
		if err := current(); err != nil {
			return errors.Join(reports, err)
		}
		if err := client.CheckServingAssignment(ctx, id, a); err != nil {
			return errors.Join(reports, err)
		}
		reports = errors.Join(reports, s.reportPlatformServingFact(ctx, client, id, a, sequence, false))
	}
	if reports == nil {
		s.mu.Lock()
		s.platformServing.ReportedAt = time.Now().UTC()
		s.mu.Unlock()
	}
	return reports
}

func (s *Service) reportPlatformServingFact(ctx context.Context, client platformconsumer.Client, id platformconsumer.Identity, assigned model.PlatformConsumerAssignment, sequence int64, positive bool) error {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: id.BoundConsumerID(), Component: id.Component, NodeID: id.NodeID, ArtifactKind: assigned.ArtifactKind, ScopeKey: assigned.ScopeKey, ReleaseSetID: assigned.ReleaseSetID, ExpectedConsumerSetID: assigned.ExpectedConsumerSetID, FencingToken: assigned.FencingToken, ProtocolVersion: "v1", SchemaVersion: "v1", CompatibilityCapabilities: []string{platformcontrol.TrafficReleaseCapabilityV1, platformcontrol.CellRoutesCapabilityV1, "caddy_apply_probe"}, Sequence: sequence, IssuedAt: time.Now().UTC(), Nonce: hex.EncodeToString(nonce), GenerationSequence: assigned.GenerationSequence, DesiredGeneration: assigned.ExpectedGeneration, ApplyStatus: "failed", ProbeStatus: "failed", LastError: "traffic serving apply, cache or readiness verification failed"}
	if positive {
		h.ActualGeneration, h.LKGGeneration = assigned.ExpectedGeneration, s.Status().LKGGeneration
		h.ApplyStatus, h.ProbeStatus, h.LastError = "applied", "passed", ""
	}
	var err error
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
	return nil
}

// Reuse only expectations derived from the verified immutable projection during
// one observation. Live bundle, route index and durable cache checks still run
// before each report; no runtime fact is cached across those checks or cycles.
type platformServingProjection struct {
	bundle  model.EdgeRouteBundle
	digests map[string]string
}

func preparePlatformServingProjection(projection model.EdgeRouteIntentSnapshot, group string) (platformServingProjection, error) {
	want, err := routeartifact.MaterializeSnapshotForGroup(projection, group)
	if err != nil {
		return platformServingProjection{}, err
	}
	digests := make(map[string]string, len(want.Routes))
	for _, r := range want.Routes {
		d, err := routeproof.Digest(r)
		if err != nil {
			return platformServingProjection{}, err
		}
		digests[r.Hostname+"\x00"+model.NormalizeAppRoutePathPrefix(r.PathPrefix)] = d
	}
	return platformServingProjection{bundle: want, digests: digests}, nil
}

func (s *Service) validatePlatformServingBundle(expected model.EdgeRouteBundle, projection platformServingProjection) error {
	bundle, ok := s.Bundle()
	// A renewed bundle makes this observation obsolete even when the traffic
	// release is unchanged. Reread and reprobe it; never report the old snapshot
	// as negative evidence against the new executor state.
	if ok && bundle.Version != expected.Version {
		return errServingBundleChanged
	}
	status := s.Status()
	index := s.currentRouteIndex()
	if !ok || !s.Config.CaddyEnabled || !status.Healthy || status.StaleCache || status.MaxStaleExceeded || !bundle.ValidUntil.After(time.Now()) || status.CaddyAppliedVersion != bundle.Version || status.CaddyLastError != "" || index == nil || index.publication.Candidate || index.bundleVersion != bundle.Version || !reflect.DeepEqual(bundle.TrafficRelease, projection.bundle.TrafficRelease) {
		return errors.New("traffic artifact is not the healthy applied Caddy bundle")
	}
	want := projection.bundle
	if len(want.Routes) != len(bundle.Routes) || !reflect.DeepEqual(want.CachePolicies, bundle.CachePolicies) || !reflect.DeepEqual(want.TLSAllowlist, bundle.TLSAllowlist) {
		return errors.New("traffic serving projection differs")
	}
	digests := maps.Clone(projection.digests)
	for _, r := range bundle.Routes {
		key := r.Hostname + "\x00" + model.NormalizeAppRoutePathPrefix(r.PathPrefix)
		d, e := routeproof.Digest(r)
		if e != nil || digests[key] != d {
			return errors.New("traffic serving route differs")
		}
		if model.EdgeRoutePolicyAllowsTraffic(r.RoutePolicy) {
			if err := probePlatformCandidateRoute(r, index, bundle.Version, d); err != nil {
				return err
			}
		}
		delete(digests, key)
	}
	if len(digests) != 0 {
		return errors.New("traffic serving route missing")
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
				state := s.platformServingRouteProofState(routes[i])
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

func (s *Service) platformServingRouteProofState(route model.EdgeRouteBinding) string {
	if slices.Contains(route.ExcludedEdgeIDs, s.Config.EdgeID) || slices.Contains(route.ExcludedEdgeGroupIDs, s.Config.EdgeGroupID) {
		return routeproof.StateExcluded
	}
	if route.Status != model.EdgeRouteStatusActive {
		return route.Status
	}
	return ""
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
	if state := s.platformServingRouteProofState(route); state != "" {
		req.Header.Set(routeproof.StateHeader, state)
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

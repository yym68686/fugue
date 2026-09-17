package edge

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"reflect"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

// TLS shadow validation never attests live certificates or changes Caddy.
type PlatformTLSCandidateStatus struct {
	State            string    `json:"state"`
	ArtifactID       string    `json:"artifact_id,omitempty"`
	Digest           string    `json:"digest,omitempty"`
	ReleaseSetID     string    `json:"release_set_id,omitempty"`
	RouteArtifactID  string    `json:"route_artifact_id,omitempty"`
	RouteDigest      string    `json:"route_digest,omitempty"`
	CertificateCount int       `json:"certificate_count"`
	AllowlistCount   int       `json:"allowlist_count"`
	Sequence         int64     `json:"sequence,omitempty"`
	VerifiedAt       time.Time `json:"verified_at,omitempty"`
	ReportedAt       time.Time `json:"reported_at,omitempty"`
	LastError        string    `json:"last_error,omitempty"`
	Serving          bool      `json:"serving"`
	TLSVerified      bool      `json:"tls_verified"`
}

type platformTLSCandidatePayload struct {
	Schema       string                                `json:"schema_version"`
	Generation   string                                `json:"generation"`
	Certificates []platformconfig.TLSIntent            `json:"certificates"`
	DomainStates []platformconfig.TLSDomainObservation `json:"domain_states,omitempty"`
	TLSAllowlist []model.EdgeTLSAllowlistEntry         `json:"tls_allowlist,omitempty"`
	Policy       platformconfig.PolicySnapshot         `json:"policy"`
	Lineage      platformconfig.Lineage                `json:"lineage"`
}

func (s *Service) verifyPlatformTLSCandidate(tls, route edgePlatformCandidate) (platformTLSCandidatePayload, error) {
	var payload platformTLSCandidatePayload
	a, artifact, release := tls.Assignment, tls.Artifact, tls.Release
	if a.ExpectedConsumerSetID == "" || a.GenerationSequence <= 0 || a.FencingToken <= 0 || a.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow ||
		a.ArtifactKind != model.PlatformArtifactKindCaddyRouteConfig || artifact.ArtifactKind != a.ArtifactKind || artifact.ID != a.ArtifactID || artifact.ScopeKey != a.ScopeKey ||
		artifact.Generation != a.ExpectedGeneration || artifact.GenerationSequence != a.GenerationSequence || artifact.ContentHash != a.ContentHash || artifact.Status != model.PlatformArtifactStatusValidated ||
		release.ID != a.ArtifactReleaseID || release.ArtifactID != a.ReleaseSetID || release.ArtifactKind != model.PlatformArtifactKindReleaseSet || release.ScopeKey != a.ScopeKey ||
		release.Status != model.PlatformArtifactReleaseStatusActive || release.ReleaseChannel != a.ReleaseChannel || release.FencingToken != a.FencingToken || release.Generation == "" || artifact.Metadata["release_set_generation"] != release.Generation {
		return payload, errors.New("TLS candidate binding mismatch")
	}
	if !platformsafety.EvaluateArtifactIntegrity(artifact, bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs)).Pass {
		return payload, errors.New("TLS candidate signature or digest rejected")
	}
	routes, err := s.verifyPlatformRouteCandidate(route.Artifact, route.Assignment, route.Release)
	if err != nil {
		return payload, err
	}
	if route.Assignment.ReleaseSetID != a.ReleaseSetID || route.Assignment.ArtifactReleaseID != a.ArtifactReleaseID || route.Assignment.FencingToken != a.FencingToken || route.Assignment.ScopeKey != a.ScopeKey || route.Release.Generation != release.Generation {
		return payload, errors.New("TLS and route candidates belong to different releases")
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return payload, errors.New("TLS candidate content invalid")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&payload) != nil || dec.Decode(&struct{}{}) != io.EOF || payload.Schema != platformconfig.SchemaVersion || payload.Generation != artifact.Metadata["intent_generation"] || platformconfig.ValidatePolicySnapshot(payload.Policy) != nil {
		return payload, errors.New("TLS candidate schema invalid")
	}
	policyDigest, err := platformconfig.Digest(payload.Policy)
	if err != nil || payload.Policy.Scope != a.ScopeKey || payload.Lineage.PolicyDigest != policyDigest || !reflect.DeepEqual(payload.Lineage, routes.Lineage) || payload.Generation != routes.Generation {
		return payload, errors.New("TLS candidate policy or route lineage mismatch")
	}
	for key, value := range platformconfig.LineageMetadata(payload.Lineage) {
		if value == "" || artifact.Metadata[key] != value {
			return payload, errors.New("TLS candidate lineage binding mismatch")
		}
	}
	intent := platformconfig.PlatformIntent{TLS: payload.Certificates}
	hosts := map[string]map[string]bool{}
	for _, r := range routes.Routes {
		intent.Routes = append(intent.Routes, r.RouteIntent)
		if hosts[r.Hostname] == nil {
			hosts[r.Hostname] = map[string]bool{}
		}
		hosts[r.Hostname][r.TLSPolicy] = true
	}
	// TLS is selected by SNI hostname, while platform and application paths
	// may share that hostname. The signed hostname reference must match an
	// actual route policy; it does not rewrite the other paths' policies.
	for _, cert := range payload.Certificates {
		if !hosts[cert.Hostname][cert.Policy] {
			return payload, errors.New("TLS certificate reference lacks a matching route policy")
		}
		delete(hosts, cert.Hostname)
	}
	for _, policies := range hosts {
		for policy := range policies {
			if policy != "" {
				return payload, errors.New("route TLS policy has no certificate reference")
			}
		}
	}
	// The signed artifact's original creation time bounds historical events.
	// These events are not renewed or converted to live TLS readiness evidence.
	captured := artifact.CreatedAt
	allowlist, err := platformconfig.CompileTLSDomains(intent, platformconfig.RuntimeSnapshot{CapturedAt: &captured, TLSDomains: payload.DomainStates})
	if err != nil {
		return payload, err
	}
	equal := func(a, b []model.EdgeTLSAllowlistEntry) bool {
		if len(a) == 0 && len(b) == 0 {
			return true
		}
		return reflect.DeepEqual(a, b)
	}
	if !equal(allowlist, payload.TLSAllowlist) || !equal(allowlist, routes.TLSAllowlist) {
		return payload, errors.New("TLS candidate allowlist differs from domain facts or route artifact")
	}
	return payload, nil
}

func (s *Service) SyncPlatformTLSShadowOnce(ctx context.Context) error {
	s.platformConsumerMu.Lock()
	defer s.platformConsumerMu.Unlock()
	selection, err := s.selectRouteBundleSource()
	if err != nil {
		return errors.New("TLS reporting activation unavailable")
	}
	if selection.candidate {
		s.mu.Lock()
		s.platformTLSCandidate.State, s.platformTLSCandidate.ReportedAt = "inactive", time.Time{}
		s.mu.Unlock()
		return nil
	}
	if strings.TrimSpace(s.Config.CachePath) == "" {
		return errors.New("TLS candidate cache path is required")
	}
	client := platformconsumer.Client{BaseURL: s.Config.APIURL, TokenFile: s.PlatformTokenFile, HTTPClient: s.HTTPClient}
	id, a, artifact, release, err := client.Sync(ctx, model.PlatformConsumerComponentEdgeWorker, s.Config.EdgeID, "global", model.PlatformArtifactKindCaddyRouteConfig)
	if err != nil {
		return err
	}
	_, ra, route, rr, err := client.Sync(ctx, model.PlatformConsumerComponentEdgeWorker, s.Config.EdgeID, "global", model.PlatformArtifactKindEdgeRouteBundle)
	if err != nil {
		return err
	}
	c := edgePlatformCandidate{Artifact: artifact, Assignment: a, Release: release}
	payload, err := s.verifyPlatformTLSCandidate(c, edgePlatformCandidate{Artifact: route, Assignment: ra, Release: rr})
	if err != nil {
		return err
	}
	path := s.Config.CachePath + ".platform-tls-shadow.json"
	var prev edgePlatformCandidate
	if raw, readErr := platformconsumer.ReadFile(path, 8<<20); readErr == nil {
		if json.Unmarshal(raw, &prev) != nil || prev.Sequence <= 0 || prev.Sequence == math.MaxInt64 {
			return errors.New("TLS candidate cursor is corrupt")
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return errors.New("TLS candidate cursor unavailable")
	}
	if prev.Assignment.FencingToken > a.FencingToken || prev.Assignment.GenerationSequence > a.GenerationSequence {
		return errors.New("TLS candidate replay rejected")
	}
	c.Sequence, c.VerifiedAt = max(prev.Sequence+1, time.Now().UnixNano()), time.Now().UTC()
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if err := lkgcache.AtomicWriteFile(path, raw, 0600); err != nil {
		return errors.New("persist TLS candidate failed")
	}
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	// No independent TLS serving/LKG generation has been established yet.
	h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: id.Component + ":" + id.NodeID, Component: id.Component, NodeID: id.NodeID, ArtifactKind: a.ArtifactKind, ScopeKey: a.ScopeKey, ReleaseSetID: a.ReleaseSetID, ExpectedConsumerSetID: a.ExpectedConsumerSetID, FencingToken: a.FencingToken, ProtocolVersion: model.PlatformConsumerProtocolVersionV1, SchemaVersion: model.PlatformConsumerSchemaVersionV1, Sequence: c.Sequence, IssuedAt: c.VerifiedAt, Nonce: hex.EncodeToString(nonce), GenerationSequence: a.GenerationSequence, DesiredGeneration: a.ExpectedGeneration, CandidateGeneration: a.ExpectedGeneration, ApplyStatus: "staged", ProbeStatus: "shadow_validated"}
	h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
	if err != nil {
		return err
	}
	current, err := s.selectRouteBundleSource()
	if err != nil || current != selection {
		return errors.New("TLS reporting activation changed")
	}
	var receipt model.PlatformConsumerHeartbeatResponse
	if err = client.PostJSON(ctx, "/v1/platform-state/consumers/trusted-heartbeat", id.Token, h, &receipt); err != nil {
		return err
	}
	if !receipt.Consumer.IdentityVerified || receipt.Consumer.ConsumerID != h.ConsumerID || receipt.Consumer.Sequence != h.Sequence || receipt.Consumer.ExpectedConsumerSetID != h.ExpectedConsumerSetID || receipt.Consumer.EvidenceHash != h.EvidenceHash {
		return errors.New("TLS heartbeat receipt mismatch")
	}
	s.mu.Lock()
	s.platformTLSCandidate = PlatformTLSCandidateStatus{State: "shadow_verified", ArtifactID: a.ArtifactID, Digest: a.ContentHash, ReleaseSetID: a.ReleaseSetID, RouteArtifactID: route.ID, RouteDigest: route.ContentHash, CertificateCount: len(payload.Certificates), AllowlistCount: len(payload.TLSAllowlist), Sequence: c.Sequence, VerifiedAt: c.VerifiedAt, ReportedAt: time.Now().UTC()}
	s.mu.Unlock()
	return nil
}

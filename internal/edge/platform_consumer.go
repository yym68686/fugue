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
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	"fugue/internal/routeartifact"
	"fugue/internal/routeproof"
)

type PlatformCandidateStatus struct {
	State            string    `json:"state"`
	ArtifactID       string    `json:"artifact_id,omitempty"`
	Digest           string    `json:"digest,omitempty"`
	ReleaseSetID     string    `json:"release_set_id,omitempty"`
	RouteCount       int       `json:"route_count"`
	RouteIndexDigest string    `json:"route_index_digest,omitempty"`
	Sequence         int64     `json:"sequence,omitempty"`
	VerifiedAt       time.Time `json:"verified_at,omitempty"`
	ReportedAt       time.Time `json:"reported_at,omitempty"`
	LastError        string    `json:"last_error,omitempty"`
}

type edgePlatformCandidate struct {
	Artifact         model.PlatformArtifact           `json:"artifact"`
	Assignment       model.PlatformConsumerAssignment `json:"assignment"`
	Release          model.PlatformArtifactRelease    `json:"release"`
	Sequence         int64                            `json:"sequence"`
	VerifiedAt       time.Time                        `json:"verified_at"`
	RouteIndexDigest string                           `json:"route_index_digest"`
}

// validatePlatformCandidateIndex probes a detached bundle using the same
// projection and lookup semantics as serving. It never loads Caddy, publishes
// the index, or changes the serving cache, so its evidence remains shadow-only.
func validatePlatformCandidateIndex(artifact model.PlatformArtifact, edgeGroupID string) (string, error) {
	bundle, err := routeartifact.MaterializeForGroup(artifact, edgeGroupID)
	if err != nil {
		return "", err
	}
	index := buildEdgeRouteIndex(bundle, edgeGroupID, routePublicationMetadata{Candidate: true})
	if err := probePlatformCandidateIndex(bundle, index); err != nil {
		return "", err
	}
	return platformconfig.Digest(bundle)
}

func probePlatformCandidateIndex(bundle model.EdgeRouteBundle, index *edgeRouteIndex) error {
	for _, expected := range bundle.Routes {
		if !model.EdgeRoutePolicyAllowsTraffic(expected.RoutePolicy) {
			continue
		}
		actual, ok, fallback, version, _ := index.routeForRequest(expected.Hostname, expected.PathPrefix)
		if !ok || version != bundle.Version || (expected.Status == model.EdgeRouteStatusActive && fallback) {
			return errors.New("candidate route index lookup failed")
		}
		want, err := routeproof.Digest(expected)
		if err != nil {
			return err
		}
		got, err := routeproof.Digest(actual)
		if err != nil || want != got {
			return errors.New("candidate route index behavior mismatch")
		}
	}
	return nil
}

func (s *Service) runPlatformShadowConsumer(ctx context.Context) {
	if s.PlatformTokenFile == "" {
		return
	}
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		if err := s.SyncPlatformShadowOnce(ctx); err != nil && ctx.Err() == nil {
			s.mu.Lock()
			s.platformCandidate.State = "failed"
			s.platformCandidate.LastError = err.Error()
			s.mu.Unlock()
			s.Logger.Printf("edge platform candidate failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Service) SyncPlatformShadowOnce(ctx context.Context) error {
	s.platformConsumerMu.Lock()
	defer s.platformConsumerMu.Unlock()
	// A missing/corrupt activation file cannot authorize either A/B worker.
	selection, err := s.selectRouteBundleSource()
	if err != nil {
		return errors.New("edge platform reporting activation unavailable")
	}
	if selection.candidate {
		s.mu.Lock()
		s.platformCandidate.State = "inactive"
		s.platformCandidate.ReportedAt = time.Time{}
		s.mu.Unlock()
		return nil
	}
	if strings.TrimSpace(s.Config.CachePath) == "" {
		return errors.New("edge platform candidate cache path is required")
	}
	client := platformconsumer.Client{BaseURL: s.Config.APIURL, TokenFile: s.PlatformTokenFile, HTTPClient: s.HTTPClient}
	id, assignment, artifact, release, err := client.Sync(ctx, model.PlatformConsumerComponentEdgeWorker, s.Config.EdgeID, "global", model.PlatformArtifactKindEdgeRouteBundle)
	if err != nil {
		return err
	}
	if assignment.ExpectedConsumerSetID == "" || assignment.ReleaseSetID == "" || assignment.ArtifactReleaseID == "" || assignment.GenerationSequence <= 0 || assignment.FencingToken <= 0 || artifact.ArtifactKind != model.PlatformArtifactKindEdgeRouteBundle || artifact.ScopeKey != assignment.ScopeKey || artifact.GenerationSequence != assignment.GenerationSequence || artifact.ID != assignment.ArtifactID || artifact.Status != model.PlatformArtifactStatusValidated || artifact.ContentHash != assignment.ContentHash || artifact.Generation != assignment.ExpectedGeneration || release.ID != assignment.ArtifactReleaseID || release.ArtifactID != assignment.ReleaseSetID || release.ArtifactKind != model.PlatformArtifactKindReleaseSet || release.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow || release.Status != model.PlatformArtifactReleaseStatusActive || release.FencingToken != assignment.FencingToken || release.Generation == "" || artifact.Metadata["release_set_generation"] != release.Generation {
		return errors.New("edge platform candidate binding mismatch")
	}
	if !platformsafety.EvaluateArtifactIntegrity(artifact, bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs)).Pass {
		return errors.New("edge platform candidate signature or digest rejected")
	}
	var payload struct {
		Schema        string                         `json:"schema_version"`
		Generation    string                         `json:"generation"`
		Routes        []platformconfig.CompiledRoute `json:"routes"`
		CachePolicies []model.CachePolicy            `json:"cache_policies,omitempty"`
		Policy        platformconfig.PolicySnapshot  `json:"policy"`
		Lineage       platformconfig.Lineage         `json:"lineage"`
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return errors.New("edge platform candidate content invalid")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&payload) != nil || dec.Decode(&struct{}{}) != io.EOF || payload.Schema != platformconfig.SchemaVersion || payload.Generation != artifact.Metadata["intent_generation"] || platformconfig.ValidatePolicySnapshot(payload.Policy) != nil {
		return errors.New("edge platform candidate schema invalid")
	}
	routeIntents := make([]platformconfig.RouteIntent, 0, len(payload.Routes))
	for _, route := range payload.Routes {
		routeIntents = append(routeIntents, route.RouteIntent)
	}
	if platformconfig.ValidatePlatformIntent(platformconfig.PlatformIntent{SchemaVersion: payload.Schema, Generation: payload.Generation, Scope: assignment.ScopeKey, Routes: routeIntents, CachePolicies: payload.CachePolicies}) != nil {
		return errors.New("edge platform candidate routes invalid")
	}
	policyDigest, err := platformconfig.Digest(payload.Policy)
	if err != nil || payload.Policy.Scope != assignment.ScopeKey || payload.Lineage.IntentGeneration != payload.Generation || payload.Lineage.PolicyGeneration != payload.Policy.Generation || payload.Lineage.PolicyDigest != policyDigest {
		return errors.New("edge platform candidate policy lineage invalid")
	}
	for key, value := range map[string]string{"intent_generation": payload.Lineage.IntentGeneration, "policy_generation": payload.Lineage.PolicyGeneration, "intent_digest": payload.Lineage.IntentDigest, "policy_digest": payload.Lineage.PolicyDigest, "input_snapshot_digest": payload.Lineage.InputSnapshotDigest, "compiler_version": payload.Lineage.CompilerVersion} {
		if value == "" || artifact.Metadata[key] != value {
			return errors.New("edge platform candidate lineage binding mismatch")
		}
	}
	routeIndexDigest, err := validatePlatformCandidateIndex(artifact, s.Config.EdgeGroupID)
	if err != nil {
		return err
	}
	path := s.Config.CachePath + ".platform-shadow.json"
	var prev edgePlatformCandidate
	if b, e := platformconsumer.ReadFile(path, 8<<20); e == nil {
		if json.Unmarshal(b, &prev) != nil || prev.Sequence <= 0 || prev.Sequence == math.MaxInt64 {
			return errors.New("edge platform candidate cursor is corrupt")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return errors.New("edge platform candidate cursor unavailable")
	}
	if prev.Assignment.FencingToken > assignment.FencingToken || prev.Assignment.GenerationSequence > assignment.GenerationSequence {
		return errors.New("edge platform candidate replay rejected")
	}
	c := edgePlatformCandidate{Artifact: artifact, Assignment: assignment, Release: release, Sequence: max(prev.Sequence+1, time.Now().UnixNano()), VerifiedAt: time.Now().UTC(), RouteIndexDigest: routeIndexDigest}
	b, err := json.Marshal(c)
	if err != nil {
		return errors.New("encode edge platform candidate failed")
	}
	if err := lkgcache.AtomicWriteFile(path, b, 0600); err != nil {
		return errors.New("persist edge platform candidate failed")
	}
	status := s.Status()
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: id.Component + ":" + id.NodeID, Component: id.Component, NodeID: id.NodeID, ArtifactKind: assignment.ArtifactKind, ScopeKey: assignment.ScopeKey, ReleaseSetID: assignment.ReleaseSetID, ExpectedConsumerSetID: assignment.ExpectedConsumerSetID, FencingToken: assignment.FencingToken, ProtocolVersion: model.PlatformConsumerProtocolVersionV1, SchemaVersion: model.PlatformConsumerSchemaVersionV1, Sequence: c.Sequence, IssuedAt: time.Now().UTC(), Nonce: hex.EncodeToString(nonce), GenerationSequence: assignment.GenerationSequence, DesiredGeneration: assignment.ExpectedGeneration, ActualGeneration: status.ServingGeneration, CandidateGeneration: assignment.ExpectedGeneration, LKGGeneration: status.LKGGeneration, ApplyStatus: "staged", ProbeStatus: "shadow_validated", ServingLKG: status.StaleCache, LKGExpired: status.MaxStaleExceeded}
	if strings.TrimSpace(status.CaddyAppliedVersion) == strings.TrimSpace(status.BundleVersion) && strings.TrimSpace(status.CaddyLastError) == "" && s.metricSnapshot().Metrics.CaddyRouteCount > 0 {
		h.CompatibilityCapabilities = []string{"caddy_apply_probe"}
	}
	h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
	if err != nil {
		return err
	}
	var receipt model.PlatformConsumerHeartbeatResponse
	currentSelection, err := s.selectRouteBundleSource()
	if err != nil || currentSelection != selection {
		return errors.New("edge platform reporting activation changed")
	}
	if err = client.PostJSON(ctx, "/v1/platform-state/consumers/trusted-heartbeat", id.Token, h, &receipt); err != nil {
		return err
	}
	if !receipt.Consumer.IdentityVerified || receipt.Consumer.ConsumerID != h.ConsumerID || receipt.Consumer.Sequence != h.Sequence || receipt.Consumer.ExpectedConsumerSetID != h.ExpectedConsumerSetID || receipt.Consumer.EvidenceHash != h.EvidenceHash {
		return errors.New("edge platform heartbeat receipt mismatch")
	}
	s.mu.Lock()
	s.platformCandidate = PlatformCandidateStatus{State: "shadow_verified", ArtifactID: assignment.ArtifactID, Digest: assignment.ContentHash, ReleaseSetID: assignment.ReleaseSetID, RouteCount: len(payload.Routes), RouteIndexDigest: routeIndexDigest, Sequence: c.Sequence, VerifiedAt: c.VerifiedAt, ReportedAt: time.Now().UTC()}
	s.mu.Unlock()
	s.Logger.Printf("edge platform candidate verified; artifact=%s digest=%s routes=%d sequence=%d serving=%s", assignment.ArtifactID, assignment.ContentHash, len(payload.Routes), c.Sequence, status.ServingGeneration)
	return nil
}

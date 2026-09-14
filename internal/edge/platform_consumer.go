package edge

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

type PlatformCandidateStatus struct {
	State        string    `json:"state"`
	ArtifactID   string    `json:"artifact_id,omitempty"`
	Digest       string    `json:"digest,omitempty"`
	ReleaseSetID string    `json:"release_set_id,omitempty"`
	RouteCount   int       `json:"route_count"`
	Sequence     int64     `json:"sequence,omitempty"`
	VerifiedAt   time.Time `json:"verified_at,omitempty"`
	ReportedAt   time.Time `json:"reported_at,omitempty"`
	LastError    string    `json:"last_error,omitempty"`
}

type edgePlatformCandidate struct {
	Artifact   model.PlatformArtifact           `json:"artifact"`
	Assignment model.PlatformConsumerAssignment `json:"assignment"`
	Release    model.PlatformArtifactRelease    `json:"release"`
	Sequence   int64                            `json:"sequence"`
	VerifiedAt time.Time                        `json:"verified_at"`
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
	if active, known := s.servingActiveForHeartbeat(); known && !active {
		return nil
	}
	client := platformconsumer.Client{BaseURL: s.Config.APIURL, TokenFile: s.PlatformTokenFile, HTTPClient: s.HTTPClient}
	id, assignment, artifact, release, err := client.Sync(ctx, model.PlatformConsumerComponentEdgeWorker, s.Config.EdgeID, "global", model.PlatformArtifactKindEdgeRouteBundle)
	if err != nil {
		return err
	}
	if artifact.ID != assignment.ArtifactID || artifact.Status != model.PlatformArtifactStatusValidated || artifact.ContentHash != assignment.ContentHash || artifact.Generation != assignment.ExpectedGeneration || release.ID != assignment.ArtifactReleaseID || release.ArtifactID != assignment.ReleaseSetID || release.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow || release.Status != model.PlatformArtifactReleaseStatusActive || release.FencingToken != assignment.FencingToken || artifact.Metadata["release_set_generation"] != release.Generation {
		return errors.New("edge platform candidate binding mismatch")
	}
	if !platformsafety.EvaluateArtifactIntegrity(artifact, bundleauth.NewKeyring(s.Config.BundleSigningKey, s.Config.BundleSigningKeyID, s.Config.BundleSigningPreviousKey, s.Config.BundleSigningPreviousKeyID, s.Config.BundleRevokedKeyIDs)).Pass {
		return errors.New("edge platform candidate signature or digest rejected")
	}
	var payload struct {
		Schema     string                        `json:"schema_version"`
		Generation string                        `json:"generation"`
		Routes     []platformconfig.RouteIntent  `json:"routes"`
		Policy     platformconfig.PolicySnapshot `json:"policy"`
		Lineage    platformconfig.Lineage        `json:"lineage"`
	}
	raw, _ := json.Marshal(artifact.Content)
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&payload) != nil || payload.Schema != platformconfig.SchemaVersion || payload.Generation != artifact.Metadata["intent_generation"] || len(payload.Routes) == 0 || platformconfig.ValidatePolicySnapshot(payload.Policy) != nil {
		return errors.New("edge platform candidate schema invalid")
	}
	path := s.Config.CachePath + ".platform-shadow.json"
	var prev edgePlatformCandidate
	if b, e := os.ReadFile(path); e == nil {
		if json.Unmarshal(b, &prev) != nil || prev.Sequence <= 0 {
			return errors.New("edge platform candidate cursor is corrupt")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return errors.New("edge platform candidate cursor unavailable")
	}
	c := edgePlatformCandidate{Artifact: artifact, Assignment: assignment, Release: release, Sequence: prev.Sequence + 1, VerifiedAt: time.Now().UTC()}
	if c.Sequence <= 0 {
		c.Sequence = time.Now().UnixNano()
	}
	b, _ := json.Marshal(c)
	if err := lkgcache.AtomicWriteFile(path, b, 0600); err != nil {
		return errors.New("persist edge platform candidate failed")
	}
	status := s.Status()
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ConsumerID: id.Component + ":" + id.NodeID, Component: id.Component, NodeID: id.NodeID, ArtifactKind: assignment.ArtifactKind, ScopeKey: assignment.ScopeKey, ReleaseSetID: assignment.ReleaseSetID, ExpectedConsumerSetID: assignment.ExpectedConsumerSetID, FencingToken: assignment.FencingToken, ProtocolVersion: model.PlatformConsumerProtocolVersionV1, SchemaVersion: model.PlatformConsumerSchemaVersionV1, Sequence: c.Sequence, IssuedAt: time.Now().UTC(), Nonce: hex.EncodeToString(nonce), GenerationSequence: assignment.GenerationSequence, DesiredGeneration: assignment.ExpectedGeneration, ActualGeneration: status.ServingGeneration, LKGGeneration: status.LKGGeneration, ApplyStatus: "staged", ProbeStatus: "shadow_validated", ServingLKG: status.StaleCache, LKGExpired: status.MaxStaleExceeded}
	h.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
	if err != nil {
		return err
	}
	var receipt model.PlatformConsumerHeartbeatResponse
	if err = client.PostJSON(ctx, "/v1/platform-state/consumers/trusted-heartbeat", id.Token, h, &receipt); err != nil {
		return err
	}
	if !receipt.Consumer.IdentityVerified || receipt.Consumer.ConsumerID != h.ConsumerID || receipt.Consumer.Sequence != h.Sequence || receipt.Consumer.ExpectedConsumerSetID != h.ExpectedConsumerSetID || receipt.Consumer.EvidenceHash != h.EvidenceHash {
		return errors.New("edge platform heartbeat receipt mismatch")
	}
	s.mu.Lock()
	s.platformCandidate = PlatformCandidateStatus{State: "shadow_verified", ArtifactID: assignment.ArtifactID, Digest: assignment.ContentHash, ReleaseSetID: assignment.ReleaseSetID, RouteCount: len(payload.Routes), Sequence: c.Sequence, VerifiedAt: c.VerifiedAt, ReportedAt: time.Now().UTC()}
	s.mu.Unlock()
	s.Logger.Printf("edge platform candidate verified; artifact=%s digest=%s routes=%d sequence=%d serving=%s", assignment.ArtifactID, assignment.ContentHash, len(payload.Routes), c.Sequence, status.ServingGeneration)
	return nil
}

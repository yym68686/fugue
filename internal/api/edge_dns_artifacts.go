package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/store"
)

const (
	edgeDNSArtifactControllerInterval = time.Minute
	edgeDNSArtifactControllerLockName = "edge-dns-artifact-controller"
	edgeDNSArtifactFreshMargin        = 30 * time.Second
)

var errEdgeDNSArtifactParityMismatch = errors.New("immutable and compatibility DNS artifact digests differ")

type edgeDNSArtifactPublicationStats struct {
	Artifacts            int
	SourceSnapshots      int
	NodeProjections      int
	RouteCompilations    int
	LegacyDerivations    int
	ImmutableWrites      int
	LegacyWrites         int
	ParityMismatches     int
	ShadowActive         int
	VerifiedLKG          int
	FullActive           int
	VerificationDeferred int
}

func (s *Server) StartBackgroundEdgeDNSArtifacts(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	s.runEdgeDNSArtifactController(ctx, time.Now().UTC())
	timer := time.NewTicker(edgeDNSArtifactControllerInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-timer.C:
			s.runEdgeDNSArtifactController(ctx, now.UTC())
		}
	}
}

func (s *Server) runEdgeDNSArtifactController(ctx context.Context, now time.Time) {
	started := time.Now().UTC()
	publication := edgeDNSArtifactPublicationStats{}
	decisionCount := 0
	acquired := true
	var err error
	if s.store != nil {
		acquired, err = s.store.WithAdvisoryLock(ctx, edgeDNSArtifactControllerLockName, func() error {
			var runErr error
			publication, decisionCount, runErr = s.rebuildEdgeDNSArtifacts(ctx, now)
			return runErr
		})
	} else {
		publication, decisionCount, err = s.rebuildEdgeDNSArtifacts(ctx, now)
	}
	duration := time.Since(started)

	s.edgeDNSArtifactMu.Lock()
	if !acquired {
		s.edgeDNSArtifactSkippedCount++
		s.edgeDNSArtifactMu.Unlock()
		if s.log != nil {
			s.log.Printf("edge dns artifact controller skipped: another writer holds lock")
		}
		return
	}
	s.edgeDNSArtifactLastRun = started
	s.edgeDNSArtifactLastDuration = duration
	s.edgeDNSArtifactLastCount = publication.Artifacts
	s.edgeDNSArtifactLastDecisions = decisionCount
	s.edgeDNSArtifactLastSourceSnapshots = publication.SourceSnapshots
	s.edgeDNSArtifactLastNodeProjections = publication.NodeProjections
	s.edgeDNSArtifactLastRouteCompilations = publication.RouteCompilations
	s.edgeDNSArtifactLastLegacyDerivations = publication.LegacyDerivations
	s.edgeDNSArtifactLastImmutableWrites = publication.ImmutableWrites
	s.edgeDNSArtifactLastLegacyWrites = publication.LegacyWrites
	s.edgeDNSArtifactLastParityMismatches = publication.ParityMismatches
	s.edgeDNSArtifactLastShadowActive = publication.ShadowActive
	s.edgeDNSArtifactLastVerifiedLKG = publication.VerifiedLKG
	s.edgeDNSArtifactLastFullActive = publication.FullActive
	s.edgeDNSArtifactLastVerifyDeferred = publication.VerificationDeferred
	s.edgeDNSArtifactRunCount++
	if err != nil {
		s.edgeDNSArtifactLastError = err.Error()
		s.edgeDNSArtifactErrorCount++
	} else {
		s.edgeDNSArtifactLastError = ""
		s.edgeDNSArtifactLastSuccess = time.Now().UTC()
	}
	s.edgeDNSArtifactMu.Unlock()

	if err != nil {
		if s.log != nil {
			s.log.Printf("edge dns artifact controller failed: artifacts=%d snapshots=%d projections=%d route_compilations=%d legacy_derivations=%d immutable_writes=%d legacy_writes=%d parity_mismatches=%d shadow_active=%d verified_lkg=%d full_active=%d verification_deferred=%d decisions=%d duration=%s err=%v", publication.Artifacts, publication.SourceSnapshots, publication.NodeProjections, publication.RouteCompilations, publication.LegacyDerivations, publication.ImmutableWrites, publication.LegacyWrites, publication.ParityMismatches, publication.ShadowActive, publication.VerifiedLKG, publication.FullActive, publication.VerificationDeferred, decisionCount, duration, err)
		}
		return
	}
	if s.log != nil {
		s.log.Printf("edge dns artifact controller complete: artifacts=%d snapshots=%d projections=%d route_compilations=%d legacy_derivations=%d immutable_writes=%d legacy_writes=%d parity_mismatches=%d shadow_active=%d verified_lkg=%d full_active=%d verification_deferred=%d decisions=%d duration=%s", publication.Artifacts, publication.SourceSnapshots, publication.NodeProjections, publication.RouteCompilations, publication.LegacyDerivations, publication.ImmutableWrites, publication.LegacyWrites, publication.ParityMismatches, publication.ShadowActive, publication.VerifiedLKG, publication.FullActive, publication.VerificationDeferred, decisionCount, duration)
	}
}

func (s *Server) rebuildEdgeDNSArtifacts(ctx context.Context, now time.Time) (edgeDNSArtifactPublicationStats, int, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if updated, err := s.reconcileHostedDNSFlattenRecords(ctx, now); err != nil {
		if s.log != nil {
			s.log.Printf("hosted dns flatten resolver failed; continuing with previous cached answers: updated=%d err=%v", updated, err)
		}
	} else if updated > 0 && s.log != nil {
		s.log.Printf("hosted dns flatten resolver updated %d records", updated)
	}
	decisionCount, err := s.reconcileEdgeDNSRoutingDecisions(now)
	if err != nil {
		return edgeDNSArtifactPublicationStats{}, decisionCount, fmt.Errorf("reconcile edge dns routing decisions: %w", err)
	}
	publication, err := s.publishEdgeDNSBundleArtifacts(ctx, now)
	if err != nil {
		return publication, decisionCount, err
	}
	return publication, decisionCount, nil
}

func (s *Server) publishEdgeDNSBundleArtifacts(ctx context.Context, now time.Time) (edgeDNSArtifactPublicationStats, error) {
	stats := edgeDNSArtifactPublicationStats{}
	if s == nil || s.store == nil {
		return stats, nil
	}
	nodes, err := s.store.ListDNSNodes("")
	if err != nil {
		return stats, fmt.Errorf("list dns nodes for artifact publication: %w", err)
	}
	nodes = freshDNSNodes(nodes, now)
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].EdgeGroupID != nodes[j].EdgeGroupID {
			return nodes[i].EdgeGroupID < nodes[j].EdgeGroupID
		}
		return nodes[i].ID < nodes[j].ID
	})

	type publicationTarget struct {
		node    model.DNSNode
		options edgeDNSBundleOptions
	}
	targets := make([]publicationTarget, 0, len(nodes))
	zones := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if !edgeDNSArtifactNodePublishable(node) {
			continue
		}
		options, ok := s.edgeDNSBundleOptionsForDNSNode(node)
		if !ok {
			continue
		}
		targets = append(targets, publicationTarget{node: node, options: options})
		zones = append(zones, options.Zone)
	}
	if len(targets) == 0 {
		return stats, nil
	}
	snapshot, err := s.loadEdgeDNSBundleCompileSnapshot(ctx, zones, now)
	if err != nil {
		return stats, fmt.Errorf("load canonical edge DNS compile snapshot: %w", err)
	}
	stats.SourceSnapshots = 1
	for _, target := range targets {
		reconciliation, err := s.reconcileEdgeDNSArtifactRelease(target.node, target.options, now)
		if err != nil {
			return stats, fmt.Errorf("reconcile immutable edge dns release for node %s: %w", target.node.ID, err)
		}
		if reconciliation.VerifiedLKG {
			stats.VerifiedLKG++
		}
		if reconciliation.VerificationDeferred {
			stats.VerificationDeferred++
		}
		bundle, err := s.compileEdgeDNSBundle(ctx, target.options, snapshot)
		if err != nil {
			stats.RouteCompilations = len(snapshot.routeBindingByCompilationKey)
			return stats, fmt.Errorf("project edge dns artifact for node %s: %w", target.node.ID, err)
		}
		stats.NodeProjections++
		artifact := newEdgeDNSBundleArtifact(target.options, bundle, now)
		platformArtifact, err := s.publishEdgeDNSBundleArtifact(artifact, target.options, now)
		if err != nil {
			if errors.Is(err, errEdgeDNSArtifactParityMismatch) {
				stats.ParityMismatches++
			}
			stats.RouteCompilations = len(snapshot.routeBindingByCompilationKey)
			return stats, fmt.Errorf("publish edge dns artifact for node %s: %w", target.node.ID, err)
		}
		stats.ImmutableWrites++
		stats.ShadowActive++
		stats.LegacyWrites++
		if reconciliation.ReadyForFull {
			if err := s.releaseFullEdgeDNSBundleArtifact(platformArtifact); err != nil {
				stats.RouteCompilations = len(snapshot.routeBindingByCompilationKey)
				return stats, fmt.Errorf("activate immutable edge dns full current for node %s: %w", target.node.ID, err)
			}
			stats.FullActive++
		}
		stats.Artifacts++
	}
	stats.RouteCompilations = len(snapshot.routeBindingByCompilationKey)
	return stats, nil
}

func (s *Server) publishEdgeDNSBundleArtifact(artifact store.EdgeDNSBundleArtifact, options edgeDNSBundleOptions, now time.Time) (model.PlatformArtifact, error) {
	if err := s.validateEdgeDNSBundleArtifact(artifact, options, now); err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("validate before publication: %w", err)
	}
	platformArtifact, err := s.publishImmutableEdgeDNSBundleArtifact(artifact)
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("publish immutable generation: %w", err)
	}
	if err := s.store.UpsertEdgeDNSBundleArtifact(artifact); err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("publish compatibility row: %w", err)
	}
	stored, err := s.store.GetEdgeDNSBundleArtifact(artifact.ScopeKey)
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("read compatibility row: %w", err)
	}
	if !edgeDNSBundleArtifactsEquivalent(artifact, stored) {
		return model.PlatformArtifact{}, errEdgeDNSArtifactParityMismatch
	}
	return platformArtifact, nil
}

type edgeDNSImmutableArtifactContent struct {
	ScopeKey          string              `json:"scope_key"`
	Zone              string              `json:"zone"`
	DNSNodeID         string              `json:"dns_node_id"`
	EdgeGroupID       string              `json:"edge_group_id"`
	AnswerIPs         []string            `json:"answer_ips"`
	RouteAAnswerIPs   []string            `json:"route_a_answer_ips,omitempty"`
	Version           string              `json:"version"`
	ETag              string              `json:"etag"`
	SourceFingerprint string              `json:"source_fingerprint"`
	Bundle            model.EdgeDNSBundle `json:"bundle"`
	GeneratedAt       time.Time           `json:"generated_at"`
	ValidUntil        time.Time           `json:"valid_until,omitempty"`
}

func (s *Server) publishImmutableEdgeDNSBundleArtifact(legacy store.EdgeDNSBundleArtifact) (model.PlatformArtifact, error) {
	content, err := edgeDNSImmutableArtifactContentMap(legacy)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	generation, err := edgeDNSImmutablePlatformGeneration(content)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	artifact, _, err := s.store.EnsurePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle,
		Scope: model.PlatformArtifactScope{
			ScopeType:   "dns-node",
			Key:         legacy.ScopeKey,
			NodeID:      legacy.DNSNodeID,
			EdgeGroupID: legacy.EdgeGroupID,
		},
		Generation:         generation,
		Content:            content,
		CompatibilityFloor: model.PlatformArtifactSchemaVersionV1,
		CreatedByType:      model.ActorTypeSystem,
		CreatedByID:        "edge-dns-artifact-controller",
		CreatedAt:          legacy.GeneratedAt,
	})
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("ensure immutable generation: %w", err)
	}
	artifact, err = s.store.ValidatePlatformArtifact(artifact.ID, []model.PlatformArtifactValidationResult{{
		Name:     "edge_dns_bundle_integrity",
		Pass:     true,
		Severity: model.RobustnessSeverityInfo,
		Message:  "signed DNS bundle and node scope passed activation validation",
		Evidence: map[string]string{
			"dns_envelope_generation": strings.TrimSpace(legacy.Bundle.Generation),
			"source_fingerprint":      legacy.SourceFingerprint,
		},
	}})
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("validate immutable generation: %w", err)
	}
	active, _, _, _, err := s.store.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		Reason:         "mirror validated DNS bundle into immutable artifact ledger",
		IdempotencyKey: "edge-dns-shadow:" + legacy.ScopeKey + ":" + generation,
	}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "edge-dns-artifact-controller"})
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("activate immutable shadow pointer: %w", err)
	}
	projected, err := edgeDNSBundleArtifactFromPlatformArtifact(active)
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("decode immutable shadow artifact: %w", err)
	}
	if !edgeDNSBundleArtifactsEquivalent(legacy, projected) {
		return model.PlatformArtifact{}, errors.New("immutable shadow artifact digest differs from source")
	}
	return active, nil
}

type edgeDNSArtifactReleaseReconciliation struct {
	VerifiedLKG          bool
	ReadyForFull         bool
	VerificationDeferred bool
}

func (s *Server) reconcileEdgeDNSArtifactRelease(node model.DNSNode, options edgeDNSBundleOptions, now time.Time) (edgeDNSArtifactReleaseReconciliation, error) {
	result := edgeDNSArtifactReleaseReconciliation{}
	scopeKey := edgeDNSBundleArtifactScopeKey(options)
	lkg, err := s.store.GetPlatformLKG(model.PlatformArtifactKindDNSAnswerBundle, scopeKey)
	if err != nil {
		return result, fmt.Errorf("load verified LKG: %w", err)
	}
	if lkg != nil && !lkg.ExpiresAt.IsZero() && !now.UTC().Before(lkg.ExpiresAt.UTC()) {
		lkg = nil
	}
	fullArtifact, fullRelease, fullFound, err := s.store.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		scopeKey,
		model.PlatformArtifactReleaseChannelFull,
	)
	if err != nil {
		return result, fmt.Errorf("load full current: %w", err)
	}
	if fullFound {
		if lkg == nil {
			return s.reconcileInitialEdgeDNSArtifactLKG(node, options, now)
		}
		if fullRelease.VerificationState == model.PlatformArtifactVerificationStateVerified {
			if lkg.ArtifactID != fullArtifact.ID || lkg.Generation != fullArtifact.Generation {
				return result, errors.New("verified full current does not match the verified LKG")
			}
			result.VerifiedLKG = true
			result.ReadyForFull = true
			return result, nil
		}
		result.VerifiedLKG = lkg != nil
		verified, err := s.verifyObservedEdgeDNSArtifactRelease(fullArtifact, fullRelease, node, options, lkg, now)
		if err != nil {
			return result, err
		}
		if verified {
			result.VerifiedLKG = true
			result.ReadyForFull = true
		} else {
			result.VerificationDeferred = true
		}
		return result, nil
	}
	if lkg != nil {
		result.VerifiedLKG = true
		result.ReadyForFull = true
		return result, nil
	}
	return s.reconcileInitialEdgeDNSArtifactLKG(node, options, now)
}

func (s *Server) reconcileInitialEdgeDNSArtifactLKG(node model.DNSNode, options edgeDNSBundleOptions, now time.Time) (edgeDNSArtifactReleaseReconciliation, error) {
	result := edgeDNSArtifactReleaseReconciliation{}
	shadowArtifact, shadowRelease, shadowFound, err := s.store.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		edgeDNSBundleArtifactScopeKey(options),
		model.PlatformArtifactReleaseChannelShadow,
	)
	if err != nil {
		return result, fmt.Errorf("load shadow current: %w", err)
	}
	if !shadowFound {
		return result, nil
	}
	verified, err := s.verifyObservedEdgeDNSArtifactRelease(shadowArtifact, shadowRelease, node, options, nil, now)
	if err != nil {
		return result, err
	}
	if verified {
		result.VerifiedLKG = true
		result.ReadyForFull = true
	} else {
		result.VerificationDeferred = true
	}
	return result, nil
}

func (s *Server) verifyObservedEdgeDNSArtifactRelease(artifact model.PlatformArtifact, release model.PlatformArtifactRelease, node model.DNSNode, options edgeDNSBundleOptions, lkg *model.PlatformLKGSnapshot, now time.Time) (bool, error) {
	projected, err := edgeDNSBundleArtifactFromPlatformArtifact(artifact)
	if err != nil {
		return false, fmt.Errorf("decode release artifact: %w", err)
	}
	// Older bundles reused the semantic version as their generation, so a node
	// heartbeat could not identify the immutable envelope it actually loaded.
	if !strings.HasPrefix(strings.TrimSpace(projected.Bundle.Generation), "dnsenv_") || projected.Bundle.Generation == projected.Bundle.Version {
		return false, nil
	}
	activated := projected
	activated.ActivatedAt = release.ReleasedAt
	activated.UpdatedAt = release.UpdatedAt
	if err := s.validateEdgeDNSBundleArtifact(activated, options, now); err != nil {
		return false, nil
	}
	compatibility, err := s.store.GetEdgeDNSBundleArtifact(artifact.ScopeKey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("load compatibility current for verification: %w", err)
	}
	databaseRollbackCompatible := edgeDNSBundleArtifactsEquivalent(projected, compatibility)
	heartbeatAt := node.LastHeartbeatAt
	watchWindow := heartbeatAt != nil && !heartbeatAt.IsZero() && heartbeatAt.UTC().After(release.ReleasedAt.UTC())
	consumerConvergence := strings.TrimSpace(node.DNSBundleVersion) == strings.TrimSpace(projected.Bundle.Version) &&
		strings.TrimSpace(node.ServingGeneration) == strings.TrimSpace(projected.Bundle.Generation) &&
		strings.TrimSpace(node.LKGGeneration) == strings.TrimSpace(projected.Bundle.Generation) &&
		node.RecordCount == len(projected.Bundle.Records)
	localProbe := dnsNodeHeartbeatFresh(node, now) && dnsNodeServingHealthOK(node) &&
		strings.EqualFold(strings.TrimSpace(node.CacheStatus), "ready") && node.UDPListen && node.TCPListen && strings.TrimSpace(node.LastError) == ""
	baselineMonotonic := lkg == nil || artifact.GenerationSequence > lkg.GenerationSequence
	if !consumerConvergence || !localProbe || !watchWindow || !baselineMonotonic || !databaseRollbackCompatible {
		return false, nil
	}
	_, _, _, verifiedLKG, err := s.store.VerifyPlatformArtifactReleaseLKG(release.ID, model.PlatformArtifactVerifyLKGRequest{
		FencingToken:    release.FencingToken,
		Reason:          "DNS node served the exact signed immutable envelope after publication",
		AllowInitialLKG: lkg == nil,
		Evidence: model.PlatformArtifactVerificationEvidence{
			ConsumerConvergence:        true,
			LocalProbe:                 true,
			PlatformEvidence:           true,
			WatchWindow:                true,
			BaselineMonotonic:          true,
			DatabaseRollbackCompatible: true,
			ExpectedConsumerSetID:      "dns-node:" + strings.TrimSpace(node.ID),
			EvidenceRefs: []string{
				"dns-heartbeat:" + strings.TrimSpace(node.ID) + ":" + heartbeatAt.UTC().Format(time.RFC3339Nano),
				"dns-envelope:" + strings.TrimSpace(projected.Bundle.Generation),
				"platform-artifact:" + artifact.ID,
				"platform-release:" + release.ID,
			},
		},
	}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "edge-dns-artifact-controller"})
	if err != nil {
		return false, fmt.Errorf("verify immutable release LKG: %w", err)
	}
	if verifiedLKG == nil || verifiedLKG.ArtifactID != artifact.ID || verifiedLKG.Generation != artifact.Generation {
		return false, errors.New("verified LKG does not match the observed immutable artifact")
	}
	return true, nil
}

func (s *Server) releaseFullEdgeDNSBundleArtifact(artifact model.PlatformArtifact) error {
	active, release, _, _, err := s.store.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		Reason:         "activate DNS answer bundle with a pinned verified per-node rollback generation",
		IdempotencyKey: "edge-dns-full:" + artifact.ScopeKey + ":" + artifact.Generation,
	}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "edge-dns-artifact-controller"})
	if err != nil {
		return err
	}
	if active.ID != artifact.ID || release.Status != model.PlatformArtifactReleaseStatusActive || release.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || release.PinnedRollbackGeneration == "" {
		return errors.New("full current did not retain the requested artifact and pinned rollback generation")
	}
	return nil
}

func edgeDNSImmutablePlatformGeneration(content map[string]any) (string, error) {
	raw, err := json.Marshal(content)
	if err != nil {
		return "", fmt.Errorf("marshal immutable edge DNS artifact generation: %w", err)
	}
	sum := sha256.Sum256(raw)
	return "dns_content_sha256_" + hex.EncodeToString(sum[:]), nil
}

func edgeDNSImmutableArtifactContentMap(artifact store.EdgeDNSBundleArtifact) (map[string]any, error) {
	content := edgeDNSImmutableArtifactContent{
		ScopeKey:          strings.TrimSpace(artifact.ScopeKey),
		Zone:              normalizeExternalAppDomain(artifact.Zone),
		DNSNodeID:         strings.TrimSpace(artifact.DNSNodeID),
		EdgeGroupID:       strings.TrimSpace(artifact.EdgeGroupID),
		AnswerIPs:         uniqueSortedStrings(artifact.AnswerIPs),
		RouteAAnswerIPs:   uniqueSortedStrings(artifact.RouteAAnswerIPs),
		Version:           strings.TrimSpace(artifact.Version),
		ETag:              strings.TrimSpace(artifact.ETag),
		SourceFingerprint: strings.TrimSpace(artifact.SourceFingerprint),
		Bundle:            artifact.Bundle,
		GeneratedAt:       canonicalEdgeDNSArtifactTime(artifact.GeneratedAt),
		ValidUntil:        canonicalEdgeDNSArtifactTime(artifact.ValidUntil),
	}
	raw, err := json.Marshal(content)
	if err != nil {
		return nil, fmt.Errorf("marshal immutable edge DNS artifact content: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("canonicalize immutable edge DNS artifact content: %w", err)
	}
	return out, nil
}

func canonicalEdgeDNSArtifactTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().Truncate(time.Microsecond)
}

func edgeDNSBundleArtifactFromPlatformArtifact(artifact model.PlatformArtifact) (store.EdgeDNSBundleArtifact, error) {
	if artifact.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || artifact.Content == nil {
		return store.EdgeDNSBundleArtifact{}, errors.New("platform artifact is not a DNS answer bundle")
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return store.EdgeDNSBundleArtifact{}, fmt.Errorf("marshal platform DNS artifact: %w", err)
	}
	var content edgeDNSImmutableArtifactContent
	if err := json.Unmarshal(raw, &content); err != nil {
		return store.EdgeDNSBundleArtifact{}, fmt.Errorf("decode platform DNS artifact: %w", err)
	}
	if strings.TrimSpace(content.ScopeKey) == "" || strings.TrimSpace(content.Version) == "" || strings.TrimSpace(content.Bundle.Version) == "" {
		return store.EdgeDNSBundleArtifact{}, errors.New("platform DNS artifact content is incomplete")
	}
	return store.EdgeDNSBundleArtifact{
		ScopeKey:          content.ScopeKey,
		Zone:              content.Zone,
		DNSNodeID:         content.DNSNodeID,
		EdgeGroupID:       content.EdgeGroupID,
		AnswerIPs:         content.AnswerIPs,
		RouteAAnswerIPs:   content.RouteAAnswerIPs,
		Version:           content.Version,
		ETag:              content.ETag,
		SourceFingerprint: content.SourceFingerprint,
		Bundle:            content.Bundle,
		GeneratedAt:       content.GeneratedAt,
		ValidUntil:        content.ValidUntil,
	}, nil
}

func edgeDNSBundleArtifactsEquivalent(left, right store.EdgeDNSBundleArtifact) bool {
	leftContent, leftErr := edgeDNSImmutableArtifactContentMap(left)
	rightContent, rightErr := edgeDNSImmutableArtifactContentMap(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	leftJSON, leftErr := json.Marshal(leftContent)
	rightJSON, rightErr := json.Marshal(rightContent)
	return leftErr == nil && rightErr == nil && slices.Equal(leftJSON, rightJSON)
}

func edgeDNSArtifactNodePublishable(node model.DNSNode) bool {
	if !node.Healthy {
		return false
	}
	switch model.NormalizeEdgeHealthStatus(node.Status) {
	case model.EdgeHealthHealthy, model.EdgeHealthDegraded:
		return true
	default:
		return false
	}
}

func (s *Server) edgeDNSBundleOptionsForDNSNode(node model.DNSNode) (edgeDNSBundleOptions, bool) {
	zone := normalizeExternalAppDomain(node.Zone)
	if zone == "" {
		zone = normalizeExternalAppDomain(s.customDomainBaseDomain)
	}
	answerIPs := []string{}
	answerIPs = appendEdgeDNSUniqueIP(answerIPs, node.PublicIPv4)
	answerIPs = appendEdgeDNSUniqueIP(answerIPs, node.PublicIPv6)
	if zone == "" || len(answerIPs) == 0 {
		return edgeDNSBundleOptions{}, false
	}
	ttl := s.dnsBundleTTL
	if ttl <= 0 || ttl > 3600 {
		ttl = defaultEdgeDNSTTL
	}
	return edgeDNSBundleOptions{
		DNSNodeID:       strings.TrimSpace(node.ID),
		EdgeGroupID:     strings.TrimSpace(node.EdgeGroupID),
		Zone:            zone,
		AnswerIPs:       answerIPs,
		RouteAAnswerIPs: append([]string(nil), s.dnsRouteAAnswerIPs...),
		TTL:             ttl,
	}, true
}

func (s *Server) edgeDNSBundleArtifactForOptions(options edgeDNSBundleOptions, now time.Time) (model.EdgeDNSBundle, bool, error) {
	if s == nil || s.store == nil {
		return model.EdgeDNSBundle{}, false, nil
	}
	artifact, err := s.store.GetEdgeDNSBundleArtifact(edgeDNSBundleArtifactScopeKey(options))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return model.EdgeDNSBundle{}, false, nil
		}
		return model.EdgeDNSBundle{}, false, err
	}
	if err := s.validateEdgeDNSBundleArtifact(artifact, options, now); err != nil {
		return model.EdgeDNSBundle{}, false, fmt.Errorf("validate activated edge DNS artifact: %w", err)
	}
	return artifact.Bundle, true, nil
}

func newEdgeDNSBundleArtifact(options edgeDNSBundleOptions, bundle model.EdgeDNSBundle, activatedAt time.Time) store.EdgeDNSBundleArtifact {
	return store.EdgeDNSBundleArtifact{
		ScopeKey:          edgeDNSBundleArtifactScopeKey(options),
		Zone:              options.Zone,
		DNSNodeID:         options.DNSNodeID,
		EdgeGroupID:       options.EdgeGroupID,
		AnswerIPs:         options.AnswerIPs,
		RouteAAnswerIPs:   options.RouteAAnswerIPs,
		Version:           bundle.Version,
		ETag:              edgeRouteBundleETag(bundle.Version),
		SourceFingerprint: edgeDNSBundleArtifactSourceFingerprint(options, bundle),
		Bundle:            bundle,
		GeneratedAt:       bundle.GeneratedAt,
		ValidUntil:        bundle.ValidUntil,
		ActivatedAt:       activatedAt,
		UpdatedAt:         activatedAt,
	}
}

func (s *Server) validateEdgeDNSBundleArtifact(artifact store.EdgeDNSBundleArtifact, options edgeDNSBundleOptions, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	expectedScopeKey := edgeDNSBundleArtifactScopeKey(options)
	if strings.TrimSpace(artifact.ScopeKey) != expectedScopeKey {
		return errors.New("scope key does not match the request")
	}
	if artifact.ActivatedAt.IsZero() {
		return errors.New("artifact is not activated")
	}
	bundle := artifact.Bundle
	if strings.TrimSpace(artifact.Version) == "" || strings.TrimSpace(artifact.Version) != strings.TrimSpace(bundle.Version) ||
		strings.TrimSpace(bundle.Generation) == "" {
		return errors.New("artifact version identity is inconsistent")
	}
	if strings.TrimSpace(artifact.ETag) != edgeRouteBundleETag(bundle.Version) {
		return errors.New("artifact ETag does not match its version")
	}
	if strings.TrimSpace(artifact.SourceFingerprint) != edgeDNSBundleArtifactSourceFingerprint(options, bundle) {
		return errors.New("artifact source fingerprint does not match the request")
	}
	if normalizeExternalAppDomain(artifact.Zone) != normalizeExternalAppDomain(options.Zone) ||
		strings.TrimSpace(artifact.DNSNodeID) != strings.TrimSpace(options.DNSNodeID) ||
		strings.TrimSpace(artifact.EdgeGroupID) != strings.TrimSpace(options.EdgeGroupID) ||
		!slices.Equal(uniqueSortedStrings(artifact.AnswerIPs), uniqueSortedStrings(options.AnswerIPs)) ||
		!slices.Equal(uniqueSortedStrings(artifact.RouteAAnswerIPs), uniqueSortedStrings(options.RouteAAnswerIPs)) {
		return errors.New("artifact scope material does not match the request")
	}
	if normalizeExternalAppDomain(bundle.Zone) != normalizeExternalAppDomain(options.Zone) ||
		strings.TrimSpace(bundle.DNSNodeID) != strings.TrimSpace(options.DNSNodeID) ||
		strings.TrimSpace(bundle.EdgeGroupID) != strings.TrimSpace(options.EdgeGroupID) {
		return errors.New("bundle identity does not match the request")
	}
	if !edgeDNSArtifactTimesMatch(artifact.GeneratedAt, bundle.GeneratedAt) ||
		!edgeDNSArtifactTimesMatch(artifact.ValidUntil, bundle.ValidUntil) {
		return errors.New("artifact validity metadata does not match the bundle")
	}
	if len(bundle.Records) == 0 {
		return errors.New("artifact contains no DNS records")
	}
	if !edgeDNSBundleHasSignature(bundle) {
		return bundleauth.ErrMissingSignature
	}
	if err := bundleauth.VerifyEdgeDNSBundleWithKeyring(bundle, s.bundleKeyring(), now); err != nil {
		return fmt.Errorf("verify bundle signature: %w", err)
	}
	if !edgeDNSBundleArtifactFresh(artifact, now) {
		return errors.New("artifact is not fresh enough to activate")
	}
	return nil
}

func edgeDNSBundleHasSignature(bundle model.EdgeDNSBundle) bool {
	if strings.TrimSpace(bundle.KeyID) != "" && strings.TrimSpace(bundle.Signature) != "" {
		return true
	}
	for _, signature := range bundle.Signatures {
		if strings.TrimSpace(signature.KeyID) != "" && strings.TrimSpace(signature.Signature) != "" {
			return true
		}
	}
	return false
}

func edgeDNSArtifactTimesMatch(stored, bundled time.Time) bool {
	if stored.IsZero() || bundled.IsZero() {
		return false
	}
	delta := stored.Sub(bundled)
	if delta < 0 {
		delta = -delta
	}
	return delta <= time.Microsecond
}

func edgeDNSBundleArtifactFresh(artifact store.EdgeDNSBundleArtifact, now time.Time) bool {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if strings.TrimSpace(artifact.Bundle.Version) == "" || strings.TrimSpace(artifact.Version) != strings.TrimSpace(artifact.Bundle.Version) {
		return false
	}
	validUntil := firstNonZeroTime(artifact.ValidUntil, artifact.Bundle.ValidUntil)
	if validUntil.IsZero() {
		return true
	}
	return now.Add(edgeDNSArtifactFreshMargin).Before(validUntil)
}

func (s *Server) recordEdgeDNSArtifactHandlerLookup(hit bool, err error) {
	if s == nil {
		return
	}
	s.edgeDNSArtifactMu.Lock()
	defer s.edgeDNSArtifactMu.Unlock()
	switch {
	case err != nil:
		s.edgeDNSArtifactHandlerLookupErrorCount++
	case hit:
		s.edgeDNSArtifactHandlerLookupHitCount++
	default:
		s.edgeDNSArtifactHandlerLookupMissCount++
	}
}

type edgeDNSBundleArtifactScopeMaterial struct {
	DNSNodeID       string   `json:"dns_node_id,omitempty"`
	EdgeGroupID     string   `json:"edge_group_id,omitempty"`
	Zone            string   `json:"zone"`
	AnswerIPs       []string `json:"answer_ips"`
	RouteAAnswerIPs []string `json:"route_a_answer_ips,omitempty"`
	TTL             int      `json:"ttl"`
}

func edgeDNSBundleArtifactScopeKey(options edgeDNSBundleOptions) string {
	material := edgeDNSBundleArtifactScopeMaterial{
		DNSNodeID:       strings.TrimSpace(options.DNSNodeID),
		EdgeGroupID:     strings.TrimSpace(options.EdgeGroupID),
		Zone:            normalizeExternalAppDomain(options.Zone),
		AnswerIPs:       uniqueSortedStrings(options.AnswerIPs),
		RouteAAnswerIPs: uniqueSortedStrings(options.RouteAAnswerIPs),
		TTL:             options.TTL,
	}
	if material.TTL <= 0 {
		material.TTL = defaultEdgeDNSTTL
	}
	payload, _ := json.Marshal(material)
	sum := sha256.Sum256(payload)
	return "edge_dns_bundle:" + hex.EncodeToString(sum[:])
}

func edgeDNSBundleArtifactSourceFingerprint(options edgeDNSBundleOptions, bundle model.EdgeDNSBundle) string {
	material := struct {
		ScopeKey string `json:"scope_key"`
		Version  string `json:"version"`
		KeyID    string `json:"key_id,omitempty"`
	}{
		ScopeKey: edgeDNSBundleArtifactScopeKey(options),
		Version:  strings.TrimSpace(bundle.Version),
		KeyID:    strings.TrimSpace(bundle.KeyID),
	}
	payload, _ := json.Marshal(material)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (s *Server) writeEdgeDNSArtifactMetrics(w io.Writer) {
	s.edgeDNSArtifactMu.Lock()
	lastRun := s.edgeDNSArtifactLastRun
	lastSuccess := s.edgeDNSArtifactLastSuccess
	lastDuration := s.edgeDNSArtifactLastDuration
	lastCount := s.edgeDNSArtifactLastCount
	lastDecisions := s.edgeDNSArtifactLastDecisions
	lastSourceSnapshots := s.edgeDNSArtifactLastSourceSnapshots
	lastNodeProjections := s.edgeDNSArtifactLastNodeProjections
	lastRouteCompilations := s.edgeDNSArtifactLastRouteCompilations
	lastLegacyDerivations := s.edgeDNSArtifactLastLegacyDerivations
	lastImmutableWrites := s.edgeDNSArtifactLastImmutableWrites
	lastLegacyWrites := s.edgeDNSArtifactLastLegacyWrites
	lastParityMismatches := s.edgeDNSArtifactLastParityMismatches
	lastShadowActive := s.edgeDNSArtifactLastShadowActive
	lastVerifiedLKG := s.edgeDNSArtifactLastVerifiedLKG
	lastFullActive := s.edgeDNSArtifactLastFullActive
	lastVerificationDeferred := s.edgeDNSArtifactLastVerifyDeferred
	runCount := s.edgeDNSArtifactRunCount
	skippedCount := s.edgeDNSArtifactSkippedCount
	errorCount := s.edgeDNSArtifactErrorCount
	lastError := s.edgeDNSArtifactLastError
	handlerLookupHitCount := s.edgeDNSArtifactHandlerLookupHitCount
	handlerLookupMissCount := s.edgeDNSArtifactHandlerLookupMissCount
	handlerLookupErrorCount := s.edgeDNSArtifactHandlerLookupErrorCount
	s.edgeDNSArtifactMu.Unlock()

	observability.WriteCounterMetric(w, "fugue_edge_dns_artifact_runs_total", "Total edge DNS artifact controller runs.", nil, float64(runCount))
	observability.WriteCounterMetric(w, "fugue_edge_dns_artifact_skipped_total", "Total edge DNS artifact controller lock skips.", nil, float64(skippedCount))
	observability.WriteCounterMetric(w, "fugue_edge_dns_artifact_errors_total", "Total edge DNS artifact controller errors.", nil, float64(errorCount))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_duration_seconds", "Duration of the last edge DNS artifact controller run.", nil, lastDuration.Seconds())
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_count", "Number of DNS bundle artifacts written by the last controller run.", nil, float64(lastCount))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_decisions", "Number of DNS routing decisions written by the last controller run.", nil, float64(lastDecisions))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_source_snapshots", "Number of canonical source snapshots loaded by the last controller run.", nil, float64(lastSourceSnapshots))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_node_projections", "Number of node-scoped projections compiled from the canonical source snapshot by the last controller run.", nil, float64(lastNodeProjections))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_route_compilations", "Number of unique TrafficEpoch route bindings compiled by the last controller run.", nil, float64(lastRouteCompilations))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_legacy_derivations", "Number of legacy per-node full derivations executed by the last controller run.", nil, float64(lastLegacyDerivations))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_immutable_writes", "Number of immutable DNS answer bundle generations activated in the shadow lane by the last controller run.", nil, float64(lastImmutableWrites))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_legacy_writes", "Number of compatibility DNS bundle rows written by the last controller run.", nil, float64(lastLegacyWrites))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_parity_mismatches", "Number of immutable-to-compatibility DNS artifact digest mismatches in the last controller run.", nil, float64(lastParityMismatches))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_shadow_active", "Number of node-scoped immutable DNS artifacts activated in the shadow lane by the last controller run.", nil, float64(lastShadowActive))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_verified_lkg", "Number of node-scoped DNS artifacts with a verified immutable rollback generation after the last controller run.", nil, float64(lastVerifiedLKG))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_full_active", "Number of node-scoped immutable DNS artifacts activated in the full lane by the last controller run.", nil, float64(lastFullActive))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_verification_deferred", "Number of node-scoped DNS releases left unverified because current consumer evidence was incomplete in the last controller run.", nil, float64(lastVerificationDeferred))
	if !lastRun.IsZero() {
		observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_run_timestamp_seconds", "Unix timestamp of the last edge DNS artifact controller run.", nil, float64(lastRun.Unix()))
	}
	if !lastSuccess.IsZero() {
		observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_success_timestamp_seconds", "Unix timestamp of the last successful edge DNS artifact controller run.", nil, float64(lastSuccess.Unix()))
	}
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_error", "Whether the last edge DNS artifact controller run failed.", map[string]string{"error": truncateMetricLabel(lastError, 160)}, boolMetric(lastError != ""))
	observability.WriteMetricHeader(w, "fugue_edge_dns_artifact_handler_lookups_total", "Edge DNS handler artifact lookups by outcome.", "counter")
	observability.WriteMetricSample(w, "fugue_edge_dns_artifact_handler_lookups_total", map[string]string{"outcome": "hit"}, float64(handlerLookupHitCount))
	observability.WriteMetricSample(w, "fugue_edge_dns_artifact_handler_lookups_total", map[string]string{"outcome": "miss"}, float64(handlerLookupMissCount))
	observability.WriteMetricSample(w, "fugue_edge_dns_artifact_handler_lookups_total", map[string]string{"outcome": "error"}, float64(handlerLookupErrorCount))
}

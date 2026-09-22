package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
)

// Historical standalone publication is retained solely for migration fixtures.
// Production starts the observation loop and the unified traffic producer.
type edgeDNSArtifactPublicationStats struct {
	Artifacts            int
	SourceSnapshots      int
	NodeProjections      int
	RouteCompilations    int
	ImmutableWrites      int
	ShadowActive         int
	VerifiedLKG          int
	FullActive           int
	VerificationDeferred int
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
	s.edgeDNSArtifactLastImmutableWrites = publication.ImmutableWrites
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
			s.log.Printf("edge dns artifact controller failed: artifacts=%d snapshots=%d projections=%d route_compilations=%d immutable_writes=%d shadow_active=%d verified_lkg=%d full_active=%d verification_deferred=%d decisions=%d duration=%s err=%v", publication.Artifacts, publication.SourceSnapshots, publication.NodeProjections, publication.RouteCompilations, publication.ImmutableWrites, publication.ShadowActive, publication.VerifiedLKG, publication.FullActive, publication.VerificationDeferred, decisionCount, duration, err)
		}
		return
	}
	if s.log != nil {
		s.log.Printf("edge dns artifact controller complete: artifacts=%d snapshots=%d projections=%d route_compilations=%d immutable_writes=%d shadow_active=%d verified_lkg=%d full_active=%d verification_deferred=%d decisions=%d duration=%s", publication.Artifacts, publication.SourceSnapshots, publication.NodeProjections, publication.RouteCompilations, publication.ImmutableWrites, publication.ShadowActive, publication.VerifiedLKG, publication.FullActive, publication.VerificationDeferred, decisionCount, duration)
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
	decisionCount, err := s.reconcileEdgeDNSRoutingDecisionsWithContext(ctx, now)
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
			stats.RouteCompilations = len(snapshot.routeBindingByCompilationKey)
			return stats, fmt.Errorf("publish edge dns artifact for node %s: %w", target.node.ID, err)
		}
		stats.ImmutableWrites++
		stats.ShadowActive++
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
func (s *Server) publishEdgeDNSBundleArtifact(artifact edgeDNSBundleArtifact, options edgeDNSBundleOptions, now time.Time) (model.PlatformArtifact, error) {
	if err := s.validateEdgeDNSBundleArtifact(artifact, options, now); err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("validate before publication: %w", err)
	}
	platformArtifact, err := s.publishImmutableEdgeDNSBundleArtifact(artifact)
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("publish immutable generation: %w", err)
	}
	return platformArtifact, nil
}

func (s *Server) publishImmutableEdgeDNSBundleArtifact(projection edgeDNSBundleArtifact) (model.PlatformArtifact, error) {
	content, err := edgeDNSImmutableArtifactContentMap(projection)
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
			Key:         projection.ScopeKey,
			NodeID:      projection.DNSNodeID,
			EdgeGroupID: projection.EdgeGroupID,
		},
		Generation:         generation,
		Content:            content,
		CompatibilityFloor: model.PlatformArtifactSchemaVersionV1,
		CreatedByType:      model.ActorTypeSystem,
		CreatedByID:        "edge-dns-artifact-controller",
		CreatedAt:          projection.GeneratedAt,
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
			"dns_envelope_generation": strings.TrimSpace(projection.Bundle.Generation),
			"source_fingerprint":      projection.SourceFingerprint,
		},
	}})
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("validate immutable generation: %w", err)
	}
	active, _, _, _, err := s.store.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		Reason:         "mirror validated DNS bundle into immutable artifact ledger",
		IdempotencyKey: "edge-dns-shadow:" + projection.ScopeKey + ":" + generation,
	}, model.Principal{ActorType: model.ActorTypeSystem, ActorID: "edge-dns-artifact-controller"})
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("activate immutable shadow pointer: %w", err)
	}
	projected, err := edgeDNSBundleArtifactFromPlatformArtifact(active)
	if err != nil {
		return model.PlatformArtifact{}, fmt.Errorf("decode immutable shadow artifact: %w", err)
	}
	if !edgeDNSBundleArtifactsEquivalent(projection, projected) {
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
	lkg, err := s.store.GetStandalonePlatformLKG(model.PlatformArtifactKindDNSAnswerBundle, scopeKey)
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
		retryExpired, err := s.canReplaceExpiredEdgeDNSCandidate(fullArtifact, fullRelease, options, lkg, now)
		if err != nil {
			return result, err
		}
		if retryExpired {
			result.ReadyForFull = true
			return result, nil
		}
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

func (s *Server) canReplaceExpiredEdgeDNSCandidate(artifact model.PlatformArtifact, release model.PlatformArtifactRelease, options edgeDNSBundleOptions, lkg *model.PlatformLKGSnapshot, now time.Time) (bool, error) {
	if release.VerificationState != model.PlatformArtifactVerificationStateServingUnverified || lkg == nil {
		return false, nil
	}
	projected, err := edgeDNSBundleArtifactFromPlatformArtifact(artifact)
	if err != nil {
		return false, err
	}
	if projected.ValidUntil.IsZero() || now.Before(projected.ValidUntil) {
		return false, nil
	}
	if err := s.validateEdgeDNSFullRelease(artifact, release); err != nil {
		return false, err
	}
	if lkg.VerifiedByReleaseID == "" || lkg.VerificationEvidenceHash == "" ||
		!now.Before(lkg.ExpiresAt) || release.PinnedRollbackGeneration != lkg.Generation {
		return false, errors.New("expired DNS candidate has no valid verified rollback baseline")
	}
	rollback, err := s.store.GetPlatformArtifact(lkg.ArtifactID)
	if err != nil {
		return false, err
	}
	if rollback.Generation != lkg.Generation || rollback.ContentHash != lkg.ContentHash {
		return false, errors.New("DNS rollback artifact differs from the verified baseline")
	}
	if err := s.store.VerifyPlatformArtifactIntegrity(rollback); err != nil {
		return false, err
	}
	// Expiry prevents the node from ever ACKing this candidate. Validate its
	// historical envelope, but retain the existing LKG and publish a fresh,
	// unverified candidate through the normal fenced release path.
	projected.ActivatedAt = release.ReleasedAt
	projected.UpdatedAt = release.UpdatedAt
	if err := s.validateEdgeDNSBundleArtifact(projected, options, projected.GeneratedAt); err != nil {
		return false, err
	}
	return true, nil
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
	heartbeatAt := node.LastHeartbeatAt
	watchWindow := heartbeatAt != nil && !heartbeatAt.IsZero() && heartbeatAt.UTC().After(release.ReleasedAt.UTC())
	if !watchWindow {
		return false, nil
	}
	rollbackCompatible := true
	evidenceRefs := []string{
		"dns-heartbeat:" + strings.TrimSpace(node.ID) + ":" + heartbeatAt.UTC().Format(time.RFC3339Nano),
		"dns-envelope:" + strings.TrimSpace(projected.Bundle.Generation),
		"platform-artifact:" + artifact.ID,
		"platform-release:" + release.ID,
	}
	if lkg != nil {
		rollbackArtifact, err := s.store.GetPlatformArtifact(lkg.ArtifactID)
		if err != nil {
			return false, fmt.Errorf("load immutable rollback artifact: %w", err)
		}
		rollbackCompatible = strings.TrimSpace(release.PinnedRollbackGeneration) == strings.TrimSpace(lkg.Generation) &&
			rollbackArtifact.ID == lkg.ArtifactID && rollbackArtifact.Generation == lkg.Generation &&
			s.store.VerifyPlatformArtifactIntegrity(rollbackArtifact) == nil
		evidenceRefs = append(evidenceRefs, "platform-lkg:"+lkg.ID, "platform-rollback-artifact:"+rollbackArtifact.ID)
	}
	consumerConvergence := strings.TrimSpace(node.DNSBundleVersion) == strings.TrimSpace(projected.Bundle.Version) &&
		strings.TrimSpace(node.ServingGeneration) == strings.TrimSpace(projected.Bundle.Generation) &&
		strings.TrimSpace(node.LKGGeneration) == strings.TrimSpace(projected.Bundle.Generation) &&
		node.RecordCount == len(projected.Bundle.Records)
	localProbe := dnsNodeHeartbeatFresh(node, now) && dnsNodeServingHealthOK(node) &&
		strings.EqualFold(strings.TrimSpace(node.CacheStatus), "ready") && node.UDPListen && node.TCPListen && strings.TrimSpace(node.LastError) == ""
	baselineMonotonic := lkg == nil || artifact.GenerationSequence > lkg.GenerationSequence
	if !consumerConvergence || !localProbe || !watchWindow || !baselineMonotonic || !rollbackCompatible {
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
			EvidenceRefs:               evidenceRefs,
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
		s.edgeDNSArtifactHandlerFullCount++
	default:
		s.edgeDNSArtifactHandlerLookupMissCount++
	}
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
	lastImmutableWrites := s.edgeDNSArtifactLastImmutableWrites
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
	handlerImmutableFullCount := s.edgeDNSArtifactHandlerFullCount
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
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_immutable_writes", "Number of immutable DNS answer bundle generations activated in the shadow lane by the last controller run.", nil, float64(lastImmutableWrites))
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
	observability.WriteMetricHeader(w, "fugue_edge_dns_artifact_handler_source_total", "Successful Edge DNS handler lookups by artifact source.", "counter")
	observability.WriteMetricSample(w, "fugue_edge_dns_artifact_handler_source_total", map[string]string{"source": "immutable_full"}, float64(handlerImmutableFullCount))
}

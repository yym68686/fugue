package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/store"
)

const (
	edgeDNSArtifactControllerInterval = time.Minute
	edgeDNSArtifactControllerLockName = "edge-dns-artifact-controller"
	edgeDNSArtifactFreshMargin        = 30 * time.Second
)

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
	artifactCount := 0
	decisionCount := 0
	acquired := true
	var err error
	if s.store != nil {
		acquired, err = s.store.WithAdvisoryLock(ctx, edgeDNSArtifactControllerLockName, func() error {
			var runErr error
			artifactCount, decisionCount, runErr = s.rebuildEdgeDNSArtifacts(ctx, now)
			return runErr
		})
	} else {
		artifactCount, decisionCount, err = s.rebuildEdgeDNSArtifacts(ctx, now)
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
	s.edgeDNSArtifactLastCount = artifactCount
	s.edgeDNSArtifactLastDecisions = decisionCount
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
			s.log.Printf("edge dns artifact controller failed: artifacts=%d decisions=%d duration=%s err=%v", artifactCount, decisionCount, duration, err)
		}
		return
	}
	if s.log != nil {
		s.log.Printf("edge dns artifact controller complete: artifacts=%d decisions=%d duration=%s", artifactCount, decisionCount, duration)
	}
}

func (s *Server) rebuildEdgeDNSArtifacts(ctx context.Context, now time.Time) (int, int, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	decisionCount, err := s.reconcileEdgeDNSRoutingDecisions(now)
	if err != nil {
		return 0, decisionCount, fmt.Errorf("reconcile edge dns routing decisions: %w", err)
	}
	artifactCount, err := s.publishEdgeDNSBundleArtifacts(ctx, now)
	if err != nil {
		return artifactCount, decisionCount, err
	}
	return artifactCount, decisionCount, nil
}

func (s *Server) publishEdgeDNSBundleArtifacts(ctx context.Context, now time.Time) (int, error) {
	if s == nil || s.store == nil {
		return 0, nil
	}
	nodes, err := s.store.ListDNSNodes("")
	if err != nil {
		return 0, fmt.Errorf("list dns nodes for artifact publication: %w", err)
	}
	nodes = freshDNSNodes(nodes, now)
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].EdgeGroupID != nodes[j].EdgeGroupID {
			return nodes[i].EdgeGroupID < nodes[j].EdgeGroupID
		}
		return nodes[i].ID < nodes[j].ID
	})

	count := 0
	for _, node := range nodes {
		if !edgeDNSArtifactNodePublishable(node) {
			continue
		}
		options, ok := s.edgeDNSBundleOptionsForDNSNode(node)
		if !ok {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/v1/edge/dns", nil)
		if err != nil {
			return count, fmt.Errorf("build edge dns artifact request context: %w", err)
		}
		bundle, err := s.deriveEdgeDNSBundle(req, options)
		if err != nil {
			return count, fmt.Errorf("derive edge dns artifact for node %s: %w", node.ID, err)
		}
		artifact := store.EdgeDNSBundleArtifact{
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
			ActivatedAt:       now,
			UpdatedAt:         now,
		}
		if err := s.store.UpsertEdgeDNSBundleArtifact(artifact); err != nil {
			return count, fmt.Errorf("publish edge dns artifact for node %s: %w", node.ID, err)
		}
		count++
	}
	return count, nil
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
	if !edgeDNSBundleArtifactFresh(artifact, now) {
		return model.EdgeDNSBundle{}, false, nil
	}
	return artifact.Bundle, true, nil
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
	runCount := s.edgeDNSArtifactRunCount
	skippedCount := s.edgeDNSArtifactSkippedCount
	errorCount := s.edgeDNSArtifactErrorCount
	lastError := s.edgeDNSArtifactLastError
	s.edgeDNSArtifactMu.Unlock()

	observability.WriteCounterMetric(w, "fugue_edge_dns_artifact_runs_total", "Total edge DNS artifact controller runs.", nil, float64(runCount))
	observability.WriteCounterMetric(w, "fugue_edge_dns_artifact_skipped_total", "Total edge DNS artifact controller lock skips.", nil, float64(skippedCount))
	observability.WriteCounterMetric(w, "fugue_edge_dns_artifact_errors_total", "Total edge DNS artifact controller errors.", nil, float64(errorCount))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_duration_seconds", "Duration of the last edge DNS artifact controller run.", nil, lastDuration.Seconds())
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_count", "Number of DNS bundle artifacts written by the last controller run.", nil, float64(lastCount))
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_decisions", "Number of DNS routing decisions written by the last controller run.", nil, float64(lastDecisions))
	if !lastRun.IsZero() {
		observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_run_timestamp_seconds", "Unix timestamp of the last edge DNS artifact controller run.", nil, float64(lastRun.Unix()))
	}
	if !lastSuccess.IsZero() {
		observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_success_timestamp_seconds", "Unix timestamp of the last successful edge DNS artifact controller run.", nil, float64(lastSuccess.Unix()))
	}
	observability.WriteGaugeMetric(w, "fugue_edge_dns_artifact_last_error", "Whether the last edge DNS artifact controller run failed.", map[string]string{"error": truncateMetricLabel(lastError, 160)}, boolMetric(lastError != ""))
}

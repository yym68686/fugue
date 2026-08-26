package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

const (
	platformReleaseEvidenceSchema        = "platform-release-evidence/v2"
	platformReleaseEvidenceDefaultWindow = 5 * time.Minute
	platformReleaseEvidenceMaxWindow     = 30 * time.Minute
	platformReleaseHeartbeatMaxAge       = 90 * time.Second
)

type platformReleaseEvidenceGroup struct {
	EdgeGroupID    string `json:"group"`
	Slot           string `json:"slot"`
	ReleaseEpoch   string `json:"release_epoch"`
	BundleVersion  string `json:"bundle_version"`
	MinHealthy     int    `json:"min_healthy"`
	FreshHealthy   int    `json:"fresh_healthy"`
	BundleFresh    bool   `json:"bundle_fresh"`
	SignatureClean bool   `json:"signature_clean"`
}

type platformReleaseEvidenceMetrics struct {
	RequestCount        int64    `json:"request_count"`
	HardFailureCount    int64    `json:"hard_failure_count"`
	Application5xxCount int64    `json:"origin_connected_application_5xx_count"`
	P95TTFBMS           float64  `json:"p95_ttfb_ms"`
	P99DurationMS       float64  `json:"p99_duration_ms"`
	Classes             []string `json:"platform_error_classes"`
}

type platformReleaseEvidence struct {
	Schema          string                         `json:"schema"`
	Status          string                         `json:"status"`
	Reason          string                         `json:"reason"`
	GeneratedAt     time.Time                      `json:"generated_at"`
	ReleaseEpoch    string                         `json:"release_epoch"`
	BundleVersion   string                         `json:"bundle_version"`
	ActivationPhase string                         `json:"activation_phase"`
	Groups          []platformReleaseEvidenceGroup `json:"groups"`
	Metrics         platformReleaseEvidenceMetrics `json:"metrics"`
	EvidenceDigest  string                         `json:"evidence_digest"`
}

func (s *Server) handleAdminGetPlatformReleaseEvidence(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "only platform admin can inspect platform release evidence")
		return
	}
	releaseEpoch := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("release_epoch")))
	if releaseEpoch == "" || len(releaseEpoch) > 256 {
		httpx.WriteError(w, http.StatusBadRequest, "release_epoch is required")
		return
	}
	window, err := parsePlatformReleaseEvidenceWindow(r.URL.Query().Get("window"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	now, err := s.store.EdgeRoutePolicyTime()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	activation, err := s.store.GetEdgeActivationState()
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	instances, epochs, err := s.store.ListEdgeNodeInstances("")
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	metrics, metricsErr := s.queryPlatformReleaseEvidenceMetrics(r.Context(), releaseEpoch, now.Add(-window), now)
	evidence := evaluatePlatformReleaseEvidence(now, releaseEpoch, activation, instances, epochs, metrics, metricsErr)
	s.appendAudit(principal, "platform.release_evidence.read", "edge_release", releaseEpoch, "", map[string]string{
		"status": evidence.Status, "evidence_digest": evidence.EvidenceDigest,
	})
	httpx.WriteJSON(w, http.StatusOK, evidence)
}

func parsePlatformReleaseEvidenceWindow(raw string) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return platformReleaseEvidenceDefaultWindow, nil
	}
	window, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil || window < time.Minute || window > platformReleaseEvidenceMaxWindow {
		return 0, fmt.Errorf("window must be between 1m and %s", platformReleaseEvidenceMaxWindow)
	}
	return window, nil
}

func (s *Server) queryPlatformReleaseEvidenceMetrics(ctx context.Context, releaseEpoch string, since, until time.Time) (platformReleaseEvidenceMetrics, error) {
	classes := []string{
		model.PlatformErrorClassRouteUnavailable,
		model.PlatformErrorClassNoHealthy,
		model.PlatformErrorClassBundleSignature,
		model.PlatformErrorClassInvariant,
		model.PlatformErrorClassOriginDNS,
		model.PlatformErrorClassOriginConnect,
		model.PlatformErrorClassOriginUnavailable,
		model.PlatformErrorClassDecisionMissing,
		model.PlatformErrorClassEvidenceUnknown,
		model.PlatformErrorClassLatencyRegression,
	}
	quoted := make([]string, 0, len(classes))
	for _, class := range classes {
		quoted = append(quoted, quoteClickHouseString(class))
	}
	query := "SELECT count() AS request_count, " +
		"countIf(platform_error_class IN (" + strings.Join(quoted, ",") + ")) AS hard_failure_count, " +
		"countIf(platform_error_class = " + quoteClickHouseString(model.PlatformErrorClassOriginConnectedApp5xx) + ") AS application_5xx_count, " +
		"quantileTDigest(0.95)(toFloat64(ttfb_ms)) AS p95_ttfb_ms, " +
		"quantileTDigestIf(0.99)(toFloat64(duration_ms), NOT streaming) AS p99_duration_ms, " +
		"arraySort(groupUniqArray(platform_error_class)) AS platform_error_classes " +
		"FROM fugue_observability.request_facts WHERE release_epoch = " + quoteClickHouseString(releaseEpoch) +
		" AND ts >= " + clickHouseDateTime64Literal(since) + " AND ts <= " + clickHouseDateTime64Literal(until) + " FORMAT JSONEachRow"
	rows, err := s.queryAppObservabilityClickHouse(ctx, query)
	if err != nil {
		return platformReleaseEvidenceMetrics{}, err
	}
	if len(rows) != 1 {
		return platformReleaseEvidenceMetrics{}, fmt.Errorf("platform evidence query returned %d rows", len(rows))
	}
	row := rows[0]
	return platformReleaseEvidenceMetrics{
		RequestCount:        int64(floatField(row, "request_count")),
		HardFailureCount:    int64(floatField(row, "hard_failure_count")),
		Application5xxCount: int64(floatField(row, "application_5xx_count")),
		P95TTFBMS:           floatField(row, "p95_ttfb_ms"),
		P99DurationMS:       floatField(row, "p99_duration_ms"),
		Classes:             stringSliceField(row["platform_error_classes"]),
	}, nil
}

func stringSliceField(value any) []string {
	raw, ok := value.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
			out = append(out, strings.TrimSpace(text))
		}
	}
	sort.Strings(out)
	return out
}

func evaluatePlatformReleaseEvidence(now time.Time, releaseEpoch string, activation model.EdgeActivationState, instances []model.EdgeNodeInstance, epochs []model.EdgeActiveEpoch, metrics platformReleaseEvidenceMetrics, metricsErr error) platformReleaseEvidence {
	evidence := platformReleaseEvidence{
		Schema: platformReleaseEvidenceSchema, Status: "unknown", Reason: "evidence is incomplete",
		GeneratedAt: now.UTC(), ReleaseEpoch: releaseEpoch,
		ActivationPhase: activation.Phase, Metrics: metrics,
	}
	finish := func(status, reason string) platformReleaseEvidence {
		evidence.Status, evidence.Reason = status, reason
		evidence.BundleVersion = platformReleaseBundleSetVersion(evidence.Groups)
		copy := evidence
		copy.EvidenceDigest = ""
		body, _ := json.Marshal(copy)
		sum := sha256.Sum256(body)
		evidence.EvidenceDigest = "sha256:" + hex.EncodeToString(sum[:])
		return evidence
	}
	if activation.RouteAuthority != model.EdgeRouteAuthorityActiveEpoch || (activation.Phase != model.EdgeActivationPhaseActive && activation.Phase != model.EdgeActivationPhaseEnforced) {
		return finish("unknown", "active-epoch route authority is not durably established")
	}
	if len(epochs) == 0 {
		return finish("unknown", "active epoch inventory is empty")
	}
	for _, epoch := range epochs {
		group := platformReleaseEvidenceGroup{EdgeGroupID: epoch.EdgeGroupID, Slot: epoch.Slot, ReleaseEpoch: epoch.ReleaseEpoch, MinHealthy: epoch.MinHealthyInstances, BundleFresh: true, SignatureClean: true}
		if epoch.ReleaseEpoch != releaseEpoch || epoch.MinHealthyInstances <= 0 {
			evidence.Groups = append(evidence.Groups, group)
			return finish("failed", "active epoch identity does not match the requested release")
		}
		for _, instance := range instances {
			if instance.EdgeGroupID != epoch.EdgeGroupID || instance.Slot != epoch.Slot || instance.ReleaseEpoch != epoch.ReleaseEpoch {
				continue
			}
			fresh := !instance.LastHeartbeatAt.IsZero() && !instance.LastHeartbeatAt.After(now.Add(5*time.Second)) && now.Sub(instance.LastHeartbeatAt) <= platformReleaseHeartbeatMaxAge
			signatureClean := strings.TrimSpace(instance.FailureClass) == ""
			reportedBundle := strings.TrimSpace(instance.Node.RouteBundleVersion)
			bundleFresh := reportedBundle != "" && strings.TrimSpace(instance.Node.CaddyAppliedVersion) == reportedBundle
			if group.BundleVersion == "" {
				group.BundleVersion = reportedBundle
			} else if reportedBundle != group.BundleVersion {
				bundleFresh = false
			}
			group.SignatureClean = group.SignatureClean && signatureClean
			group.BundleFresh = group.BundleFresh && bundleFresh
			if fresh && signatureClean && bundleFresh && instance.EffectiveHealthy && instance.ConsecutiveHealthy >= 2 && !instance.Node.Draining && model.NormalizeEdgeTLSStatus(instance.Node.TLSStatus) == model.EdgeTLSStatusReady {
				group.FreshHealthy++
			}
		}
		if group.BundleVersion == "" {
			group.BundleFresh = false
		}
		evidence.Groups = append(evidence.Groups, group)
		if group.FreshHealthy < group.MinHealthy || !group.BundleFresh || !group.SignatureClean {
			return finish("failed", "active cohort is not fresh, bundle-current, signature-clean, and healthy")
		}
	}
	if metricsErr != nil || metrics.RequestCount == 0 {
		return finish("unknown", "request evidence is unavailable")
	}
	if metrics.HardFailureCount > 0 {
		return finish("failed", "platform request evidence contains a hard failure")
	}
	return finish("passed", "active cohort, bundle, route, origin, link, and latency evidence passed")
}

func platformReleaseBundleSetVersion(groups []platformReleaseEvidenceGroup) string {
	versions := make([]string, 0, len(groups))
	for _, group := range groups {
		if groupID, version := strings.TrimSpace(group.EdgeGroupID), strings.TrimSpace(group.BundleVersion); groupID != "" && version != "" {
			versions = append(versions, groupID+"\x00"+version)
		}
	}
	if len(versions) == 0 {
		return ""
	}
	sort.Strings(versions)
	body, _ := json.Marshal(versions)
	sum := sha256.Sum256(body)
	return "edgebundles_" + hex.EncodeToString(sum[:])
}

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleListPlatformArtifacts(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.read scope required")
		return
	}
	filter := model.PlatformArtifactFilter{
		ArtifactKind: r.URL.Query().Get("kind"),
		ScopeKey:     r.URL.Query().Get("scope"),
		Status:       r.URL.Query().Get("status"),
		Limit:        queryIntDefault(r, "limit", 100),
	}
	artifacts, err := s.store.ListPlatformArtifacts(filter)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.PlatformArtifactListResponse{
		Artifacts:   artifacts,
		GeneratedAt: time.Now().UTC(),
	})
}

func (s *Server) handleCreatePlatformArtifact(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.HasScope("artifact.create_draft") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.create_draft scope required")
		return
	}
	var req model.PlatformArtifactCreateRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	artifact := model.PlatformArtifact{
		ArtifactKind:       req.ArtifactKind,
		Scope:              req.Scope,
		Generation:         req.Generation,
		Content:            req.Content,
		CompatibilityFloor: req.CompatibilityFloor,
		Metadata:           req.Metadata,
		CreatedByType:      principal.ActorType,
		CreatedByID:        principal.ActorID,
	}
	created, err := s.store.CreatePlatformArtifact(artifact)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "platform_artifact.draft_created", "platform_artifact", created.ID, "", map[string]string{
		"artifact_kind": created.ArtifactKind,
		"scope_key":     created.ScopeKey,
		"generation":    created.Generation,
		"content_hash":  created.ContentHash,
	})
	httpx.WriteJSON(w, http.StatusCreated, model.PlatformArtifactResponse{Artifact: created})
}

func (s *Server) handleGetPlatformArtifact(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.read scope required")
		return
	}
	artifact, err := s.store.GetPlatformArtifact(r.PathValue("artifact_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.PlatformArtifactResponse{Artifact: artifact})
}

func (s *Server) handleValidatePlatformArtifact(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.HasScope("artifact.validate") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.validate scope required")
		return
	}
	req := model.PlatformArtifactValidateRequest{DryRun: true}
	if r.Body != nil && r.ContentLength != 0 {
		if err := httpx.DecodeJSON(r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	artifact, err := s.store.GetPlatformArtifact(r.PathValue("artifact_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	results := validatePlatformArtifactDraft(artifact)
	pass := platformArtifactValidationPass(results)
	if !req.DryRun {
		artifact, err = s.store.ValidatePlatformArtifact(artifact.ID, results)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		action := "platform_artifact.validation_failed"
		if pass {
			action = "platform_artifact.validated"
		}
		s.appendAudit(principal, action, "platform_artifact", artifact.ID, "", map[string]string{
			"artifact_kind": artifact.ArtifactKind,
			"scope_key":     artifact.ScopeKey,
			"generation":    artifact.Generation,
			"pass":          fmt.Sprintf("%t", pass),
		})
	}
	httpx.WriteJSON(w, http.StatusOK, model.PlatformArtifactValidationResponse{
		Artifact: artifact,
		Results:  results,
		Pass:     pass,
		DryRun:   req.DryRun,
	})
}

func (s *Server) handleReleasePlatformArtifact(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var req model.PlatformArtifactReleaseRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.ReleaseChannel = store.NormalizePlatformReleaseChannel(req.ReleaseChannel)
	if req.ReleaseChannel == "" {
		httpx.WriteError(w, http.StatusBadRequest, "release_channel must be shadow, gray, or full")
		return
	}
	if scope := platformArtifactReleaseScope(req.ReleaseChannel); !principal.HasScope(scope) {
		httpx.WriteError(w, http.StatusForbidden, scope+" scope required")
		return
	}
	if req.ForcePublish && !principal.HasScope("artifact.force_publish") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.force_publish scope required")
		return
	}
	if req.ForcePublish && strings.TrimSpace(req.Reason) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "force_publish requires reason")
		return
	}
	artifact, release, message, lkg, err := s.store.ReleasePlatformArtifact(r.PathValue("artifact_id"), req, principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	action := "platform_artifact." + req.ReleaseChannel + "_released"
	s.appendAudit(principal, action, "platform_artifact", artifact.ID, "", map[string]string{
		"artifact_kind":   artifact.ArtifactKind,
		"scope_key":       artifact.ScopeKey,
		"generation":      artifact.Generation,
		"release_id":      release.ID,
		"release_channel": release.ReleaseChannel,
		"force_publish":   fmt.Sprintf("%t", req.ForcePublish),
	})
	httpx.WriteJSON(w, http.StatusOK, model.PlatformArtifactReleaseResponse{
		Artifact: artifact,
		Release:  release,
		Message:  message,
		LKG:      lkg,
	})
}

func (s *Server) handleRollbackPlatformArtifact(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.HasScope("artifact.rollback") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.rollback scope required")
		return
	}
	var req model.PlatformArtifactRollbackRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.ToGeneration) == "" || strings.TrimSpace(req.Reason) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "to_generation and reason are required")
		return
	}
	req.ReleaseChannel = store.NormalizePlatformReleaseChannel(req.ReleaseChannel)
	if req.ReleaseChannel == "" {
		httpx.WriteError(w, http.StatusBadRequest, "release_channel must be shadow, gray, or full")
		return
	}
	if req.ForcePublish && !principal.HasScope("artifact.force_publish") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.force_publish scope required")
		return
	}
	artifact, release, message, lkg, err := s.store.RollbackPlatformArtifact(r.PathValue("artifact_id"), req, principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "platform_artifact.rollback_completed", "platform_artifact", artifact.ID, "", map[string]string{
		"artifact_kind":   artifact.ArtifactKind,
		"scope_key":       artifact.ScopeKey,
		"generation":      artifact.Generation,
		"release_id":      release.ID,
		"release_channel": release.ReleaseChannel,
		"to_generation":   req.ToGeneration,
	})
	httpx.WriteJSON(w, http.StatusOK, model.PlatformArtifactReleaseResponse{
		Artifact: artifact,
		Release:  release,
		Message:  message,
		LKG:      lkg,
	})
}

func (s *Server) handleListPlatformArtifactConsumers(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.read scope required")
		return
	}
	artifact, err := s.store.GetPlatformArtifact(r.PathValue("artifact_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	consumers, err := s.store.ListPlatformConsumers(artifact.ArtifactKind, artifact.ScopeKey)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.PlatformArtifactConsumersResponse{
		Consumers:   consumers,
		GeneratedAt: time.Now().UTC(),
	})
}

func (s *Server) handleGetPlatformArtifactLKG(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.read scope required")
		return
	}
	artifact, err := s.store.GetPlatformArtifact(r.PathValue("artifact_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	lkg, err := s.store.GetPlatformLKG(artifact.ArtifactKind, artifact.ScopeKey)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.PlatformArtifactLKGResponse{LKG: lkg})
}

func platformArtifactReleaseScope(channel string) string {
	switch store.NormalizePlatformReleaseChannel(channel) {
	case model.PlatformArtifactReleaseChannelShadow:
		return "artifact.release_shadow"
	case model.PlatformArtifactReleaseChannelGray:
		return "artifact.release_gray"
	case model.PlatformArtifactReleaseChannelFull:
		return "artifact.release_full"
	default:
		return "artifact.release"
	}
}

func (s *Server) handleGetPlatformStateArtifact(w http.ResponseWriter, r *http.Request) {
	_ = mustPrincipal(r)
	kind := store.NormalizePlatformArtifactKind(r.PathValue("artifact_kind"))
	if kind == "" {
		httpx.WriteError(w, http.StatusBadRequest, "unknown artifact kind")
		return
	}
	scopeKey := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("scope_key")))
	if scopeKey == "" {
		scopeKey = "global"
	}
	channel := store.NormalizePlatformReleaseChannel(r.URL.Query().Get("channel"))
	if channel == "" {
		httpx.WriteError(w, http.StatusBadRequest, "channel must be shadow, gray, or full")
		return
	}
	currentGeneration := strings.TrimSpace(r.URL.Query().Get("current_generation"))
	waitSeconds := queryIntDefault(r, "wait_seconds", 0)
	if waitSeconds < 0 {
		waitSeconds = 0
	}
	if waitSeconds > 30 {
		waitSeconds = 30
	}
	var artifact model.PlatformArtifact
	var release model.PlatformArtifactRelease
	var found bool
	var err error
	waited := false
	deadline := time.Now().Add(time.Duration(waitSeconds) * time.Second)
	for {
		artifact, release, found, err = s.store.GetActivePlatformArtifact(kind, scopeKey, channel)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if currentGeneration == "" || !found || artifact.Generation != currentGeneration || waitSeconds == 0 || time.Now().After(deadline) {
			break
		}
		waited = true
		time.Sleep(500 * time.Millisecond)
	}
	lkg, err := s.store.GetPlatformLKG(kind, scopeKey)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	messages, err := s.store.ListPlatformReleaseMessages(kind, scopeKey, time.Now().Add(-24*time.Hour), 20)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	response := model.PlatformStateArtifactResponse{
		Messages: messages,
		LKG:      lkg,
		Waited:   waited,
	}
	if found {
		response.Artifact = &artifact
		response.Release = &release
		response.Generation = artifact.Generation
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) handlePlatformConsumerHeartbeat(w http.ResponseWriter, r *http.Request) {
	_ = mustPrincipal(r)
	var req model.PlatformConsumerHeartbeatRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	consumer, err := s.store.UpsertPlatformConsumerHeartbeat(req)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	drift := consumer.DesiredGeneration != "" && consumer.ActualGeneration != "" && consumer.DesiredGeneration != consumer.ActualGeneration
	httpx.WriteJSON(w, http.StatusOK, model.PlatformConsumerHeartbeatResponse{
		Consumer: consumer,
		Drift:    drift,
	})
}

func (s *Server) handleListSubsystemFailureContracts(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.SubsystemFailureContractListResponse{
		Contracts:   subsystemFailureContracts(),
		GeneratedAt: time.Now().UTC(),
	})
}

func (s *Server) handleGetSubsystemFailureContract(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	subsystem := strings.TrimSpace(strings.ToLower(r.PathValue("subsystem")))
	for _, contract := range subsystemFailureContracts() {
		if contract.Subsystem == subsystem {
			httpx.WriteJSON(w, http.StatusOK, model.SubsystemFailureContractResponse{Contract: contract})
			return
		}
	}
	httpx.WriteError(w, http.StatusNotFound, "subsystem failure contract not found")
}

func validatePlatformArtifactDraft(artifact model.PlatformArtifact) []model.PlatformArtifactValidationResult {
	results := []model.PlatformArtifactValidationResult{
		{Name: "schema.kind", Pass: store.NormalizePlatformArtifactKind(artifact.ArtifactKind) != "", Severity: model.RobustnessSeverityBlockPublish, Message: "artifact kind must be known"},
		{Name: "schema.scope", Pass: strings.TrimSpace(artifact.ScopeKey) != "", Severity: model.RobustnessSeverityBlockPublish, Message: "artifact scope key must be derived"},
		{Name: "schema.content", Pass: len(artifact.Content) > 0, Severity: model.RobustnessSeverityBlockPublish, Message: "artifact content must be a non-empty JSON object"},
		{Name: "content.hash", Pass: strings.HasPrefix(artifact.ContentHash, "sha256:"), Severity: model.RobustnessSeverityBlockPublish, Message: "artifact must have a content-addressed sha256 hash"},
	}
	secretPath := firstSecretLikeContentPath(artifact.Content, "")
	results = append(results, model.PlatformArtifactValidationResult{
		Name:     "secret_safe.content",
		Pass:     secretPath == "",
		Severity: model.RobustnessSeverityBlockPublish,
		Message:  firstNonEmpty("artifact content must not contain secret-like keys", secretPath),
		Evidence: map[string]string{"path": secretPath},
	})
	results = append(results, platformArtifactInvariantValidation(artifact))
	results = append(results, model.PlatformArtifactValidationResult{
		Name:     "compatibility.floor",
		Pass:     artifact.CompatibilityFloor == "" || strings.HasPrefix(strings.ToLower(artifact.CompatibilityFloor), "v"),
		Severity: model.RobustnessSeverityWarning,
		Message:  "compatibility_floor is empty or version-prefixed",
	})
	return results
}

func platformArtifactInvariantValidation(artifact model.PlatformArtifact) model.PlatformArtifactValidationResult {
	pass := true
	message := "no kind-specific invariant violations"
	switch artifact.ArtifactKind {
	case model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig:
		_, pass = artifact.Content["routes"]
		message = "route artifacts must include routes"
	case model.PlatformArtifactKindDNSAnswerBundle:
		_, records := artifact.Content["records"]
		_, answers := artifact.Content["answers"]
		pass = records || answers
		message = "DNS artifacts must include records or answers"
	}
	return model.PlatformArtifactValidationResult{
		Name:     "invariant." + artifact.ArtifactKind,
		Pass:     pass,
		Severity: model.RobustnessSeverityBlockPublish,
		Message:  message,
	}
}

func platformArtifactValidationPass(results []model.PlatformArtifactValidationResult) bool {
	for _, result := range results {
		if !result.Pass {
			return false
		}
	}
	return true
}

func firstSecretLikeContentPath(value any, path string) string {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			childPath := key
			if path != "" {
				childPath = path + "." + key
			}
			if platformArtifactSecretLikeKey(key) {
				return childPath
			}
			if found := firstSecretLikeContentPath(child, childPath); found != "" {
				return found
			}
		}
	case []any:
		for index, child := range typed {
			childPath := fmt.Sprintf("%s[%d]", path, index)
			if found := firstSecretLikeContentPath(child, childPath); found != "" {
				return found
			}
		}
	case json.RawMessage:
		var decoded any
		if json.Unmarshal(typed, &decoded) == nil {
			return firstSecretLikeContentPath(decoded, path)
		}
	}
	return ""
}

func platformArtifactSecretLikeKey(key string) bool {
	normalized := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(key)), "-", "_")
	for _, marker := range []string{"secret", "password", "token", "private_key", "api_key", "access_key", "credential"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func subsystemFailureContracts() []model.SubsystemFailureContract {
	now := time.Now().UTC()
	names := []struct {
		subsystem string
		summary   string
	}{
		{"control_plane_api", "API readiness, store access, auth, and OpenAPI route serving"},
		{"controller", "background reconciliation workers and platform automation loops"},
		{"platform_state_release_system", "versioned artifact validation, release messages, consumers, LKG, and rollback"},
		{"node_guardian", "node deep health, quarantine decisions, and repair planning"},
		{"node_updater", "node desired-state pull, task execution, and local repair hooks"},
		{"kubernetes_cni_dns", "Kubernetes API, CNI, kube-dns, CoreDNS, and service networking"},
		{"edge_front", "client-facing edge proxy, TLS, body read, cache, and request attribution"},
		{"edge_worker", "edge route bundle consumer, local LKG, probes, and origin connectivity"},
		{"dns_server", "authoritative DNS serving, answer bundle consumer, health gates, and LKG"},
		{"dns_answer_policy", "scoped answer ranking, exploration, cooldown, and fallback"},
		{"caddy_route_bundle", "Caddy route config generation, validation, reload, and rollback"},
		{"runtime_scheduler", "runtime placement, node eligibility, and quarantine hard gates"},
		{"app_runtime", "managed app pods, service routing, logs, and runtime probes"},
		{"database_stateful_services", "managed databases, stateful continuity, backups, restore, and fencing"},
		{"observability_metrics", "metrics ingestion, dashboards, alert evidence, and degraded-signal behavior"},
		{"automatic_repair_system", "repair planner, execution guardrails, leases, rate limits, and audit"},
	}
	contracts := make([]model.SubsystemFailureContract, 0, len(names))
	for _, item := range names {
		contracts = append(contracts, model.SubsystemFailureContract{
			Subsystem: item.subsystem,
			Owner:     "platform",
			Summary:   item.summary,
			FailureModes: []model.FailureMode{
				{ID: item.subsystem + ".unavailable", Description: "subsystem is unavailable or cannot serve its critical path", Severity: model.RobustnessSeverityBlockPublish},
				{ID: item.subsystem + ".stale_generation", Description: "subsystem is serving stale desired state or stale generated artifacts", Severity: model.RobustnessSeverityDegraded},
				{ID: item.subsystem + ".bad_output", Description: "subsystem produced invalid or unsafe output", Severity: model.RobustnessSeverityBlockPublish},
			},
			DetectionSignals: []model.DetectionSignal{
				{Name: "readiness", Description: "deep readiness probe for the subsystem", Required: true},
				{Name: "generation_drift", Description: "desired generation differs from actual generation", Required: true},
				{Name: "request_or_operation_attribution", Description: "request or operation error class points at the subsystem"},
			},
			IsolationActions: []model.IsolationAction{
				{Name: "quarantine", Description: "remove unsafe node, edge, route, or artifact from new traffic", Automatic: true},
				{Name: "block_release", Description: "stop releases that would expand blast radius", Automatic: true},
			},
			FallbackBehaviors: []model.FallbackBehavior{
				{Name: "serve_lkg", Description: "continue serving validated local or published last-known-good state when fresh state is unavailable"},
				{Name: "fail_closed", Description: "refuse unsafe publication when no validated fallback exists"},
			},
			RepairActions: []model.RepairAction{
				{Name: "resync_desired_state", Description: "pull desired state again and re-apply atomically", SafetyClass: "low", Automatic: true},
				{Name: "restart_stateless_component", Description: "restart stateless component after LKG is available", SafetyClass: "medium"},
			},
			RollbackPaths: []model.RollbackPath{
				{Name: "release_previous_generation", Description: "publish a new release record pointing at a previously validated generation"},
			},
			AttributionClasses: []string{item.subsystem + ".unavailable", item.subsystem + ".stale_generation", item.subsystem + ".bad_output"},
			HumanApprovalBoundaries: []model.HumanApprovalBoundary{
				{Action: "force_publish", Description: "publishing despite failed validation requires explicit human reason", Required: true},
				{Action: "stateful_repair", Description: "stateful failover or destructive repair requires fence and backup evidence", Required: true},
			},
			ObserveOnlyAllowed:         true,
			AutomaticQuarantineAllowed: true,
			AutomaticRepairAllowed:     item.subsystem != "database_stateful_services",
			HumanApprovalRequired:      item.subsystem == "database_stateful_services" || item.subsystem == "automatic_repair_system",
			RunbookRef:                 "docs/runbooks/" + item.subsystem + ".md",
			UpdatedAt:                  now,
		})
	}
	return contracts
}

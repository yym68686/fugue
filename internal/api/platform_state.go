package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fugue/internal/auth"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
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
	if artifact.ArtifactKind == model.PlatformArtifactKindPolicySnapshot {
		if err := validatePlatformPolicyArtifact(artifact); err != nil {
			httpx.WriteError(w, http.StatusConflict, err.Error())
			return
		}
	}
	if artifact.ArtifactKind == model.PlatformArtifactKindPlatformIntent {
		if err := validatePlatformIntentArtifact(artifact); err != nil {
			httpx.WriteError(w, http.StatusConflict, err.Error())
			return
		}
	}
	results := validatePlatformArtifactDraft(artifact)
	if artifact.ArtifactKind == model.PlatformArtifactKindReleaseSet {
		results = append(results, s.validateReleaseSetReferences(artifact))
	}
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

func validatePlatformPolicyArtifact(artifact model.PlatformArtifact) error {
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return fmt.Errorf("policy snapshot content is not JSON: %w", err)
	}
	var policy platformconfig.PolicySnapshot
	if err := json.Unmarshal(raw, &policy); err != nil {
		return fmt.Errorf("policy snapshot schema is invalid: %w", err)
	}
	if err := platformconfig.ValidatePolicySnapshot(policy); err != nil {
		return fmt.Errorf("policy snapshot is invalid: %w", err)
	}
	digest, err := platformconfig.Digest(policy)
	if err != nil {
		return fmt.Errorf("policy snapshot digest failed: %w", err)
	}
	if expected := strings.TrimSpace(artifact.Metadata["policy_digest"]); expected != "" && expected != digest {
		return fmt.Errorf("policy snapshot digest does not match metadata")
	}
	return nil
}

func validatePlatformIntentArtifact(artifact model.PlatformArtifact) error {
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return fmt.Errorf("platform intent content is not JSON: %w", err)
	}
	var intent platformconfig.PlatformIntent
	if err := json.Unmarshal(raw, &intent); err != nil {
		return fmt.Errorf("platform intent schema is invalid: %w", err)
	}
	intentDigest, err := platformconfig.Digest(intent)
	if err != nil {
		return fmt.Errorf("platform intent digest failed: %w", err)
	}
	if err := platformconfig.ValidatePlatformIntent(intent); err != nil {
		return fmt.Errorf("platform intent is invalid: %w", err)
	}
	if expected := strings.TrimSpace(artifact.Metadata["intent_digest"]); expected != "" && expected != intentDigest {
		return fmt.Errorf("platform intent digest does not match metadata")
	}
	return nil
}

func (s *Server) validateReleaseSetReferences(artifact model.PlatformArtifact) model.PlatformArtifactValidationResult {
	ids, idsOK := artifact.Content["artifact_ids"].([]any)
	kinds, kindsOK := artifact.Content["artifact_kinds"].([]any)
	if !idsOK || !kindsOK || len(ids) == 0 || len(ids) != len(kinds) {
		return model.PlatformArtifactValidationResult{Name: "release_set.references", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set artifact references must be parallel non-empty arrays"}
	}
	if dependencies, exists := artifact.Content["dependencies"].([]any); exists {
		allowed := map[string]struct{}{}
		for _, rawKind := range kinds {
			if kind, ok := rawKind.(string); ok {
				allowed[kind] = struct{}{}
			}
		}
		seenEdges := map[string]struct{}{}
		for _, rawDependency := range dependencies {
			dependency, ok := rawDependency.(map[string]any)
			if !ok {
				return model.PlatformArtifactValidationResult{Name: "release_set.dependencies", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set dependency graph is malformed"}
			}
			from, fromOK := dependency["from"].(string)
			to, toOK := dependency["to"].(string)
			relation, relationOK := dependency["relation"].(string)
			if !fromOK || !toOK || !relationOK || relation != "requires" {
				return model.PlatformArtifactValidationResult{Name: "release_set.dependencies", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set dependency relation must be requires"}
			}
			if _, ok := allowed[from]; !ok {
				return model.PlatformArtifactValidationResult{Name: "release_set.dependencies", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set dependency references an unknown source kind"}
			}
			if _, ok := allowed[to]; !ok {
				return model.PlatformArtifactValidationResult{Name: "release_set.dependencies", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set dependency references an unknown target kind"}
			}
			key := from + "\x00" + to + "\x00" + relation
			if _, exists := seenEdges[key]; exists {
				return model.PlatformArtifactValidationResult{Name: "release_set.dependencies", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set dependency graph contains duplicate edges"}
			}
			seenEdges[key] = struct{}{}
		}
	}
	seen := map[string]struct{}{}
	intentDigest := artifact.Metadata["intent_digest"]
	policyDigest := artifact.Metadata["policy_digest"]
	releaseSetGeneration := strings.TrimSpace(artifact.Generation)
	for index, rawID := range ids {
		id, ok := rawID.(string)
		if !ok || strings.TrimSpace(id) == "" {
			return model.PlatformArtifactValidationResult{Name: "release_set.references", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set contains an invalid artifact id"}
		}
		if _, exists := seen[id]; exists {
			return model.PlatformArtifactValidationResult{Name: "release_set.references", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set contains duplicate artifact ids", Evidence: map[string]string{"artifact_id": id}}
		}
		seen[id] = struct{}{}
		child, err := s.store.GetPlatformArtifact(id)
		if err != nil {
			return model.PlatformArtifactValidationResult{Name: "release_set.references", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set references an unknown artifact", Evidence: map[string]string{"artifact_id": id}}
		}
		expectedKind, ok := kinds[index].(string)
		if !ok || strings.TrimSpace(expectedKind) != child.ArtifactKind || child.Status != model.PlatformArtifactStatusValidated {
			return model.PlatformArtifactValidationResult{Name: "release_set.references", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set child artifact kind or validation status does not match", Evidence: map[string]string{"artifact_id": id, "expected_kind": fmt.Sprint(expectedKind), "actual_kind": child.ArtifactKind, "status": child.Status}}
		}
		if intentDigest != "" && child.Metadata["intent_digest"] != "" && child.Metadata["intent_digest"] != intentDigest {
			return model.PlatformArtifactValidationResult{Name: "release_set.lineage", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set child intent digest does not match", Evidence: map[string]string{"artifact_id": id}}
		}
		if policyDigest != "" && child.Metadata["policy_digest"] != "" && child.Metadata["policy_digest"] != policyDigest {
			return model.PlatformArtifactValidationResult{Name: "release_set.lineage", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set child policy digest does not match", Evidence: map[string]string{"artifact_id": id}}
		}
		if child.Metadata["release_set_generation"] != releaseSetGeneration {
			return model.PlatformArtifactValidationResult{Name: "release_set.lineage", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set child release generation does not match", Evidence: map[string]string{"artifact_id": id, "expected_release_set_generation": releaseSetGeneration, "actual_release_set_generation": child.Metadata["release_set_generation"]}}
		}
	}
	return model.PlatformArtifactValidationResult{Name: "release_set.references", Pass: true, Severity: model.RobustnessSeverityBlockPublish, Message: "all release set child artifacts exist, are validated, and match lineage"}
}

func (s *Server) handleReleasePlatformArtifact(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
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
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform administrator required")
		return
	}
	if req.ForcePublish {
		req.SoftOverride = true
	}
	if req.SoftOverride && req.KernelBreakGlass != nil {
		httpx.WriteError(w, http.StatusBadRequest, "soft_override and kernel_break_glass are mutually exclusive")
		return
	}
	if req.SoftOverride &&
		!principal.HasScope("artifact.soft_override") &&
		!principal.HasScope("artifact.force_publish") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.soft_override scope required")
		return
	}
	if req.SoftOverride && strings.TrimSpace(req.Reason) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "soft_override requires reason")
		return
	}
	if req.KernelBreakGlass != nil {
		if !principal.HasExplicitScope("artifact.kernel_break_glass") {
			httpx.WriteError(w, http.StatusForbidden, "explicit artifact.kernel_break_glass scope required")
			return
		}
		if strings.TrimSpace(req.Reason) == "" {
			httpx.WriteError(w, http.StatusBadRequest, "kernel_break_glass requires reason")
			return
		}
		artifact, err := s.store.GetPlatformArtifact(r.PathValue("artifact_id"))
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		if err := platformsafety.ValidateKernelBreakGlassAuthorization(req.KernelBreakGlass, artifact, time.Now().UTC()); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if req.SoftOverride {
		req.ForcePublish = false
	}
	if req.KernelBreakGlass == nil && !req.SoftOverride {
		req.ForcePublish = false
	}
	if releaseArtifact, err := s.store.GetPlatformArtifact(r.PathValue("artifact_id")); err != nil {
		s.writeStoreError(w, err)
		return
	} else if releaseArtifact.ArtifactKind == model.PlatformArtifactKindReleaseSet {
		if referenceResult := s.validateReleaseSetReferences(releaseArtifact); !referenceResult.Pass {
			httpx.WriteError(w, http.StatusConflict, referenceResult.Message)
			return
		}
		if req.ReleaseChannel == model.PlatformArtifactReleaseChannelFull {
			if convergenceResult := s.validateReleaseSetConvergence(releaseArtifact); !convergenceResult.Pass {
				httpx.WriteError(w, http.StatusConflict, convergenceResult.Message)
				return
			}
		}
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
		"override_mode":   firstNonEmpty(release.OverrideMode, "none"),
		"override_expiry": platformReleaseOverrideExpiry(release.OverrideExpiresAt),
		"bypassed":        strings.Join(release.BypassedInvariants, ","),
		"reason":          strings.TrimSpace(req.Reason),
	})
	httpx.WriteJSON(w, http.StatusOK, model.PlatformArtifactReleaseResponse{
		Artifact: artifact,
		Release:  release,
		Message:  message,
		LKG:      lkg,
	})
}

func (s *Server) validateReleaseSetConvergence(artifact model.PlatformArtifact) model.PlatformArtifactValidationResult {
	sets, err := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ReleaseSetID: artifact.ID, Limit: 200})
	if err != nil {
		return model.PlatformArtifactValidationResult{Name: "release_set.convergence", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set consumer convergence could not be evaluated"}
	}
	for _, set := range sets {
		consumers, consumerErr := s.store.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
		if consumerErr != nil {
			return model.PlatformArtifactValidationResult{Name: "release_set.convergence", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "release set consumer convergence could not be evaluated"}
		}
		status := platformcontrol.EvaluateConsumerConvergence(set, consumers, time.Now().UTC())
		if set.RequiresConsumers && !status.Pass {
			return model.PlatformArtifactValidationResult{Name: "release_set.convergence", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "required release set consumers have not converged", Evidence: map[string]string{"expected_consumer_set_id": set.ID, "state": status.State, "required_passing": fmt.Sprintf("%d", status.RequiredPassing), "required_expected": fmt.Sprintf("%d", status.RequiredExpected)}}
		}
	}
	return model.PlatformArtifactValidationResult{Name: "release_set.convergence", Pass: true, Severity: model.RobustnessSeverityBlockPublish, Message: "required release set consumers have converged"}
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
	if req.ForcePublish {
		req.SoftOverride = true
	}
	if req.SoftOverride && req.KernelBreakGlass != nil {
		httpx.WriteError(w, http.StatusBadRequest, "soft_override and kernel_break_glass are mutually exclusive")
		return
	}
	if req.SoftOverride &&
		!principal.HasScope("artifact.soft_override") &&
		!principal.HasScope("artifact.force_publish") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.soft_override scope required")
		return
	}
	if req.KernelBreakGlass != nil {
		if !principal.IsPlatformAdmin() || !principal.HasExplicitScope("artifact.kernel_break_glass") {
			httpx.WriteError(w, http.StatusForbidden, "explicit artifact.kernel_break_glass scope required")
			return
		}
		if err := platformsafety.ValidateKernelBreakGlassAuthorization(
			req.KernelBreakGlass,
			model.PlatformArtifact{ID: req.ToGeneration, Generation: req.ToGeneration},
			time.Now().UTC(),
		); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if req.SoftOverride {
		req.ForcePublish = false
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
		"override_mode":   firstNonEmpty(release.OverrideMode, "none"),
		"override_expiry": platformReleaseOverrideExpiry(release.OverrideExpiresAt),
		"bypassed":        strings.Join(release.BypassedInvariants, ","),
		"reason":          strings.TrimSpace(req.Reason),
	})
	httpx.WriteJSON(w, http.StatusOK, model.PlatformArtifactReleaseResponse{
		Artifact: artifact,
		Release:  release,
		Message:  message,
		LKG:      lkg,
	})
}

func (s *Server) handleVerifyPlatformArtifactReleaseLKG(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() || !principal.HasScope("artifact.verify_lkg") {
		httpx.WriteError(w, http.StatusForbidden, "artifact.verify_lkg scope required")
		return
	}
	var wireReq platformArtifactVerifyLKGHTTPRequest
	if err := httpx.DecodeJSON(r, &wireReq); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if wireReq.FencingToken <= 0 || strings.TrimSpace(wireReq.Reason) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "positive fencing_token and reason are required")
		return
	}
	req, evidenceStates, err := wireReq.modelRequest()
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if nonPassing := nonPassingPlatformVerificationEvidence(evidenceStates); len(nonPassing) > 0 {
		httpx.WriteError(w, http.StatusConflict, "verification evidence did not pass: "+strings.Join(nonPassing, ","))
		return
	}
	artifact, release, message, lkg, err := s.store.VerifyPlatformArtifactReleaseLKG(r.PathValue("release_id"), req, principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "platform_artifact.verified_lkg_promoted", "platform_artifact_release", release.ID, "", map[string]string{
		"artifact_id":   artifact.ID,
		"artifact_kind": artifact.ArtifactKind,
		"scope_key":     artifact.ScopeKey,
		"generation":    artifact.Generation,
		"fencing_token": fmt.Sprintf("%d", release.FencingToken),
		"evidence_hash": lkg.VerificationEvidenceHash,
		"reason":        strings.TrimSpace(req.Reason),
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

func (s *Server) handleListPlatformExpectedConsumerSets(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() || !principal.HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "platform admin with artifact.read scope required")
		return
	}
	limit := 50
	if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 1 || parsed > 200 {
			httpx.WriteError(w, http.StatusBadRequest, "limit must be between 1 and 200")
			return
		}
		limit = parsed
	}
	sets, err := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{
		ReleaseSetID:      r.URL.Query().Get("release_set_id"),
		ArtifactReleaseID: r.URL.Query().Get("artifact_release_id"),
		ArtifactKind:      r.URL.Query().Get("artifact_kind"),
		ScopeKey:          r.URL.Query().Get("scope_key"),
		Limit:             limit,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.PlatformExpectedConsumerSetListResponse{
		ExpectedConsumerSets: sets,
		GeneratedAt:          time.Now().UTC(),
	})
}

func (s *Server) handlePreparePlatformReleaseSetConsumers(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() || !principal.HasScope("artifact.release_shadow") {
		httpx.WriteError(w, http.StatusForbidden, "platform admin with artifact.release_shadow scope required")
		return
	}
	var request struct {
		ReleaseSetID      string `json:"release_set_id"`
		ArtifactReleaseID string `json:"artifact_release_id"`
	}
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	releaseSet, err := s.store.GetPlatformArtifact(request.ReleaseSetID)
	if err != nil || releaseSet.ArtifactKind != model.PlatformArtifactKindReleaseSet {
		httpx.WriteError(w, http.StatusNotFound, "release set not found")
		return
	}
	if releaseSet.Status != model.PlatformArtifactStatusValidated {
		httpx.WriteError(w, http.StatusConflict, "release set must be validated")
		return
	}
	if strings.TrimSpace(request.ArtifactReleaseID) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "artifact_release_id is required")
		return
	}
	edges, _, err := s.store.ListEdgeNodes("")
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	dns, err := s.store.ListDNSNodes("")
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	updaters, err := s.store.ListNodeUpdaters("", true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	runtimes, err := s.store.ListRuntimes("", true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	topology := platformcontrol.ExpectedConsumerTopology{EdgeNodes: edges, DNSNodes: dns, NodeUpdaters: updaters, Runtimes: runtimes}
	ids, okIDs := releaseSet.Content["artifact_ids"].([]any)
	kinds, okKinds := releaseSet.Content["artifact_kinds"].([]any)
	if !okIDs || !okKinds || len(ids) != len(kinds) {
		httpx.WriteError(w, http.StatusConflict, "release set references are invalid")
		return
	}
	sets := make([]model.PlatformExpectedConsumerSet, 0, len(kinds))
	for i, rawKind := range kinds {
		kind, ok := rawKind.(string)
		if !ok {
			httpx.WriteError(w, http.StatusConflict, "release set artifact kind is invalid")
			return
		}
		artifactID, ok := ids[i].(string)
		if !ok {
			httpx.WriteError(w, http.StatusConflict, "release set artifact ID is invalid")
			return
		}
		child, childErr := s.store.GetPlatformArtifact(artifactID)
		if childErr != nil {
			s.writeStoreError(w, childErr)
			return
		}
		set, buildErr := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: releaseSet.ID, ArtifactReleaseID: request.ArtifactReleaseID, ArtifactKind: kind, Scope: child.Scope, ScopeKey: child.ScopeKey, Generation: child.Generation, Revision: int64(i + 1), PreparedAt: time.Now().UTC(), Topology: topology})
		if buildErr != nil {
			httpx.WriteError(w, http.StatusConflict, buildErr.Error())
			return
		}
		created, createErr := s.store.CreatePlatformExpectedConsumerSet(set)
		if createErr != nil {
			if errors.Is(createErr, store.ErrConflict) {
				existing, listErr := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ReleaseSetID: releaseSet.ID, ArtifactKind: kind, Limit: 20})
				if listErr != nil || len(existing) != 1 || existing[0].ArtifactReleaseID != request.ArtifactReleaseID {
					httpx.WriteError(w, http.StatusConflict, "expected consumer set already exists with conflicting identity")
					return
				}
				created = existing[0]
			} else {
				s.writeStoreError(w, createErr)
				return
			}
		}
		sets = append(sets, created)
	}
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"release_set_id": releaseSet.ID, "artifact_release_id": request.ArtifactReleaseID, "expected_consumer_sets": sets, "generated_at": time.Now().UTC()})
}

func (s *Server) handleListPlatformConsumerConvergence(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() || !principal.HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "platform admin with artifact.read scope required")
		return
	}
	sets, err := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{
		ReleaseSetID:      r.URL.Query().Get("release_set_id"),
		ArtifactReleaseID: r.URL.Query().Get("artifact_release_id"),
		ArtifactKind:      r.URL.Query().Get("artifact_kind"),
		ScopeKey:          r.URL.Query().Get("scope_key"),
		Limit:             queryIntDefault(r, "limit", 50),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	statuses := make([]model.PlatformConsumerConvergenceStatus, 0, len(sets))
	for _, set := range sets {
		consumers, consumerErr := s.store.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
		if consumerErr != nil {
			s.writeStoreError(w, consumerErr)
			return
		}
		statuses = append(statuses, platformcontrol.EvaluateConsumerConvergence(set, consumers, time.Now().UTC()))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"convergence": statuses, "generated_at": time.Now().UTC()})
}

func (s *Server) handleListPlatformRuntimeFacts(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() || !principal.HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "platform admin with artifact.read scope required")
		return
	}
	limit := queryIntDefault(r, "limit", 200)
	if limit > 1000 {
		httpx.WriteError(w, http.StatusBadRequest, "limit cannot exceed 1000")
		return
	}
	events, err := s.store.ListAuditEvents("", true, limit)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	consumerID := strings.TrimSpace(r.URL.Query().Get("consumer_id"))
	releaseSetID := strings.TrimSpace(r.URL.Query().Get("release_set_id"))
	artifactKind := strings.TrimSpace(r.URL.Query().Get("artifact_kind"))
	facts := make([]model.AuditEvent, 0, len(events))
	for _, event := range events {
		if event.Action != "platform_consumer.heartbeat_accepted" && !strings.HasPrefix(event.Action, "platform_artifact.") {
			continue
		}
		if consumerID != "" && event.TargetID != consumerID && event.Metadata["consumer_id"] != consumerID {
			continue
		}
		if releaseSetID != "" && event.Metadata["release_set_id"] != releaseSetID {
			continue
		}
		if artifactKind != "" && event.Metadata["artifact_kind"] != artifactKind {
			continue
		}
		facts = append(facts, event)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"runtime_facts": facts, "generated_at": time.Now().UTC()})
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

func (s *Server) handleGetPlatformPolicyLKG(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() || !principal.HasScope("artifact.read") {
		httpx.WriteError(w, http.StatusForbidden, "platform admin artifact.read scope required")
		return
	}
	lkg, err := s.store.GetPlatformLKG(model.PlatformArtifactKindPolicySnapshot, "global")
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "verified platform policy LKG is unavailable")
		return
	}
	if lkg == nil {
		httpx.WriteError(w, http.StatusNotFound, "verified platform policy LKG not found")
		return
	}
	artifact, err := s.store.GetPlatformArtifact(lkg.ArtifactID)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "verified platform policy LKG artifact is unavailable")
		return
	}
	if artifact.Status != model.PlatformArtifactStatusValidated ||
		!platformsafety.EvaluatePlatformLKGSnapshot(*lkg, artifact, s.bundleKeyring(), time.Now().UTC()).Pass {
		httpx.WriteError(w, http.StatusServiceUnavailable, "verified platform policy LKG is invalid")
		return
	}
	if err := validatePlatformPolicyArtifact(artifact); err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"artifact": artifact, "lkg": lkg})
}

func (s *Server) verifiedPlatformArtifactForScope(kind, scopeKey string) (model.PlatformArtifact, bool, error) {
	lkg, err := s.store.GetPlatformLKG(kind, scopeKey)
	if err != nil {
		return model.PlatformArtifact{}, false, err
	}
	if lkg == nil {
		return model.PlatformArtifact{}, false, nil
	}
	artifact, err := s.store.GetPlatformArtifact(lkg.ArtifactID)
	if err != nil {
		return model.PlatformArtifact{}, false, err
	}
	if artifact.Status != model.PlatformArtifactStatusValidated ||
		!platformsafety.EvaluatePlatformLKGSnapshot(*lkg, artifact, s.bundleKeyring(), time.Now().UTC()).Pass {
		return model.PlatformArtifact{}, false, nil
	}
	return artifact, true, nil
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

func platformReleaseOverrideExpiry(expiresAt *time.Time) string {
	if expiresAt == nil {
		return ""
	}
	return expiresAt.UTC().Format(time.RFC3339Nano)
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
	principal := mustPrincipal(r)
	if !principal.HasScope("platform.consumer.heartbeat") {
		httpx.WriteError(w, http.StatusForbidden, "platform.consumer.heartbeat scope required")
		return
	}
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

func (s *Server) handleTrustedPlatformConsumerHeartbeat(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.PlatformComponentIdentityFromContext(r.Context())
	if !ok {
		httpx.WriteError(w, http.StatusInternalServerError, "verified platform component identity missing")
		return
	}
	var heartbeat platformcontrol.PlatformConsumerHeartbeatEnvelope
	if err := httpx.DecodeJSON(r, &heartbeat); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	consumer, err := s.store.AcceptTrustedPlatformConsumerHeartbeatWithAudit(
		claims,
		heartbeat.ExpectedConsumerSetID,
		heartbeat,
		time.Now().UTC(),
		platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
		s.heartbeatAuditKeyring,
	)
	if err != nil {
		writeTrustedPlatformConsumerHeartbeatError(w, err)
		return
	}
	drift := consumer.DesiredGeneration != "" && consumer.ActualGeneration != "" && consumer.DesiredGeneration != consumer.ActualGeneration
	httpx.WriteJSON(w, http.StatusOK, model.PlatformConsumerHeartbeatResponse{
		Consumer: consumer,
		Drift:    drift,
	})
}

func writeTrustedPlatformConsumerHeartbeatError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrInvalidInput):
		httpx.WriteError(w, http.StatusBadRequest, "invalid trusted heartbeat request")
	case errors.Is(err, store.ErrNotFound):
		httpx.WriteError(w, http.StatusNotFound, "expected consumer set not found")
	case errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatImpersonation),
		errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatExpectation):
		httpx.WriteError(w, http.StatusForbidden, "platform component identity does not match expected consumer topology")
	case errors.Is(err, store.ErrConflict),
		errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatReplay),
		errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatGenerationBack),
		errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatFencingBack):
		httpx.WriteError(w, http.StatusConflict, "trusted heartbeat is not monotonic")
	case errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatInvalid),
		errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatStale),
		errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatFuture),
		errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatEvidence):
		httpx.WriteError(w, http.StatusUnprocessableEntity, "trusted heartbeat evidence is invalid")
	default:
		httpx.WriteError(w, http.StatusInternalServerError, "trusted heartbeat could not be recorded")
	}
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
		{Name: "schema.version", Pass: artifact.SchemaVersion == model.PlatformArtifactSchemaVersionV1, Severity: model.RobustnessSeverityBlockPublish, Message: "artifact schema version must be supported"},
		{Name: "schema.content", Pass: len(artifact.Content) > 0, Severity: model.RobustnessSeverityBlockPublish, Message: "artifact content must be a non-empty JSON object"},
		{Name: "content.hash", Pass: strings.HasPrefix(artifact.ContentHash, "sha256:"), Severity: model.RobustnessSeverityBlockPublish, Message: "artifact must have a content-addressed sha256 hash"},
		{Name: "generation.sequence", Pass: artifact.GenerationSequence > 0, Severity: model.RobustnessSeverityBlockPublish, Message: "artifact generation sequence must be positive"},
		{
			Name: "provenance.signature",
			Pass: artifact.Provenance.Issuer == model.PlatformArtifactIssuerFugue &&
				artifact.Provenance.Algorithm == model.PlatformSignatureHMACSHA256 &&
				strings.TrimSpace(artifact.Provenance.KeyID) != "" &&
				strings.TrimSpace(artifact.Provenance.Signature) != "" &&
				!artifact.Provenance.SignedAt.IsZero(),
			Severity: model.RobustnessSeverityBlockPublish,
			Message:  "artifact must carry control-plane provenance",
		},
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
	case model.PlatformArtifactKindReleaseSet:
		ids, idsOK := artifact.Content["artifact_ids"].([]any)
		kinds, kindsOK := artifact.Content["artifact_kinds"].([]any)
		lineage, lineageOK := artifact.Content["lineage"].(map[string]any)
		seenKinds := map[string]bool{}
		for _, value := range kinds {
			if kind, ok := value.(string); ok {
				seenKinds[kind] = true
			}
		}
		pass = idsOK && kindsOK && lineageOK && len(ids) == len(kinds) && len(ids) >= 3 &&
			seenKinds[model.PlatformArtifactKindEdgeRouteBundle] &&
			seenKinds[model.PlatformArtifactKindDNSAnswerBundle] &&
			seenKinds[model.PlatformArtifactKindCaddyRouteConfig] &&
			strings.TrimSpace(fmt.Sprint(lineage["intent_digest"])) != "" &&
			strings.TrimSpace(fmt.Sprint(lineage["policy_digest"])) != ""
		message = "release sets must bind route, DNS, and TLS artifacts with lineage"
	case model.PlatformArtifactKindReleaseGuardPolicy:
		return releaseSignalPolicyValidationResult(artifact)
	case model.PlatformArtifactKindGatePolicyRegistry:
		return gatePolicyValidationResult(artifact)
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
		{"control_plane_topology", "control-plane capability discovery, quorum explain, endpoint mode, and redundancy risk status"},
		{"controller", "background reconciliation workers and platform automation loops"},
		{"platform_state_release_system", "versioned artifact validation, release messages, consumers, LKG, and rollback"},
		{"local_wal_reconciliation", "node-local autonomy WAL records, signer verification, expiry, replay, and incident merge"},
		{"node_guardian", "node deep health, quarantine decisions, and repair planning"},
		{"node_updater", "node desired-state pull, task execution, and local repair hooks"},
		{"kubernetes_cni_dns", "Kubernetes API, CNI, kube-dns, CoreDNS, and service networking"},
		{"edge_front", "client-facing edge proxy, TLS, body read, cache, and request attribution"},
		{"edge_worker", "edge route bundle consumer, local LKG, probes, and origin connectivity"},
		{"edge_origin_health", "per-hostname origin DNS, ClusterIP, endpoint, HTTP, TTFB, body-write, and response-write health"},
		{"endpoint_lkg_fallback", "short-TTL endpoint last-known-good fallback for stateless HTTP routes when control-plane or service DNS is unavailable"},
		{"dns_server", "authoritative DNS serving, answer bundle consumer, health gates, and LKG"},
		{"dns_answer_policy", "scoped answer ranking, exploration, cooldown, and fallback"},
		{"peer_emergency_overlay", "TTL-bound peer health signals, signed evidence, temporary filtering, and false-positive controls"},
		{"caddy_route_bundle", "Caddy route config generation, validation, reload, and rollback"},
		{"runtime_scheduler", "runtime placement, node eligibility, and quarantine hard gates"},
		{"runtime_agent", "runtime node task execution, workload health reporting, node-local cache, and app lifecycle hooks"},
		{"app_runtime", "managed app pods, service routing, logs, and runtime probes"},
		{"image_cache_agent", "node image cache hydration, verification, retention, and repair replication"},
		{"database_stateful_services", "managed databases, stateful continuity, backups, restore, and fencing"},
		{"observability_metrics", "metrics ingestion, dashboards, alert evidence, and degraded-signal behavior"},
		{"automatic_repair_system", "repair planner, execution guardrails, leases, rate limits, and audit"},
		{"github_actions_runner", "control-plane deploy runner health, failure-domain spread, workflow attribution, and fail-closed behavior"},
		{"external_watchdog", "out-of-cluster synthetic probes, provider power actions, and audited recovery attempts"},
		{"provider_power_watchdog", "provider or hypervisor power event import, classification, and recovery evidence"},
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
				{Action: "soft_override", Description: "bypassing non-kernel validation requires platform administration and an explicit human reason", Required: true},
				{Action: "kernel_break_glass", Description: "a narrowly bounded kernel recovery requires explicit non-inherited permission, dual confirmation, and a short TTL", Required: true},
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

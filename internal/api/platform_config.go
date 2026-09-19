package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
)

type platformConfigCompileRequest struct {
	Intent          platformconfig.PlatformIntent  `json:"intent"`
	Policy          platformconfig.PolicySnapshot  `json:"policy"`
	RuntimeSnapshot platformconfig.RuntimeSnapshot `json:"runtime_snapshot,omitempty"`
	InputSnapshot   map[string]any                 `json:"input_snapshot,omitempty"`
}

type platformConfigCompileResponse struct {
	Lineage         platformconfig.Lineage    `json:"lineage"`
	ReleaseSet      platformconfig.ReleaseSet `json:"release_set"`
	IntentArtifact  model.PlatformArtifact    `json:"intent_artifact"`
	PolicyArtifact  model.PlatformArtifact    `json:"policy_artifact"`
	RouteArtifact   model.PlatformArtifact    `json:"route_artifact"`
	DNSArtifact     model.PlatformArtifact    `json:"dns_artifact"`
	TLSArtifact     model.PlatformArtifact    `json:"tls_artifact"`
	ReleaseArtifact model.PlatformArtifact    `json:"release_artifact"`
}

type platformConfigLineageResponse struct {
	Artifact     model.PlatformArtifact            `json:"artifact"`
	Lineage      platformconfig.Lineage            `json:"lineage"`
	LKG          *model.PlatformLKGSnapshot        `json:"lkg,omitempty"`
	Dependencies []platformConfigLineageDependency `json:"dependencies,omitempty"`
}

type platformConfigLineageDependency struct {
	Artifact model.PlatformArtifact     `json:"artifact"`
	Lineage  platformconfig.Lineage     `json:"lineage"`
	LKG      *model.PlatformLKGSnapshot `json:"lkg,omitempty"`
}

func (s *Server) handleCompilePlatformConfig(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var request platformConfigCompileRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{
		Intent:          request.Intent,
		Policy:          request.Policy,
		RuntimeSnapshot: request.RuntimeSnapshot,
		InputSnapshot:   request.InputSnapshot,
		CreatedAt:       time.Now().UTC(),
	})
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := s.materializePlatformCompilation(r.Context(), compiled, principal, nil)
	if err != nil {
		s.writePlatformConfigMaterializationError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, result)
}

type platformConfigCompileArtifactsRequest struct {
	IntentArtifactID string                         `json:"intent_artifact_id"`
	PolicyArtifactID string                         `json:"policy_artifact_id"`
	RuntimeSnapshot  platformconfig.RuntimeSnapshot `json:"runtime_snapshot,omitempty"`
}

// handleCompilePlatformConfigFromArtifacts compiles only immutable validated
// artifacts. It deliberately does not read business tables or accept inline
// serving configuration, so a caller can replay the exact stored inputs.
func (s *Server) handleCompilePlatformConfigFromArtifacts(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var request platformConfigCompileArtifactsRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(request.IntentArtifactID) == "" || strings.TrimSpace(request.PolicyArtifactID) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "intent_artifact_id and policy_artifact_id are required")
		return
	}
	intentArtifact, err := s.store.GetPlatformArtifact(request.IntentArtifactID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpx.WriteError(w, http.StatusNotFound, "intent artifact not found")
		} else {
			httpx.WriteError(w, http.StatusServiceUnavailable, "intent artifact storage unavailable")
		}
		return
	}
	policyArtifact, err := s.store.GetPlatformArtifact(request.PolicyArtifactID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			httpx.WriteError(w, http.StatusNotFound, "policy artifact not found")
		} else {
			httpx.WriteError(w, http.StatusServiceUnavailable, "policy artifact storage unavailable")
		}
		return
	}
	if intentArtifact.ID != request.IntentArtifactID || policyArtifact.ID != request.PolicyArtifactID {
		httpx.WriteError(w, http.StatusNotFound, "exact artifact IDs are required")
		return
	}
	if intentArtifact.ArtifactKind != model.PlatformArtifactKindPlatformIntent || policyArtifact.ArtifactKind != model.PlatformArtifactKindPolicySnapshot ||
		intentArtifact.Status != model.PlatformArtifactStatusValidated || policyArtifact.Status != model.PlatformArtifactStatusValidated {
		httpx.WriteError(w, http.StatusConflict, "compile inputs must be validated platform intent and policy artifacts")
		return
	}
	if !platformsafety.EvaluateArtifactIntegrity(intentArtifact, s.bundleKeyring()).Pass || !platformsafety.EvaluateArtifactIntegrity(policyArtifact, s.bundleKeyring()).Pass {
		httpx.WriteError(w, http.StatusConflict, "compile inputs must have trusted signatures and matching content digests")
		return
	}
	if err := validatePlatformIntentArtifact(intentArtifact); err != nil {
		httpx.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	if err := validatePlatformPolicyArtifact(policyArtifact); err != nil {
		httpx.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	intentSchema, _ := intentArtifact.Content["schema_version"].(string)
	policySchema, _ := policyArtifact.Content["schema_version"].(string)
	if strings.TrimSpace(intentSchema) != platformconfig.SchemaVersion || strings.TrimSpace(policySchema) != platformconfig.SchemaVersion {
		httpx.WriteError(w, http.StatusConflict, "compile artifacts must declare the current platform config schema")
		return
	}
	var intent platformconfig.PlatformIntent
	var policy platformconfig.PolicySnapshot
	if err := decodeCompilerArtifactContent(intentArtifact, &intent); err != nil {
		httpx.WriteError(w, http.StatusConflict, "intent artifact content cannot be decoded")
		return
	}
	if err := decodeCompilerArtifactContent(policyArtifact, &policy); err != nil {
		httpx.WriteError(w, http.StatusConflict, "policy artifact content cannot be decoded")
		return
	}
	if intent.Generation != intentArtifact.Generation || policy.Generation != policyArtifact.Generation {
		httpx.WriteError(w, http.StatusConflict, "artifact generation does not match typed content")
		return
	}
	intent = platformconfig.NormalizePlatformIntent(intent)
	policy = platformconfig.NormalizePolicySnapshot(policy)
	intentScope, _ := store.NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "global", Key: intent.Scope})
	policyScope, _ := store.NormalizePlatformArtifactScope(model.PlatformArtifactScope{ScopeType: "global", Key: policy.Scope})
	if intentScope != intentArtifact.Scope || policyScope != policyArtifact.Scope {
		httpx.WriteError(w, http.StatusConflict, "artifact scope does not match typed content")
		return
	}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: intent, Policy: policy, RuntimeSnapshot: request.RuntimeSnapshot, CreatedAt: time.Now().UTC()})
	if err != nil {
		httpx.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	result, err := s.materializePlatformCompilation(r.Context(), compiled, principal, &platformConfigStoredInputs{Intent: intentArtifact, Policy: policyArtifact})
	if err != nil {
		s.writePlatformConfigMaterializationError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, result)
}

func (s *Server) writePlatformConfigMaterializationError(w http.ResponseWriter, err error) {
	var referenceError *platformConfigReferenceError
	if errors.As(err, &referenceError) {
		httpx.WriteError(w, http.StatusConflict, referenceError.Error())
		return
	}
	s.writeStoreError(w, err)
}

func decodeCompilerArtifactContent(artifact model.PlatformArtifact, output any) error {
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(output)
}

func (s *Server) handlePlatformConfigEnvironmentImportPreview(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	generation := strings.TrimSpace(r.URL.Query().Get("generation"))
	if generation == "" {
		httpx.WriteError(w, http.StatusBadRequest, "generation is required")
		return
	}
	env := map[string]string{}
	for _, item := range os.Environ() {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			env[key] = value
		}
	}
	result, err := s.importPlatformEnvironment(env, generation)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, result)
}

type platformConfigEnvironmentImportRequest struct {
	Generation string `json:"generation"`
}

func (s *Server) handlePlatformConfigEnvironmentImport(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var request platformConfigEnvironmentImportRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	env := map[string]string{}
	for _, item := range os.Environ() {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			env[key] = value
		}
	}
	result, err := s.importPlatformEnvironment(env, request.Generation)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	now := time.Now().UTC()
	artifact := model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindPlatformIntent,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   result.Intent.Generation,
		Content:      mustPlatformIntentContent(result.Intent),
		Metadata: map[string]string{
			"intent_digest": resultIntentDigest(result.Intent),
			"source":        "env-migration",
			"source_digest": result.SourceDigest,
			"migrated_at":   now.Format(time.RFC3339Nano),
			"migrated_by":   strings.TrimSpace(principal.ActorID),
		},
		CreatedByType: principal.ActorType,
		CreatedByID:   principal.ActorID,
	}
	created, _, err := s.store.EnsurePlatformArtifact(artifact)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if err := validatePlatformIntentArtifact(created); err != nil {
		httpx.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	validated, err := s.store.ValidatePlatformArtifact(created.ID, []model.PlatformArtifactValidationResult{{
		Name: "platform_config.environment_import", Pass: true,
		Severity: model.RobustnessSeverityBlockPublish,
		Message:  "legacy serving environment imported into immutable PlatformIntent draft",
		Evidence: map[string]string{"source": "env-migration", "source_digest": result.SourceDigest},
	}})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "platform_config.environment_imported", "platform_artifact", validated.ID, "", map[string]string{
		"source": "env-migration", "source_digest": result.SourceDigest, "generation": result.Intent.Generation,
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"artifact": validated, "source_digest": result.SourceDigest, "imported_keys": result.ImportedKeys})
}

func mustPlatformIntentContent(intent platformconfig.PlatformIntent) map[string]any {
	raw, _ := json.Marshal(intent)
	content := map[string]any{}
	_ = json.Unmarshal(raw, &content)
	return content
}

func resultIntentDigest(intent platformconfig.PlatformIntent) string {
	digest, _ := platformconfig.Digest(intent)
	return digest
}

func (s *Server) handleGetPlatformArtifactLineage(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	artifact, err := s.store.GetPlatformArtifact(r.PathValue("artifact_id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	var lkg *model.PlatformLKGSnapshot
	if snapshot, lkgErr := s.store.GetPlatformLKG(artifact.ArtifactKind, artifact.ScopeKey); lkgErr == nil {
		lkg = snapshot
	} else if lkgErr != nil && !isStoreNotFound(lkgErr) {
		s.writeStoreError(w, lkgErr)
		return
	}
	dependencies, dependencyErr := s.platformArtifactLineageDependencies(artifact)
	if dependencyErr != nil {
		httpx.WriteError(w, http.StatusConflict, dependencyErr.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, platformConfigLineageResponse{
		Artifact: artifact, Lineage: platformconfig.LineageFromArtifact(artifact), LKG: lkg, Dependencies: dependencies,
	})
}

func (s *Server) handleGetPlatformHostnameLineage(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	hostname := normalizeExternalAppDomain(r.URL.Query().Get("hostname"))
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	entries := []platformConfigLineageDependency{}
	for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig} {
		artifacts, err := s.store.ListPlatformArtifacts(model.PlatformArtifactFilter{ArtifactKind: kind, Status: model.PlatformArtifactStatusValidated, Limit: 200})
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		for _, artifact := range artifacts {
			if !platformArtifactContentContainsHostname(artifact.Content, hostname) {
				continue
			}
			lkg, lkgErr := s.store.GetPlatformLKG(artifact.ArtifactKind, artifact.ScopeKey)
			if lkgErr != nil && !isStoreNotFound(lkgErr) {
				s.writeStoreError(w, lkgErr)
				return
			}
			entries = append(entries, platformConfigLineageDependency{Artifact: artifact, Lineage: platformconfig.LineageFromArtifact(artifact), LKG: lkg})
		}
	}
	slices.SortFunc(entries, func(left, right platformConfigLineageDependency) int {
		if left.Artifact.ArtifactKind < right.Artifact.ArtifactKind {
			return -1
		}
		if left.Artifact.ArtifactKind > right.Artifact.ArtifactKind {
			return 1
		}
		return strings.Compare(left.Artifact.Generation, right.Artifact.Generation)
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"hostname": hostname, "artifacts": entries, "generated_at": time.Now().UTC()})
}

func platformArtifactContentContainsHostname(content map[string]any, hostname string) bool {
	if content == nil {
		return false
	}
	var visit func(any) bool
	visit = func(value any) bool {
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				if strings.Contains(strings.ToLower(key), "hostname") {
					if candidate, ok := child.(string); ok && normalizeExternalAppDomain(candidate) == hostname {
						return true
					}
				}
				if visit(child) {
					return true
				}
			}
		case []any:
			for _, child := range typed {
				if visit(child) {
					return true
				}
			}
		}
		return false
	}
	return visit(content)
}

func (s *Server) platformArtifactLineageDependencies(artifact model.PlatformArtifact) ([]platformConfigLineageDependency, error) {
	if artifact.ArtifactKind != model.PlatformArtifactKindReleaseSet {
		return nil, nil
	}
	ids, ok := artifact.Content["artifact_ids"].([]any)
	if !ok {
		return nil, fmt.Errorf("release set lineage dependencies are malformed")
	}
	dependencies := make([]platformConfigLineageDependency, 0, len(ids))
	for _, rawID := range ids {
		id, ok := rawID.(string)
		if !ok || strings.TrimSpace(id) == "" {
			return nil, fmt.Errorf("release set lineage dependency id is invalid")
		}
		child, err := s.store.GetPlatformArtifact(id)
		if err != nil {
			return nil, fmt.Errorf("release set lineage dependency %q is unavailable", id)
		}
		childLKG, lkgErr := s.store.GetPlatformLKG(child.ArtifactKind, child.ScopeKey)
		if lkgErr != nil && !isStoreNotFound(lkgErr) {
			return nil, lkgErr
		}
		dependencies = append(dependencies, platformConfigLineageDependency{Artifact: child, Lineage: platformconfig.LineageFromArtifact(child), LKG: childLKG})
	}
	return dependencies, nil
}

func isStoreNotFound(err error) bool {
	return errors.Is(err, store.ErrNotFound)
}

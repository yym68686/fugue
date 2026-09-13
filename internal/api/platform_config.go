package api

import (
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
	artifacts := []*model.PlatformArtifact{
		&compiled.IntentArtifact,
		&compiled.PolicyArtifact,
		&compiled.RouteArtifact,
		&compiled.DNSArtifact,
		&compiled.TLSArtifact,
	}
	for _, artifact := range artifacts {
		artifact.CreatedByType = strings.TrimSpace(principal.ActorType)
		artifact.CreatedByID = strings.TrimSpace(principal.ActorID)
		stored, _, storeErr := s.store.EnsurePlatformArtifact(*artifact)
		if storeErr != nil {
			s.writeStoreError(w, storeErr)
			return
		}
		*artifact = stored
		validated, validateErr := s.store.ValidatePlatformArtifact(stored.ID, []model.PlatformArtifactValidationResult{{
			Name:     "platform_config.compiler",
			Pass:     true,
			Severity: model.RobustnessSeverityBlockPublish,
			Message:  "deterministic platform configuration compiler passed",
			Evidence: map[string]string{
				"intent_digest":    compiled.Lineage.IntentDigest,
				"policy_digest":    compiled.Lineage.PolicyDigest,
				"compiler_version": compiled.Lineage.CompilerVersion,
			},
		}})
		if validateErr != nil {
			s.writeStoreError(w, validateErr)
			return
		}
		*artifact = validated
	}
	compiled.ReleaseSet.ArtifactIDs = []string{compiled.RouteArtifact.ID, compiled.DNSArtifact.ID, compiled.TLSArtifact.ID}
	releaseArtifact := platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet, compiled.ReleaseSet.ArtifactIDs, time.Now().UTC())
	releaseArtifact.CreatedByType = strings.TrimSpace(principal.ActorType)
	releaseArtifact.CreatedByID = strings.TrimSpace(principal.ActorID)
	storedRelease, _, storeErr := s.store.EnsurePlatformArtifact(releaseArtifact)
	if storeErr != nil {
		s.writeStoreError(w, storeErr)
		return
	}
	if referenceResult := s.validateReleaseSetReferences(storedRelease); !referenceResult.Pass {
		httpx.WriteError(w, http.StatusConflict, referenceResult.Message)
		return
	}
	validatedRelease, validateErr := s.store.ValidatePlatformArtifact(storedRelease.ID, []model.PlatformArtifactValidationResult{{
		Name:     "platform_config.release_set",
		Pass:     true,
		Severity: model.RobustnessSeverityBlockPublish,
		Message:  "route, DNS, and TLS artifacts are bound to one immutable release set",
	}})
	if validateErr != nil {
		s.writeStoreError(w, validateErr)
		return
	}
	compiled.ReleaseArtifact = validatedRelease
	s.appendAudit(principal, "platform_config.compiled", "platform_release_set", compiled.ReleaseSet.Generation, "", map[string]string{
		"intent_digest":    compiled.Lineage.IntentDigest,
		"policy_digest":    compiled.Lineage.PolicyDigest,
		"compiler_version": compiled.Lineage.CompilerVersion,
		"release_artifact": compiled.ReleaseArtifact.ID,
	})
	httpx.WriteJSON(w, http.StatusCreated, platformConfigCompileResponse{
		Lineage:         compiled.Lineage,
		ReleaseSet:      compiled.ReleaseSet,
		IntentArtifact:  compiled.IntentArtifact,
		PolicyArtifact:  compiled.PolicyArtifact,
		RouteArtifact:   compiled.RouteArtifact,
		DNSArtifact:     compiled.DNSArtifact,
		TLSArtifact:     compiled.TLSArtifact,
		ReleaseArtifact: compiled.ReleaseArtifact,
	})
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
	result, err := platformconfig.ImportEnvironment(env, generation)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, result)
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

package api

import (
	"errors"
	"net/http"
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
	httpx.WriteJSON(w, http.StatusOK, platformConfigLineageResponse{
		Artifact: artifact,
		Lineage:  platformconfig.LineageFromArtifact(artifact),
		LKG:      lkg,
	})
}

func isStoreNotFound(err error) bool {
	return errors.Is(err, store.ErrNotFound)
}

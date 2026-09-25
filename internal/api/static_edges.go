package api

import (
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

type createStaticEdgeRegistrationRequest struct {
	TenantID               string `json:"tenant_id"`
	ProjectID              string `json:"project_id"`
	Name                   string `json:"name"`
	EdgeID                 string `json:"edge_id"`
	Transport              string `json:"transport"`
	ManagerURL             string `json:"manager_url"`
	CertificateFingerprint string `json:"certificate_fingerprint"`
	SigningKeyID           string `json:"signing_key_id"`
	PossessionProofDigest  string `json:"possession_proof_digest"`
}

type updateStaticEdgePossessionProofRequest struct {
	PossessionProofDigest string `json:"possession_proof_digest"`
	SigningKeyID          string `json:"signing_key_id"`
	Ready                 bool   `json:"ready"`
}

func canReadStaticEdges(principal model.Principal) bool {
	return principal.IsPlatformAdmin() || principal.HasScope("static_edge.read") || principal.HasScope("static_edge.write")
}

func canWriteStaticEdges(principal model.Principal) bool {
	return principal.IsPlatformAdmin() || principal.HasScope("static_edge.write")
}

func (s *Server) handleListStaticEdgeRegistrations(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canReadStaticEdges(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing static_edge.read or static_edge.write scope")
		return
	}
	tenantID := principal.TenantID
	if principal.IsPlatformAdmin() {
		tenantID = strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	}
	projectID := strings.TrimSpace(r.URL.Query().Get("project_id"))
	if !principal.IsPlatformAdmin() {
		projectID = projectIDForPrincipal(principal, projectID)
		if projectID != "" && !principal.AllowsProject(projectID) {
			httpx.WriteError(w, http.StatusForbidden, "project is outside the credential boundary")
			return
		}
	}
	registrations, err := s.store.ListStaticEdgeRegistrations(store.StaticEdgeRegistrationFilter{
		TenantID: tenantID, ProjectID: projectID, PlatformAdmin: principal.IsPlatformAdmin(),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.StaticEdgeRegistrationListResponse{Registrations: registrations, GeneratedAt: time.Now().UTC()})
}

func (s *Server) handleCreateStaticEdgeRegistration(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canWriteStaticEdges(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing static_edge.write scope")
		return
	}
	var request createStaticEdgeRegistrationRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	projectID := strings.TrimSpace(request.ProjectID)
	if projectID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "project_id is required")
		return
	}
	project, err := s.store.GetProject(projectID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && !principalAllowsProject(principal, project) {
		httpx.WriteError(w, http.StatusForbidden, "project is outside the credential boundary")
		return
	}
	tenantID := project.TenantID
	if principal.IsPlatformAdmin() && strings.TrimSpace(request.TenantID) != "" {
		tenantID = strings.TrimSpace(request.TenantID)
	}
	if tenantID != project.TenantID || (!principal.IsPlatformAdmin() && tenantID != principal.TenantID) {
		httpx.WriteError(w, http.StatusForbidden, "tenant and project do not match the credential boundary")
		return
	}
	registration, err := s.store.CreateStaticEdgeRegistration(model.StaticEdgeRegistration{
		TenantID: tenantID, ProjectID: project.ID, Name: request.Name, EdgeID: request.EdgeID,
		Transport: request.Transport, ManagerURL: request.ManagerURL, CertificateFingerprint: request.CertificateFingerprint,
		SigningKeyID: request.SigningKeyID, PossessionProofDigest: request.PossessionProofDigest,
		Status: model.StaticEdgeStatusPending,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "static_edge.register", "static_edge", registration.ID, registration.TenantID, map[string]string{
		"project_id": registration.ProjectID, "edge_id": registration.EdgeID, "transport": registration.Transport,
	})
	httpx.WriteJSON(w, http.StatusCreated, model.StaticEdgeRegistrationResponse{Registration: registration})
}

func (s *Server) loadAuthorizedStaticEdge(w http.ResponseWriter, r *http.Request, principal model.Principal) (model.StaticEdgeRegistration, bool) {
	registration, err := s.store.GetStaticEdgeRegistration(r.PathValue("id"), principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return model.StaticEdgeRegistration{}, false
	}
	if !principal.IsPlatformAdmin() && !principal.AllowsProject(registration.ProjectID) {
		httpx.WriteError(w, http.StatusForbidden, "static edge is outside the credential boundary")
		return model.StaticEdgeRegistration{}, false
	}
	return registration, true
}

func (s *Server) handleGetStaticEdgeRegistration(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canReadStaticEdges(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing static_edge.read or static_edge.write scope")
		return
	}
	registration, ok := s.loadAuthorizedStaticEdge(w, r, principal)
	if !ok {
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.StaticEdgeRegistrationResponse{Registration: registration})
}

func (s *Server) handleUpdateStaticEdgePossessionProof(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canWriteStaticEdges(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing static_edge.write scope")
		return
	}
	registration, ok := s.loadAuthorizedStaticEdge(w, r, principal)
	if !ok {
		return
	}
	var request updateStaticEdgePossessionProofRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	updated, err := s.store.UpdateStaticEdgeRegistrationProof(registration.ID, registration.TenantID, request.PossessionProofDigest, request.SigningKeyID, request.Ready)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "static_edge.proof.update", "static_edge", updated.ID, updated.TenantID, map[string]string{
		"project_id": updated.ProjectID, "status": updated.Status,
	})
	httpx.WriteJSON(w, http.StatusOK, model.StaticEdgeRegistrationResponse{Registration: updated})
}

func (s *Server) handleRevokeStaticEdgeRegistration(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canWriteStaticEdges(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing static_edge.write scope")
		return
	}
	registration, ok := s.loadAuthorizedStaticEdge(w, r, principal)
	if !ok {
		return
	}
	revoked, err := s.store.RevokeStaticEdgeRegistration(registration.ID, registration.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "static_edge.revoke", "static_edge", revoked.ID, revoked.TenantID, map[string]string{
		"project_id": revoked.ProjectID, "edge_id": revoked.EdgeID,
	})
	httpx.WriteJSON(w, http.StatusOK, model.StaticEdgeRegistrationResponse{Registration: revoked})
}

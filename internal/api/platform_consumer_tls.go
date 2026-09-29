package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"

	"fugue/internal/auth"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeartifact"
)

func (s *Server) handleGetPlatformConsumerTLSCertificate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	claims, ok := auth.PlatformComponentIdentityFromContext(r.Context())
	if !ok || claims.Component != model.PlatformConsumerComponentEdgeWorker || claims.AuthorityID == "" || claims.ScopeKey != platformconfig.AuthorityCellScope(claims.AuthorityID) {
		httpx.WriteError(w, http.StatusForbidden, "scoped Edge Worker identity required")
		return
	}
	host := normalizeExternalAppDomain(r.PathValue("hostname"))
	setIDs := r.URL.Query()["expected_consumer_set_id"]
	if host == "" || host != r.PathValue("hostname") || len(setIDs) != 1 || strings.TrimSpace(setIDs[0]) == "" || r.PathValue("artifact_id") == "" {
		httpx.WriteError(w, http.StatusBadRequest, "exact hostname, artifact and expected set required")
		return
	}
	lookup, ref, err := s.servingTLSCertificateOwner(claims, r.PathValue("artifact_id"), setIDs[0], host)
	if err != nil {
		httpx.WriteError(w, http.StatusNotFound, "serving certificate assignment unavailable")
		return
	}
	domain, err := s.store.GetAppDomain(host)
	if err != nil || domain.Status != model.AppDomainStatusVerified || domain.AppID != ref.AppID || domain.TenantID != ref.TenantID {
		httpx.WriteError(w, http.StatusNotFound, "verified certificate owner unavailable")
		return
	}
	certificate, err := s.store.GetEdgeTLSCertificate(host)
	if err != nil || certificate.Hostname != host || certificate.AppID != ref.AppID || certificate.TenantID != ref.TenantID || certificate.CertificatePEM == "" || certificate.PrivateKeyPEM == "" {
		httpx.WriteError(w, http.StatusNotFound, "authorized certificate unavailable")
		return
	}
	// No cached assignment can disclose credentials after a serving authority
	// or domain owner change observed during this request.
	current, currentRef, err := s.servingTLSCertificateOwner(claims, r.PathValue("artifact_id"), setIDs[0], host)
	currentDomain, domainErr := s.store.GetAppDomain(host)
	if err != nil || domainErr != nil || !reflect.DeepEqual(lookup.Assignment, current.Assignment) || !reflect.DeepEqual(lookup.Release, current.Release) || !reflect.DeepEqual(ref, currentRef) || currentDomain.Status != model.AppDomainStatusVerified || currentDomain.AppID != ref.AppID || currentDomain.TenantID != ref.TenantID {
		httpx.WriteError(w, http.StatusConflict, "serving certificate authorization changed")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"certificate": certificate})
}

func (s *Server) servingTLSCertificateOwner(claims platformcontrol.PlatformComponentIdentityClaims, artifactID, setID, host string) (consumerArtifactLookup, routeartifact.CertificateOwner, error) {
	fail := errors.New("certificate is not owned by current serving cell assignment")
	read := newConsumerArtifactReader(s.store.GetPlatformArtifact)
	resolved, err := s.resolvePlatformConsumerAssignmentsWithReader(claims, read)
	if err != nil {
		return consumerArtifactLookup{}, routeartifact.CertificateOwner{}, fail
	}
	for _, item := range resolved {
		if item.Artifact.ID != artifactID || item.Artifact.ArtifactKind != model.PlatformArtifactKindCaddyRouteConfig || item.Assignment.ExpectedConsumerSetID != setID || item.Release.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow {
			continue
		}
		projection, found, err := s.edgeRouteIntentSnapshotFromTrafficScope(claims.AuthorityID, claims.ScopeKey, read)
		if err != nil || !found || projection.TrafficRelease == nil || projection.TrafficRelease.ReleaseID != item.Release.ID || projection.TrafficRelease.FencingToken != item.Release.FencingToken || projection.TrafficRelease.ReleaseSetID != item.Assignment.ReleaseSetID {
			return consumerArtifactLookup{}, routeartifact.CertificateOwner{}, fail
		}
		bundle, err := routeartifact.MaterializeSnapshotForGroup(projection, claims.AuthorityID)
		if err != nil {
			return consumerArtifactLookup{}, routeartifact.CertificateOwner{}, fail
		}
		ref, err := certificateReferenceForRoutes(item.Artifact, bundle, host)
		return item, ref, err
	}
	return consumerArtifactLookup{}, routeartifact.CertificateOwner{}, fail
}

func certificateReferenceForRoutes(artifact model.PlatformArtifact, bundle model.EdgeRouteBundle, host string) (routeartifact.CertificateOwner, error) {
	var payload struct {
		Certificates []platformconfig.TLSIntent `json:"certificates"`
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil || json.Unmarshal(raw, &payload) != nil {
		return routeartifact.CertificateOwner{}, errors.New("signed certificate references invalid")
	}
	return routeartifact.CustomDomainCertificateOwner(payload.Certificates, bundle, host)
}

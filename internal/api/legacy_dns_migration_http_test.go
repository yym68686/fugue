package api

import (
	"net/http"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

// Historical compiler/migration fixtures may inspect standalone bundles.
// This adapter is test-only and is never registered by the serving router.
func (s *Server) handleMigrationDNSBundleForTest(w http.ResponseWriter, r *http.Request) {
	authContext, ok := s.authorizeEdgeRequest(w, r)
	if !ok {
		return
	}

	options, err := s.edgeDNSBundleOptionsFromRequest(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := authContext.constrain(&options.DNSNodeID, &options.EdgeGroupID); err != nil {
		httpx.WriteError(w, http.StatusForbidden, err.Error())
		return
	}
	allowed, err := s.enforceScopedDNSNode(authContext, options.DNSNodeID, options.EdgeGroupID, options.Zone)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !allowed {
		httpx.WriteError(w, http.StatusForbidden, "dns token cannot access another DNS zone")
		return
	}
	bundle, artifactOK, artifactErr := s.edgeDNSBundleArtifactForOptions(options, time.Now().UTC())
	s.recordEdgeDNSArtifactHandlerLookup(artifactOK, artifactErr)
	if artifactErr != nil {
		if s.log != nil {
			s.log.Printf("edge dns artifact rejected; retaining DNS node LKG: dns_node_id=%s edge_group_id=%s zone=%s err=%v", options.DNSNodeID, options.EdgeGroupID, options.Zone, artifactErr)
		}
		httpx.WriteError(w, http.StatusServiceUnavailable, "edge DNS artifact is unavailable; retain the current verified LKG")
		return
	}
	if artifactOK {
		writeEdgeDNSBundleResponse(w, bundle)
		return
	}
	if s.log != nil {
		s.log.Printf("edge dns artifact missing; retaining DNS node LKG: dns_node_id=%s edge_group_id=%s zone=%s", options.DNSNodeID, options.EdgeGroupID, options.Zone)
	}
	httpx.WriteError(w, http.StatusServiceUnavailable, "edge DNS artifact is unavailable; retain the current verified LKG")
}

func writeEdgeDNSBundleResponse(w http.ResponseWriter, bundle model.EdgeDNSBundle) {
	etag := edgeRouteBundleETag(bundle.Version)
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "private, no-cache")
	w.Header().Set("X-Fugue-DNS-Bundle-Version", bundle.Version)

	// DNS bundles carry signed validity windows. Re-send unchanged content so
	// DNS nodes can refresh valid_until instead of going stale behind a 304.
	httpx.WriteJSON(w, http.StatusOK, bundle)
}

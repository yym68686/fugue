package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"fugue/internal/auth"
	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

func (s *Server) handleGetPlatformConsumerDNSRouteSources(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	claims, ok := auth.PlatformComponentIdentityFromContext(r.Context())
	if !ok || claims.Component != model.PlatformConsumerComponentDNSServer || claims.AuthorityID == "" || claims.ScopeKey != platformconfig.AuthorityCellScope(claims.AuthorityID) {
		httpx.WriteError(w, http.StatusForbidden, "scoped DNS consumer identity required")
		return
	}
	ids := r.URL.Query()["expected_consumer_set_id"]
	if len(ids) != 1 || ids[0] == "" || strings.TrimSpace(ids[0]) != ids[0] || r.PathValue("artifact_id") == "" {
		httpx.WriteError(w, http.StatusBadRequest, "exact DNS artifact and expected set required")
		return
	}
	response, err := observeAssignedDNSRouteSources(r.Context(), func() (consumerArtifactLookup, error) {
		return s.assignedDNSRouteSourceOwner(claims, r.PathValue("artifact_id"), ids[0])
	}, s.store.ObserveDNSRouteSources)
	if err != nil {
		code := http.StatusServiceUnavailable
		if errors.Is(err, store.ErrConflict) {
			code = http.StatusConflict
		} else if errors.Is(err, store.ErrNotFound) {
			code = http.StatusNotFound
		}
		httpx.WriteError(w, code, "approved routing source observation unavailable")
		return
	}
	raw, err := json.Marshal(response)
	if err != nil || len(raw) > 16<<20 {
		httpx.WriteError(w, http.StatusServiceUnavailable, "routing source response exceeds bound")
		return
	}
	w.Header().Set("ETag", strconv.Quote(response.Snapshot.SelectionDigest))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func observeAssignedDNSRouteSources(ctx context.Context, lookup func() (consumerArtifactLookup, error), observe func(context.Context, model.PlatformArtifact) (model.PlatformDNSRouteSourceSnapshot, error)) (model.PlatformConsumerDNSRouteSourcesResponse, error) {
	before, err := lookup()
	if err != nil {
		return model.PlatformConsumerDNSRouteSourcesResponse{}, err
	}
	snapshot, err := observe(ctx, before.Artifact)
	if err != nil {
		return model.PlatformConsumerDNSRouteSourcesResponse{}, err
	}
	current, err := lookup()
	if err != nil || !reflect.DeepEqual(before.Assignment, current.Assignment) || !reflect.DeepEqual(before.Release, current.Release) || snapshot.DNSArtifactID != before.Artifact.ID || snapshot.DNSArtifactDigest != before.Artifact.ContentHash {
		return model.PlatformConsumerDNSRouteSourcesResponse{}, store.ErrConflict
	}
	return model.PlatformConsumerDNSRouteSourcesResponse{Assignment: before.Assignment, Release: before.Release, Snapshot: snapshot}, nil
}

func (s *Server) assignedDNSRouteSourceOwner(claims platformcontrol.PlatformComponentIdentityClaims, artifactID, setID string) (consumerArtifactLookup, error) {
	items, err := s.resolvePlatformConsumerAssignmentsWithReader(claims, newConsumerArtifactReader(s.store.GetPlatformArtifact))
	if err != nil {
		return consumerArtifactLookup{}, err
	}
	for _, item := range items {
		if item.Artifact.ID != artifactID || item.Artifact.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || item.Assignment.ExpectedConsumerSetID != setID {
			continue
		}
		sources, err := platformconfig.DNSRouteSourceAuthorizations(item.Artifact)
		if err != nil || len(sources) == 0 {
			return consumerArtifactLookup{}, store.ErrNotFound
		}
		// For serving reads the same exact release must still be selected for
		// this DNS authority. Shadow remains a validation-only observation.
		if item.Release.ReleaseChannel != "shadow" {
			selected, err := s.consumerAssignmentIsSelected(item, claims.AuthorityID, newConsumerArtifactReader(s.store.GetPlatformArtifact))
			if err != nil || !selected {
				return consumerArtifactLookup{}, store.ErrNotFound
			}
		}
		return item, nil
	}
	return consumerArtifactLookup{}, store.ErrNotFound
}

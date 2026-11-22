package api

import (
	"context"
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
	loadArtifact := s.store.NewPlatformArtifactReadSessionContext(r.Context())
	observeSources := s.store.NewDNSRouteSourceObserver()
	response, err := observeAssignedDNSRouteSources(r.Context(), func() (consumerArtifactLookup, error) {
		return s.assignedDNSRouteSourceOwnerContext(r.Context(), claims, r.PathValue("artifact_id"), ids[0], loadArtifact)
	}, func(ctx context.Context, owner consumerArtifactLookup) (model.PlatformDNSRouteSourceSnapshot, error) {
		dns := owner.Artifact
		snapshot, err := observeSources(ctx, dns)
		if err != nil {
			return model.PlatformDNSRouteSourceSnapshot{}, err
		}
		// Admit the inputs of the original exact owner, not an owner
		// resolved midway through the observation. The outer read checks
		// that owner again after both closure admission and ledger reread.
		parent, err := loadArtifact(owner.Assignment.ReleaseSetID)
		if err != nil {
			return model.PlatformDNSRouteSourceSnapshot{}, err
		}
		reads := append(dnsSourceInputReadSelections(snapshot), consumerInputReadSelection(owner, parent))
		if err := s.store.CheckPlatformInputRead(ctx, reads); err != nil {
			return model.PlatformDNSRouteSourceSnapshot{}, err
		}
		current, err := observeSources(ctx, dns)
		if err != nil || current.SelectionDigest != snapshot.SelectionDigest {
			return model.PlatformDNSRouteSourceSnapshot{}, store.ErrConflict
		}
		return snapshot, nil
	})
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
	if revalidateDNSSourceResponse(w, r, response, nil) {
		return
	}
	raw, err := json.Marshal(response)
	if err != nil || len(raw) > 16<<20 {
		httpx.WriteError(w, http.StatusServiceUnavailable, "routing source response exceeds bound")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func observeAssignedDNSRouteSources(ctx context.Context, lookup func() (consumerArtifactLookup, error), observe func(context.Context, consumerArtifactLookup) (model.PlatformDNSRouteSourceSnapshot, error)) (model.PlatformConsumerDNSRouteSourcesResponse, error) {
	before, err := lookup()
	if err != nil {
		return model.PlatformConsumerDNSRouteSourcesResponse{}, err
	}
	snapshot, err := observe(ctx, before)
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
	return s.assignedDNSRouteSourceOwnerWithReader(claims, artifactID, setID, s.store.GetPlatformArtifact)
}

func (s *Server) assignedDNSRouteSourceOwnerWithReader(claims platformcontrol.PlatformComponentIdentityClaims, artifactID, setID string, loadArtifact func(string) (model.PlatformArtifact, error)) (consumerArtifactLookup, error) {
	return s.assignedDNSRouteSourceOwnerContext(context.Background(), claims, artifactID, setID, loadArtifact)
}

func (s *Server) assignedDNSRouteSourceOwnerContext(ctx context.Context, claims platformcontrol.PlatformComponentIdentityClaims, artifactID, setID string, loadArtifact func(string) (model.PlatformArtifact, error)) (consumerArtifactLookup, error) {
	// This endpoint is a lookup by exact expected set, not a discovery
	// request. Use its ledger row ONLY to locate a lane; it never grants
	// membership. Independently resolve that active lane's signed closure,
	// latest expected set and selected serving owner on each side of the read.
	// A stale set or superseded row cannot resurrect an earlier assignment.
	set, err := s.store.GetPlatformExpectedConsumerSetContext(ctx, setID)
	if err != nil {
		return consumerArtifactLookup{}, err
	}
	if set.ID != setID || set.ScopeKey != claims.ScopeKey || set.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || set.ArtifactReleaseID == "" {
		return consumerArtifactLookup{}, store.ErrNotFound
	}
	lane, err := s.store.GetPlatformArtifactReleaseContext(ctx, set.ArtifactReleaseID)
	if err != nil {
		return consumerArtifactLookup{}, err
	}
	if lane.ScopeKey != set.ScopeKey || lane.ID != set.ArtifactReleaseID || lane.ArtifactID != set.ReleaseSetID || lane.ArtifactKind != model.PlatformArtifactKindReleaseSet || lane.Status != model.PlatformArtifactReleaseStatusActive {
		return consumerArtifactLookup{}, store.ErrNotFound
	}
	switch lane.ReleaseChannel {
	case model.PlatformArtifactReleaseChannelShadow, model.PlatformArtifactReleaseChannelGray, model.PlatformArtifactReleaseChannelFull:
	default:
		return consumerArtifactLookup{}, store.ErrNotFound
	}
	readArtifact := newConsumerArtifactReader(loadArtifact)
	items, err := s.resolveConsumerAssignmentChannels(ctx, claims, readArtifact, []string{lane.ReleaseChannel})
	if err != nil {
		return consumerArtifactLookup{}, err
	}
	for _, item := range items {
		if item.Release.ID != lane.ID || item.Assignment.ReleaseSetID != set.ReleaseSetID || item.Artifact.ID != artifactID || item.Artifact.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || item.Assignment.ExpectedConsumerSetID != setID {
			continue
		}
		sources, err := platformconfig.DNSRouteSourceAuthorizations(item.Artifact)
		if err != nil || len(sources) == 0 {
			return consumerArtifactLookup{}, store.ErrNotFound
		}
		// For serving reads the same exact release must still be selected for
		// this DNS authority. Shadow remains a validation-only observation.
		if item.Release.ReleaseChannel != "shadow" {
			selected, err := s.consumerAssignmentIsSelected(item, claims.AuthorityID, newConsumerArtifactReader(loadArtifact))
			if err != nil || !selected {
				return consumerArtifactLookup{}, store.ErrNotFound
			}
		}
		return item, nil
	}
	return consumerArtifactLookup{}, store.ErrNotFound
}

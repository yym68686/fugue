package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"slices"
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

func (s *Server) handleGetPlatformConsumerRetainedDNSRouteSources(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	claims, ok := auth.PlatformComponentIdentityFromContext(r.Context())
	if !ok || claims.Component != model.PlatformConsumerComponentDNSServer || claims.AuthorityID == "" || claims.ScopeKey != platformconfig.AuthorityCellScope(claims.AuthorityID) || !slices.Contains(claims.ArtifactKinds, model.PlatformArtifactKindDNSAnswerBundle) {
		httpx.WriteError(w, http.StatusForbidden, "scoped DNS consumer identity required")
		return
	}
	ids := r.URL.Query()["expected_consumer_set_id"]
	if len(ids) != 1 || ids[0] == "" || strings.TrimSpace(ids[0]) != ids[0] || r.PathValue("artifact_id") == "" {
		httpx.WriteError(w, http.StatusBadRequest, "exact DNS artifact and expected set required")
		return
	}
	response, err := observeRetainedDNSRouteSources(r.Context(), func() (consumerArtifactLookup, model.PlatformLKGSnapshot, error) {
		return s.retainedDNSRouteSourceOwner(claims, r.PathValue("artifact_id"), ids[0])
	}, s.store.ObserveDNSRouteSources)
	if err != nil {
		code := http.StatusServiceUnavailable
		if errors.Is(err, store.ErrConflict) {
			code = http.StatusConflict
		} else if errors.Is(err, store.ErrNotFound) {
			code = http.StatusNotFound
		}
		httpx.WriteError(w, code, "retained DNS source observation unavailable")
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

func observeRetainedDNSRouteSources(ctx context.Context, lookup func() (consumerArtifactLookup, model.PlatformLKGSnapshot, error), observe func(context.Context, model.PlatformArtifact) (model.PlatformDNSRouteSourceSnapshot, error)) (model.PlatformConsumerRetainedDNSRouteSourcesResponse, error) {
	before, lkg, err := lookup()
	if err != nil {
		return model.PlatformConsumerRetainedDNSRouteSourcesResponse{}, err
	}
	snapshot, err := observe(ctx, before.Artifact)
	if err != nil {
		return model.PlatformConsumerRetainedDNSRouteSourcesResponse{}, err
	}
	current, currentLKG, err := lookup()
	if err != nil || !reflect.DeepEqual(before.Assignment, current.Assignment) || !reflect.DeepEqual(before.Release, current.Release) || !reflect.DeepEqual(lkg, currentLKG) || snapshot.DNSArtifactID != before.Artifact.ID || snapshot.DNSArtifactDigest != before.Artifact.ContentHash {
		return model.PlatformConsumerRetainedDNSRouteSourcesResponse{}, store.ErrConflict
	}
	return model.PlatformConsumerRetainedDNSRouteSourcesResponse{PlatformConsumerDNSRouteSourcesResponse: model.PlatformConsumerDNSRouteSourcesResponse{Assignment: before.Assignment, Release: before.Release, Snapshot: snapshot}, RetainedLKG: lkg}, nil
}

// The positive LKG is a separate read authority. A pending gray/full assignment
// cannot lend its policy to an old DNS artifact or revoke its read just by being
// selected. Conversely, arbitrary historical full releases grant no access.
func (s *Server) retainedDNSRouteSourceOwner(claims platformcontrol.PlatformComponentIdentityClaims, artifactID, setID string) (consumerArtifactLookup, model.PlatformLKGSnapshot, error) {
	fail := func() (consumerArtifactLookup, model.PlatformLKGSnapshot, error) {
		return consumerArtifactLookup{}, model.PlatformLKGSnapshot{}, store.ErrNotFound
	}
	lkg, err := s.store.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, claims.ScopeKey)
	if err != nil {
		return consumerArtifactLookup{}, model.PlatformLKGSnapshot{}, err
	}
	if lkg == nil {
		return fail()
	}
	read := newConsumerArtifactReader(s.store.GetPlatformArtifact)
	parent, err := read(lkg.ArtifactID)
	if err != nil || parent.ScopeKey != claims.ScopeKey || parent.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluatePlatformLKGSnapshot(*lkg, parent, s.bundleKeyring(), time.Now().UTC()).Pass || !validateReleaseSetReferences(parent, read).Pass {
		return fail()
	}
	release, err := s.store.GetPlatformArtifactRelease(lkg.VerifiedByReleaseID)
	if err != nil || release.ArtifactID != parent.ID || release.ArtifactKind != parent.ArtifactKind || release.ScopeKey != claims.ScopeKey || release.Generation != parent.Generation || release.FencingToken <= 0 || release.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || (release.Status != model.PlatformArtifactReleaseStatusActive && release.Status != model.PlatformArtifactReleaseStatusSuperseded) || release.VerificationState != model.PlatformArtifactVerificationStateVerified || release.VerifiedLKGGeneration != parent.Generation || release.VerifiedAt == nil {
		return fail()
	}
	kind := model.PlatformArtifactKindDNSAnswerBundle
	sets, err := s.store.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{ReleaseSetID: parent.ID, ArtifactReleaseID: release.ID, ArtifactKind: kind, ScopeKey: claims.ScopeKey})
	if err != nil {
		return consumerArtifactLookup{}, model.PlatformLKGSnapshot{}, err
	}
	var latest *model.PlatformExpectedConsumerSet
	for i := range sets {
		if latest == nil || sets[i].Revision > latest.Revision {
			latest = &sets[i]
		}
	}
	if latest == nil || latest.ID != setID || platformcontrol.ValidateDeclaredTrafficConsumerSet(parent, *latest) != nil {
		return fail()
	}
	child, err := consumerAssignmentChild(parent, kind, read)
	if err != nil || child.ID != artifactID || child.ScopeKey != claims.ScopeKey || child.Status != model.PlatformArtifactStatusValidated || child.Generation != latest.ExpectedGeneration || s.store.VerifyPlatformArtifactIntegrity(child) != nil {
		return fail()
	}
	sources, err := platformconfig.DNSRouteSourceAuthorizations(child)
	if err != nil || len(sources) == 0 {
		return fail()
	}
	for _, expected := range platformcontrol.ProjectExpectedConsumerOwners(*latest).Consumers {
		if !platformcontrol.ExpectedConsumerIdentityMatches(expected, claims) || expected.ScopeKey != claims.ScopeKey || expected.ArtifactKind != kind || child.Generation != expected.ExpectedGeneration {
			continue
		}
		assignment := model.PlatformConsumerAssignment{ExpectedConsumerSetID: latest.ID, ReleaseSetID: parent.ID, ArtifactReleaseID: release.ID, ArtifactKind: kind, ScopeKey: claims.ScopeKey, ExpectedGeneration: child.Generation, Revision: latest.Revision, ArtifactID: child.ID, ContentHash: child.ContentHash, GenerationSequence: child.GenerationSequence, ReleaseChannel: release.ReleaseChannel, FencingToken: release.FencingToken, ExpectedProtocolVersion: expected.ExpectedProtocolVersion, ExpectedSchemaVersion: expected.ExpectedSchemaVersion, CompatibilityCapabilities: append([]string(nil), expected.CompatibilityCapabilities...), HeartbeatDeadline: expected.HeartbeatDeadline, ConvergenceDeadline: expected.ConvergenceDeadline}
		return consumerArtifactLookup{Artifact: child, Assignment: assignment, Release: release}, *lkg, nil
	}
	return fail()
}

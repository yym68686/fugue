package platformconsumer

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"

	"fugue/internal/dnsroutewire"
	"fugue/internal/model"
)

// Source observation includes two coherent ledger reads, complete input
// admission and assignment rechecks. Keep that bounded work separate from the
// small interactive request budget; an earlier caller deadline still wins.
const dnsRouteSourceReadTimeout = 45 * time.Second

// DNSRouteSources observes the routing ledger bound to this exact DNS
// assignment. Signature/policy validation belongs to the DNS consumer.
func (c Client) DNSRouteSources(ctx context.Context, id Identity, a model.PlatformConsumerAssignment, r model.PlatformArtifactRelease, previous ...model.PlatformDNSRouteSourceSnapshot) (model.PlatformDNSRouteSourceSnapshot, error) {
	id, err := c.RefreshIdentity(ctx, id)
	if err != nil {
		return model.PlatformDNSRouteSourceSnapshot{}, err
	}
	var result model.PlatformConsumerDNSRouteSourcesResponse
	validator := ""
	var unchanged func(http.Header) error
	if len(previous) == 1 && previous[0].DNSArtifactID == a.ArtifactID && previous[0].DNSArtifactDigest == a.ContentHash {
		prior := model.PlatformConsumerDNSRouteSourcesResponse{Assignment: a, Release: r, Snapshot: previous[0]}
		validator, _ = dnsroutewire.Validator(prior, nil)
		unchanged = func(headers http.Header) error {
			observed, err := freshDNSObservation(headers, prior.Snapshot.ObservedAt)
			if err == nil {
				result = prior
				result.Snapshot = ownDNSSourceSnapshotEnvelopes(prior.Snapshot)
				result.Snapshot.ObservedAt = observed
			}
			return err
		}
	}
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/v1/platform-state/consumers/artifacts/" + url.PathEscape(a.ArtifactID) + "/route-sources?expected_consumer_set_id=" + url.QueryEscape(a.ExpectedConsumerSetID)
	if err := c.jsonRevalidatedBudget(ctx, endpoint, id.Token, http.MethodGet, nil, &result, 16<<20, validator, unchanged, dnsRouteSourceReadTimeout); err != nil {
		var status *responseStatusError
		if errors.As(err, &status) && status.Code == http.StatusConflict {
			return result.Snapshot, ErrAssignmentChanged
		}
		if status != nil && status.Code == http.StatusNotFound {
			// A rollback can deselect gray between the two source reads. The
			// endpoint then returns 404, not a source snapshot conflict. Confirm
			// current selection before classifying it as a bounded retry; absent
			// source data, failed rechecks and authorization errors remain errors.
			check := c.CheckServingAssignment
			if a.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow {
				check = c.CheckAssignment
			}
			if changed := check(ctx, id, a); errors.Is(changed, ErrAssignmentChanged) {
				return result.Snapshot, ErrAssignmentChanged
			}
		}
		return result.Snapshot, err
	}
	if !reflect.DeepEqual(result.Assignment, a) || !reflect.DeepEqual(result.Release, r) {
		return result.Snapshot, ErrAssignmentChanged
	}
	return result.Snapshot, nil
}

// RetainedDNSRouteSources reads only the authority's current positive LKG.
// Verification metadata may advance after the cached release was applied; its
// immutable publication binding and exact expected membership must not change.
func (c Client) RetainedDNSRouteSources(ctx context.Context, id Identity, a model.PlatformConsumerAssignment, r model.PlatformArtifactRelease, previous ...model.PlatformConsumerRetainedDNSRouteSourcesResponse) (model.PlatformConsumerRetainedDNSRouteSourcesResponse, error) {
	id, err := c.RefreshIdentity(ctx, id)
	if err != nil {
		return model.PlatformConsumerRetainedDNSRouteSourcesResponse{}, err
	}
	var result model.PlatformConsumerRetainedDNSRouteSourcesResponse
	validator := ""
	var unchanged func(http.Header) error
	if len(previous) == 1 && reflect.DeepEqual(previous[0].Assignment, a) && previous[0].Release.ID == r.ID {
		prior := previous[0]
		validator, _ = dnsroutewire.Validator(prior.PlatformConsumerDNSRouteSourcesResponse, &prior.RetainedLKG)
		unchanged = func(headers http.Header) error {
			observed, err := freshDNSObservation(headers, prior.Snapshot.ObservedAt)
			if err == nil {
				result = prior
				result.Snapshot = ownDNSSourceSnapshotEnvelopes(prior.Snapshot)
				result.Snapshot.ObservedAt = observed
			}
			return err
		}
	}
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/v1/platform-state/consumers/artifacts/" + url.PathEscape(a.ArtifactID) + "/retained-route-sources?expected_consumer_set_id=" + url.QueryEscape(a.ExpectedConsumerSetID)
	if err := c.jsonRevalidatedBudget(ctx, endpoint, id.Token, http.MethodGet, nil, &result, 16<<20, validator, unchanged, dnsRouteSourceReadTimeout); err != nil {
		return result, err
	}
	got := result.Release
	if !reflect.DeepEqual(result.Assignment, a) || got.ID != r.ID || got.ArtifactID != r.ArtifactID || got.ArtifactKind != r.ArtifactKind || got.ScopeKey != r.ScopeKey || got.Generation != r.Generation || got.ReleaseChannel != r.ReleaseChannel || got.FencingToken != r.FencingToken || !got.ReleasedAt.Equal(r.ReleasedAt) || got.CanaryRuleRef != r.CanaryRuleRef || got.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || (got.Status != model.PlatformArtifactReleaseStatusActive && got.Status != model.PlatformArtifactReleaseStatusSuperseded) || got.VerificationState != model.PlatformArtifactVerificationStateVerified || got.VerifiedLKGGeneration != r.Generation || got.VerifiedAt == nil || result.RetainedLKG.VerifiedByReleaseID != r.ID {
		return result, ErrAssignmentChanged
	}
	return result, nil
}

// Content is immutable and verified against actual bytes on both sides. Own
// the small containing slices so a consumer can replace content references
// without writing a previously published serving snapshot's envelopes.
func ownDNSSourceSnapshotEnvelopes(snapshot model.PlatformDNSRouteSourceSnapshot) model.PlatformDNSRouteSourceSnapshot {
	snapshot.Scopes = slices.Clone(snapshot.Scopes)
	for i := range snapshot.Scopes {
		scope := &snapshot.Scopes[i]
		scope.Lanes = slices.Clone(scope.Lanes)
		scope.Publications = slices.Clone(scope.Publications)
		for j := range scope.Publications {
			scope.Publications[j].Selections = slices.Clone(scope.Publications[j].Selections)
		}
	}
	return snapshot
}

func freshDNSObservation(headers http.Header, previous time.Time) (time.Time, error) {
	observed, err := time.Parse(time.RFC3339Nano, headers.Get(dnsroutewire.ObservedAtHeader))
	now := time.Now()
	if err != nil || observed.IsZero() || !observed.After(previous) || observed.After(now.Add(time.Second)) || now.Sub(observed) > time.Minute {
		return time.Time{}, errors.New("DNS source revalidation observation is not fresh")
	}
	return observed, nil
}

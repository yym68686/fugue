package platformconsumer

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"fugue/internal/model"
)

// DNSRouteSources observes the routing ledger bound to this exact DNS
// assignment. Signature/policy validation belongs to the DNS consumer.
func (c Client) DNSRouteSources(ctx context.Context, id Identity, a model.PlatformConsumerAssignment, r model.PlatformArtifactRelease) (model.PlatformDNSRouteSourceSnapshot, error) {
	var result model.PlatformConsumerDNSRouteSourcesResponse
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/v1/platform-state/consumers/artifacts/" + url.PathEscape(a.ArtifactID) + "/route-sources?expected_consumer_set_id=" + url.QueryEscape(a.ExpectedConsumerSetID)
	if err := c.jsonLimit(ctx, endpoint, id.Token, http.MethodGet, nil, &result, 16<<20); err != nil {
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
func (c Client) RetainedDNSRouteSources(ctx context.Context, id Identity, a model.PlatformConsumerAssignment, r model.PlatformArtifactRelease) (model.PlatformConsumerRetainedDNSRouteSourcesResponse, error) {
	var result model.PlatformConsumerRetainedDNSRouteSourcesResponse
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/v1/platform-state/consumers/artifacts/" + url.PathEscape(a.ArtifactID) + "/retained-route-sources?expected_consumer_set_id=" + url.QueryEscape(a.ExpectedConsumerSetID)
	if err := c.jsonLimit(ctx, endpoint, id.Token, http.MethodGet, nil, &result, 16<<20); err != nil {
		return result, err
	}
	got := result.Release
	if !reflect.DeepEqual(result.Assignment, a) || got.ID != r.ID || got.ArtifactID != r.ArtifactID || got.ArtifactKind != r.ArtifactKind || got.ScopeKey != r.ScopeKey || got.Generation != r.Generation || got.ReleaseChannel != r.ReleaseChannel || got.FencingToken != r.FencingToken || !got.ReleasedAt.Equal(r.ReleasedAt) || got.CanaryRuleRef != r.CanaryRuleRef || got.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || (got.Status != model.PlatformArtifactReleaseStatusActive && got.Status != model.PlatformArtifactReleaseStatusSuperseded) || got.VerificationState != model.PlatformArtifactVerificationStateVerified || got.VerifiedLKGGeneration != r.Generation || got.VerifiedAt == nil || result.RetainedLKG.VerifiedByReleaseID != r.ID {
		return result, ErrAssignmentChanged
	}
	return result, nil
}

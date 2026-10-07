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
		return result.Snapshot, err
	}
	if !reflect.DeepEqual(result.Assignment, a) || !reflect.DeepEqual(result.Release, r) {
		return result.Snapshot, ErrAssignmentChanged
	}
	return result.Snapshot, nil
}

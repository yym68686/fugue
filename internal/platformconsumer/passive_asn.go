package platformconsumer

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"fugue/internal/model"
)

func (c Client) PassiveASN(ctx context.Context, identity Identity, a model.PlatformConsumerAssignment) (model.PlatformArtifact, error) {
	var result model.PlatformArtifact
	base, err := url.Parse(strings.TrimSpace(c.BaseURL))
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil {
		return result, errors.New("platform API endpoint is invalid")
	}
	// The configured API origin can carry local authority metadata. It is not
	// a request query and must not absorb the artifact path or expected set.
	base.RawQuery, base.Fragment, base.RawFragment, base.ForceQuery = "", "", "", false
	endpoint := strings.TrimRight(base.String(), "/") + "/v1/platform-state/consumers/artifacts/" + url.PathEscape(a.ArtifactID) + "/passive-asn?expected_consumer_set_id=" + url.QueryEscape(a.ExpectedConsumerSetID)
	err = c.jsonLimit(ctx, endpoint, identity.Token, http.MethodGet, nil, &result, 4<<20)
	return result, err
}

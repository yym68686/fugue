package platformconsumer

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"fugue/internal/model"
)

func (c Client) PassiveASN(ctx context.Context, identity Identity, a model.PlatformConsumerAssignment) (model.PlatformArtifact, error) {
	var result model.PlatformArtifact
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/v1/platform-state/consumers/artifacts/" + url.PathEscape(a.ArtifactID) + "/passive-asn?expected_consumer_set_id=" + url.QueryEscape(a.ExpectedConsumerSetID)
	err := c.jsonLimit(ctx, endpoint, identity.Token, http.MethodGet, nil, &result, 4<<20)
	return result, err
}

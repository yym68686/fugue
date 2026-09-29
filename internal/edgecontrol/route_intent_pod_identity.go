package edgecontrol

import (
	"context"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/staticedgecontract"
)

const routeIntentIdentityPath = "/v1/platform-state/consumers/identity"

// Exchange on the same pinned private TLS origin as RouteIntent. Controls
// receive a short-lived cell-scoped token, never Core's signing material. A
// failed exchange cannot fall back to a local issuer or another endpoint.
func (client *RouteIntentClient) exchangePodCredential(ctx context.Context) (string, error) {
	raw, err := readPrivateProjectedFile(client.podTokenFile, 32768)
	if err != nil {
		return "", ErrRouteIntentCredential
	}
	defer zeroBytes(raw)
	token := strings.TrimSpace(string(raw))
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", ErrRouteIntentCredential
	}
	endpoint := *client.endpoint
	endpoint.Path, endpoint.RawPath, endpoint.RawQuery, endpoint.ForceQuery = routeIntentIdentityPath, "", "", false
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), nil)
	if err != nil {
		return "", ErrRouteIntentCredential
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "fugue-edge-control/pod-identity-v1")
	response, err := client.client.Do(request)
	if err != nil {
		return "", ErrRouteIntentCredential
	}
	defer response.Body.Close()
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != http.StatusOK || err != nil || contentType != "application/json" || response.Header.Get("Cache-Control") != "no-store" {
		return "", ErrRouteIntentCredential
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 32769))
	if err != nil || len(body) == 0 || len(body) > 32768 {
		return "", ErrRouteIntentCredential
	}
	defer zeroBytes(body)
	var identity struct {
		Token         string    `json:"token"`
		ExpiresAt     time.Time `json:"expires_at"`
		Component     string    `json:"component"`
		NodeID        string    `json:"node_id"`
		AuthorityID   string    `json:"authority_id"`
		ConsumerID    string    `json:"consumer_id"`
		ScopeKey      string    `json:"scope_key"`
		ArtifactKinds []string  `json:"artifact_kinds"`
	}
	if staticedgecontract.StrictJSON(body, &identity) != nil {
		return "", ErrRouteIntentCredential
	}
	wantID, err := platformcontrol.PlatformConsumerID(model.PlatformConsumerComponentEdgeControl, client.nodeID, client.groupID)
	now := client.now()
	if err != nil || identity.Token == "" || len(identity.Token) > 16384 || strings.ContainsAny(identity.Token, " \t\r\n") ||
		identity.Component != model.PlatformConsumerComponentEdgeControl || identity.NodeID != client.nodeID || identity.AuthorityID != client.groupID || identity.ConsumerID != wantID ||
		identity.ScopeKey != "global" || len(identity.ArtifactKinds) != 1 || identity.ArtifactKinds[0] != model.PlatformArtifactKindEdgeRouteIntent ||
		!identity.ExpiresAt.After(now.Add(10*time.Second)) || identity.ExpiresAt.After(now.Add(2*time.Minute+platformcontrol.PlatformComponentIdentityFutureSkew)) {
		return "", ErrRouteIntentCredential
	}
	return identity.Token, nil
}

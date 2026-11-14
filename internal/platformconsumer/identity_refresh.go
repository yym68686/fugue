package platformconsumer

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"
)

// RefreshIdentity renews only authentication, never an assignment or evidence.
// Long observations must not outlive their short-lived component credential.
// Reissue through the live Pod credential and require identical authorization;
// a rejection is final, not a reason to retry a heartbeat or extend token TTL.
func (c Client) RefreshIdentity(ctx context.Context, before Identity) (Identity, error) {
	// Opaque bearer-only callers retain their existing transport behavior.
	// Identity discovery always supplies and validates an explicit expiry.
	const refreshMargin = 30 * time.Second
	if before.ExpiresAt.IsZero() || before.ExpiresAt.After(time.Now().Add(refreshMargin)) {
		return before, nil
	}
	if err := ctx.Err(); err != nil {
		return Identity{}, err
	}
	raw, err := ReadFile(c.TokenFile, 32768)
	if err != nil || len(raw) == 0 {
		return Identity{}, errors.New("platform Pod credential unavailable")
	}
	var after Identity
	if err := c.requestJSON(ctx, "/v1/platform-state/consumers/identity", strings.TrimSpace(string(raw)), http.MethodPost, nil, &after); err != nil {
		return Identity{}, err
	}
	wantedID := before.BoundConsumerID()
	oldKinds, newKinds := slices.Clone(before.ArtifactKinds), slices.Clone(after.ArtifactKinds)
	slices.Sort(oldKinds)
	slices.Sort(newKinds)
	if wantedID == "" || before.Token == "" || before.AuthorityID != c.AuthorityID ||
		after.AuthorityID != before.AuthorityID || after.Component != before.Component ||
		after.NodeID != before.NodeID || after.ScopeKey != before.ScopeKey ||
		after.ConsumerID != before.ConsumerID || (after.ConsumerID != "" && after.ConsumerID != wantedID) ||
		len(oldKinds) == 0 || !slices.Equal(oldKinds, newKinds) ||
		after.Token == "" || !after.ExpiresAt.After(time.Now().Add(refreshMargin)) {
		return Identity{}, errors.New("refreshed platform credential identity mismatch")
	}
	return after, nil
}

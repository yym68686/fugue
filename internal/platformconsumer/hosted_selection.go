package platformconsumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
	"fugue/internal/routeartifact"
	"fugue/internal/tlsissuance"
)

// Absence is never an instruction to remove an already selected host, to
// adopt an older fence, or to restore another host from an old global bundle.
var ErrNoHostedSelection = errors.New("no hosted decision for this receiver")

type hostedWireArtifact struct{ model.PlatformArtifact }

func (a *hostedWireArtifact) UnmarshalJSON(b []byte) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	return d.Decode(&a.PlatformArtifact)
}

// Reads a signed decision for exactly one host. Does not install it, emit
// an ACK, claim probe success or extend its issuer's deadline.
func (c Client) hostedReader(ctx context.Context, id Identity, serving model.PlatformConsumerAssignment) (Identity, string, error) {
	if id.BoundConsumerID() == "" || id.ScopeKey != "global" || serving.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || serving.ScopeKey != id.ScopeKey || serving.ExpectedConsumerSetID == "" || serving.ArtifactID == "" || !slices.Contains(id.ArtifactKinds, serving.ArtifactKind) {
		return id, "", errors.New("missing serving recipient")
	}
	id, e := c.RefreshIdentity(ctx, id)
	if e != nil {
		return id, "", e
	}
	base, e := url.Parse(strings.TrimSpace(c.BaseURL))
	if e != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" || base.User != nil {
		return id, "", errors.New("invalid serving origin")
	}
	base.RawQuery, base.Fragment, base.RawFragment, base.ForceQuery = "", "", "", false
	return id, strings.TrimRight(base.String(), "/") + "/v1/platform-state/consumers/artifacts/" + url.PathEscape(serving.ArtifactID) + "/host-selections", nil
}

func (c Client) HostedSelection(ctx context.Context, id Identity, serving model.PlatformConsumerAssignment, host string, knownFence int64, knownDecision string, keys bundleauth.Keyring) (routeartifact.HostedSelectedPacket, error) {
	var empty routeartifact.HostedSelectedPacket
	if !tlsissuance.ValidHostname(host) {
		return empty, errors.New("invalid hostname")
	}
	id, path, e := c.hostedReader(ctx, id, serving)
	if e != nil {
		return empty, e
	}
	enrollment, e := platformcontrol.ConsumerEnrollmentFromID(id.Component, id.NodeID, id.AuthorityID, id.ScopeKey, id.BoundConsumerID())
	if e != nil {
		return empty, e
	}
	who := platformcontrol.PlatformComponentIdentityClaims{Component: id.Component, NodeID: id.NodeID, ScopeKey: id.ScopeKey, AuthorityID: id.AuthorityID, ArtifactKinds: id.ArtifactKinds, WorkerEnrollmentID: enrollment}
	path += "/" + url.PathEscape(host) + "?expected_consumer_set_id=" + url.QueryEscape(serving.ExpectedConsumerSetID)
	var a hostedWireArtifact
	if e = c.jsonLimit(ctx, path, id.Token, http.MethodGet, nil, &a, 5<<20); e != nil {
		var status *responseStatusError
		if errors.As(e, &status) && status.Code == http.StatusNotFound {
			return empty, ErrNoHostedSelection
		}
		return empty, e
	}
	if e = c.CheckServingAssignment(ctx, id, serving); e != nil {
		return empty, e
	}
	return routeartifact.VerifyHostedPacket(a.PlatformArtifact, host, who, serving, knownFence, knownDecision, keys, time.Now().UTC())
}

// Bounded discovery. The caller owns the cursor, and re-fetches the signed
// command before any application. Missing hints never delete a local head.
func (c Client) HostedSelections(ctx context.Context, id Identity, serving model.PlatformConsumerAssignment, after string) (routeartifact.HostedHintPage, error) {
	var empty, page routeartifact.HostedHintPage
	if after != "" && !tlsissuance.ValidHostname(after) {
		return empty, errors.New("invalid host cursor")
	}
	id, path, e := c.hostedReader(ctx, id, serving)
	if e != nil {
		return empty, e
	}
	q := url.Values{"expected_consumer_set_id": {serving.ExpectedConsumerSetID}}
	if after != "" {
		q.Set("after", after)
	}
	if e = c.jsonLimit(ctx, path+"?"+q.Encode(), id.Token, http.MethodGet, nil, &page, 32<<10); e != nil {
		return empty, e
	}
	if page.Selections == nil || len(page.Selections) > routeartifact.HostedHintLimit {
		return empty, errors.New("invalid host selection page")
	}
	prev := after
	for _, h := range page.Selections {
		if h.Host <= prev || !tlsissuance.ValidHostname(h.Host) || h.Fence < 1 || !platformproducer.ValidDigest(h.Decision) {
			return empty, errors.New("invalid host selection hint")
		}
		prev = h.Host
	}
	if page.Next != "" && (page.Next != prev || len(page.Selections) != routeartifact.HostedHintLimit) {
		return empty, errors.New("nonadvancing host selection cursor")
	}
	if e = c.CheckServingAssignment(ctx, id, serving); e != nil {
		return empty, e
	}
	return page, nil
}

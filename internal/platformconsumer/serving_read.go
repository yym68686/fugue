package platformconsumer

import (
	"context"
	"crypto/sha256"
	"slices"
	"time"

	"fugue/internal/model"
)

type servingAssignmentSnapshot struct {
	identity   Identity
	credential [sha256.Size]byte
	base       string
	firstKind  string
	items      []model.PlatformConsumerAssignment
}

func (s *servingAssignmentSnapshot) matches(id Identity, credential [sha256.Size]byte, base, channel, kind string) bool {
	if s == nil || s.items == nil || channel != "serving" || s.base != base || s.credential != credential || kind == s.firstKind ||
		!s.identity.ExpiresAt.After(time.Now()) || len(s.identity.ArtifactKinds) != len(id.ArtifactKinds) ||
		s.identity.Component != id.Component || s.identity.NodeID != id.NodeID || s.identity.ScopeKey != id.ScopeKey ||
		s.identity.ConsumerID != id.ConsumerID || s.identity.AuthorityID != id.AuthorityID {
		return false
	}
	for _, allowed := range id.ArtifactKinds {
		if !slices.Contains(s.identity.ArtifactKinds, allowed) {
			return false
		}
	}
	if !slices.Contains(s.identity.ArtifactKinds, kind) {
		return false
	}
	// A missing sibling in the first list might have been published while the
	// first artifact was verified. Preserve its independent discovery in that
	// case. Ambiguity in a list we do reuse is still rejected by the selector.
	for _, item := range s.items {
		if item.ScopeKey == id.ScopeKey && item.ArtifactKind == kind &&
			(item.ReleaseChannel == "serving" || item.ReleaseChannel == "gray" || item.ReleaseChannel == "full") {
			return true
		}
	}
	return false
}

// NewServingRead returns a single-observation reader for two sibling kinds.
// Do not retain it across verification attempts or use it concurrently. Each
// download still exchanges the live Pod credential and checks the returned
// artifact envelope against the exact assignment. Rechecks use the Client.
func (c Client) NewServingRead(component, nodeID, scope string) *ServingRead {
	return &ServingRead{client: c, component: component, nodeID: nodeID, scope: scope}
}

type ServingRead struct {
	client                   Client
	component, nodeID, scope string
	snapshot                 *servingAssignmentSnapshot
	used                     bool
}

func (r *ServingRead) Sync(ctx context.Context, kind string) (Identity, model.PlatformConsumerAssignment, model.PlatformArtifact, model.PlatformArtifactRelease, error) {
	if r.used {
		snapshot := r.snapshot
		r.snapshot = nil
		return r.client.syncChannelWithSnapshot(ctx, r.component, r.nodeID, r.scope, kind, "serving", snapshot)
	}
	r.used = true
	var snapshot servingAssignmentSnapshot
	id, assignment, artifact, release, err := r.client.syncChannelWithSnapshot(ctx, r.component, r.nodeID, r.scope, kind, "serving", &snapshot)
	if err == nil && snapshot.items != nil {
		r.snapshot = &snapshot
	}
	return id, assignment, artifact, release, err
}

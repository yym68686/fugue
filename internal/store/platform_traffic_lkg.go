package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

// This is one transaction's recovery result, not a parallel release machine.
// All references keep the same verification release and evidence hash.
func trafficLKGMembers(state *model.State, parent model.PlatformArtifact, release model.PlatformArtifactRelease, keys bundleauth.Keyring, now time.Time) ([]model.PlatformArtifact, error) {
	if parent.ArtifactKind != model.PlatformArtifactKindReleaseSet {
		return nil, nil
	}
	fail := func() ([]model.PlatformArtifact, error) {
		return nil, fmt.Errorf("%w: traffic verified recovery binding is incomplete", ErrConflict)
	}
	if release.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow {
		return fail()
	}
	if err := validateFullReleaseSetInState(state, parent, keys, now); err != nil {
		return nil, err
	}
	// The evidence selector may not verify a different (newer) publication
	// while this call promotes an older release of the same artifact.
	for _, r := range state.PlatformArtifactReleases {
		if r.ArtifactKind == parent.ArtifactKind && r.ScopeKey == parent.ScopeKey && (r.ArtifactID == parent.ID || r.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow) && r.Status == model.PlatformArtifactReleaseStatusActive && r.ID != release.ID && !r.ReleasedAt.Before(release.ReleasedAt) {
			return fail()
		}
	}
	ids, ok := parent.Content["artifact_ids"].([]any)
	if !ok || len(ids) != 3 {
		return fail()
	}
	members := []model.PlatformArtifact{}
	kinds := map[string]bool{}
	var policy platformconfig.PolicySnapshot
	for _, id := range ids {
		index := platformArtifactIndex(state.PlatformArtifacts, fmt.Sprint(id))
		if index < 0 {
			return fail()
		}
		child := state.PlatformArtifacts[index]
		switch child.ArtifactKind {
		case model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig:
		default:
			return fail()
		}
		if kinds[child.ArtifactKind] {
			return fail()
		}
		kinds[child.ArtifactKind] = true
		members = append(members, child)
		var p platformconfig.PolicySnapshot
		raw, err := json.Marshal(child.Content["policy"])
		if err != nil || decodeTrafficLKGPolicy(raw, &p) != nil || platformconfig.ValidatePolicySnapshot(p) != nil {
			return fail()
		}
		digest, err := platformconfig.Digest(p)
		if err != nil || digest != parent.Metadata["policy_digest"] || p.Generation != parent.Metadata["policy_generation"] || p.Scope != parent.ScopeKey {
			return fail()
		}
		if policy.Generation != "" && !reflect.DeepEqual(policy, p) {
			return fail()
		}
		policy = p
	}
	var policyArtifact *model.PlatformArtifact
	for _, a := range state.PlatformArtifacts {
		if a.ArtifactKind == model.PlatformArtifactKindPolicySnapshot && a.ScopeKey == parent.ScopeKey && a.Generation == policy.Generation {
			if policyArtifact != nil && policyArtifact.ID != a.ID {
				return fail()
			}
			copy := a
			policyArtifact = &copy
		}
	}
	if policyArtifact == nil || policyArtifact.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(*policyArtifact, keys).Pass {
		return fail()
	}
	var storedPolicy platformconfig.PolicySnapshot
	raw, err := json.Marshal(policyArtifact.Content)
	if err != nil || decodeTrafficLKGPolicy(raw, &storedPolicy) != nil || !reflect.DeepEqual(policy, storedPolicy) || policyArtifact.Metadata["policy_digest"] != parent.Metadata["policy_digest"] {
		return fail()
	}
	members = append(members, *policyArtifact)
	sort.Slice(members, func(i, j int) bool { return members[i].ArtifactKind < members[j].ArtifactKind })
	return members, nil
}

func buildTrafficMemberLKGs(members []model.PlatformArtifact, releaseID, evidence string, now time.Time, keys bundleauth.Keyring) ([]model.PlatformLKGSnapshot, error) {
	out := make([]model.PlatformLKGSnapshot, 0, len(members))
	for _, a := range members {
		s, err := buildPlatformLKGSnapshot(a, releaseID, evidence, now, keys)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func decodeTrafficLKGPolicy(raw []byte, out *platformconfig.PolicySnapshot) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return ErrInvalidInput
	}
	return nil
}

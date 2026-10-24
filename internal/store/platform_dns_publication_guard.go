package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func dnsPublicationKey(a model.PlatformArtifact, req model.PlatformArtifactReleaseRequest) (string, error) {
	raw, err := json.Marshal(req.DNSPublicationPrecondition)
	if err != nil {
		return "", err
	}
	var guard map[string]any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if err := d.Decode(&guard); err != nil {
		return "", err
	}
	digest, err := platformconfig.Digest(map[string]any{"artifact_id": a.ID, "content_hash": a.ContentHash, "release_channel": req.ReleaseChannel, "canary_rule_ref": req.CanaryRuleRef, "precondition": guard})
	return "dns-publication/" + digest, err
}

func validDNSPublicationRef(ref model.PlatformPublicationPrecondition) bool {
	validID := func(id string) bool { return id != "" && len(id) <= 256 && !strings.ContainsAny(id, " \t\r\n\x00") }
	return validID(ref.ArtifactID) && validID(ref.ReleaseID) && platformproducer.ValidDigest(ref.ContentHash) && ref.FencingToken > 0
}

func dnsOnlyPublication(a model.PlatformArtifact) bool {
	role, _ := a.Content["publication_role"].(string)
	_, err := platformconfig.ValidateReleaseComposition(a)
	return a.ArtifactKind == model.PlatformArtifactKindReleaseSet && role == platformconfig.PublicationRoleCellDNS &&
		err == nil
}

func validateDNSPublicationGuardRequest(a model.PlatformArtifact, req model.PlatformArtifactReleaseRequest) error {
	p := req.DNSPublicationPrecondition
	if p == nil || !dnsOnlyPublication(a) || req.ProducerReconfiguration != nil || req.SoftOverride || req.ForcePublish || req.KernelBreakGlass != nil ||
		req.ReleaseChannel == "" || NormalizePlatformReleaseChannel(req.ReleaseChannel) != req.ReleaseChannel || len(p.Lanes) != 3 ||
		p.Lanes["shadow"] == nil || p.Lanes["full"] == nil || p.LKGID == "" || len(p.LKGID) > 256 ||
		!validDNSPublicationRef(p.LKGPublication) || !platformproducer.ValidDigest(p.VerificationEvidenceHash) {
		return ErrInvalidInput
	}
	for _, channel := range []string{"shadow", "gray", "full"} {
		ref, exists := p.Lanes[channel]
		if !exists || ref != nil && !validDNSPublicationRef(*ref) {
			return ErrInvalidInput
		}
	}
	key, err := dnsPublicationKey(a, req)
	if err != nil || req.IdempotencyKey != key {
		return ErrInvalidInput
	}
	return nil
}

// A failed successor cannot consume or rewrite the previous positive LKG.
// Observed lane fences are preconditions, never publication authority.
func validateDNSPublicationGuard(state *model.State, a model.PlatformArtifact, req model.PlatformArtifactReleaseRequest, keys bundleauth.Keyring, at time.Time) error {
	if req.DNSPublicationPrecondition == nil {
		return nil
	}
	if err := validateDNSPublicationGuardRequest(a, req); err != nil {
		return err
	}
	if a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
		return ErrConflict
	}
	p := req.DNSPublicationPrecondition
	key := platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, req.ReleaseChannel)
	if existing, found := platformReleaseByIdempotencyKey(state.PlatformArtifactReleases, key, req.IdempotencyKey); found {
		lane, exists := platformReleaseLaneByKey(state.PlatformReleaseLanes, key)
		if !exists || existing.ArtifactID != a.ID || existing.Status != model.PlatformArtifactReleaseStatusActive || lane.ActiveReleaseID != existing.ID {
			return ErrConflict
		}
	}
	for _, channel := range []string{"shadow", "gray", "full"} {
		ref := p.Lanes[channel]
		laneKey := platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, channel)
		lane, exists := platformReleaseLaneByKey(state.PlatformReleaseLanes, laneKey)
		if exists && lane.Frozen {
			return ErrConflict
		}
		var previousFence int64
		if ref != nil {
			old, _, err := exactPublication(state, *ref, a.ArtifactKind, a.ScopeKey, channel, keys, false)
			if err != nil || !dnsOnlyPublication(old) {
				return ErrConflict
			}
			previousFence = ref.FencingToken
		}
		// Permit only an exact active retry or the immediate post-write state.
		if channel == req.ReleaseChannel && exists && lane.ActiveReleaseID != "" {
			i := platformArtifactReleaseIndex(state.PlatformArtifactReleases, lane.ActiveReleaseID)
			if i >= 0 {
				r := state.PlatformArtifactReleases[i]
				if r.ArtifactID == a.ID && r.IdempotencyKey == req.IdempotencyKey && r.Status == model.PlatformArtifactReleaseStatusActive &&
					r.LaneKey == laneKey && r.FencingToken > previousFence && r.FencingToken-previousFence == 1 && lane.FencingToken == r.FencingToken {
					continue
				}
			}
		}
		if ref == nil {
			if exists && (lane.ActiveReleaseID != "" || lane.FencingToken != 0) {
				return ErrConflict
			}
		} else if _, _, err := exactPublication(state, *ref, a.ArtifactKind, a.ScopeKey, channel, keys, true); err != nil {
			return err
		}
	}
	lkg := verifiedPlatformLKGSnapshotFromState(state, a.ArtifactKind, a.ScopeKey, at, keys)
	if lkg == nil || lkg.ID != p.LKGID || lkg.ArtifactID != p.LKGPublication.ArtifactID || lkg.ContentHash != p.LKGPublication.ContentHash ||
		lkg.VerifiedByReleaseID != p.LKGPublication.ReleaseID || lkg.VerificationEvidenceHash != p.VerificationEvidenceHash {
		return ErrConflict
	}
	i := platformArtifactReleaseIndex(state.PlatformArtifactReleases, lkg.VerifiedByReleaseID)
	if i < 0 {
		return ErrConflict
	}
	r := state.PlatformArtifactReleases[i]
	if r.ReleaseChannel != "gray" && r.ReleaseChannel != "full" || r.VerificationState != model.PlatformArtifactVerificationStateVerified || r.VerifiedLKGGeneration != lkg.Generation {
		return ErrConflict
	}
	old, _, err := exactPublication(state, p.LKGPublication, a.ArtifactKind, a.ScopeKey, r.ReleaseChannel, keys, false)
	if err != nil || !dnsOnlyPublication(old) {
		return ErrConflict
	}
	return nil
}

// The caller holds the exclusive DNS authority scope lock before these reads.
// Row acquisition follows the same ordering on every invocation and is bounded
// by three lanes, their publications and one positive LKG.
func (s *Store) pgDNSPublicationGuard(ctx context.Context, tx *sql.Tx, a model.PlatformArtifact, req model.PlatformArtifactReleaseRequest) error {
	if req.DNSPublicationPrecondition == nil {
		return nil
	}
	if err := validateDNSPublicationGuardRequest(a, req); err != nil {
		return err
	}
	state := &model.State{PlatformArtifacts: []model.PlatformArtifact{a}}
	load := func(id string) error {
		if platformArtifactReleaseIndex(state.PlatformArtifactReleases, id) >= 0 {
			return nil
		}
		r, err := pgGetPlatformArtifactRelease(ctx, tx, id, false)
		if err != nil {
			return err
		}
		state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, r)
		if platformArtifactIndex(state.PlatformArtifacts, r.ArtifactID) < 0 {
			value, err := pgGetPlatformArtifactForUpdate(ctx, tx, r.ArtifactID, false)
			if err != nil {
				return err
			}
			state.PlatformArtifacts = append(state.PlatformArtifacts, value)
		}
		return nil
	}
	for _, channel := range []string{"shadow", "gray", "full"} {
		if ref := req.DNSPublicationPrecondition.Lanes[channel]; ref != nil {
			if err := load(ref.ReleaseID); err != nil {
				return err
			}
		}
		lane, err := pgGetPlatformReleaseLaneForUpdate(ctx, tx, platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, channel))
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return err
		}
		state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, lane)
		if lane.ActiveReleaseID != "" {
			if err := load(lane.ActiveReleaseID); err != nil {
				return err
			}
		}
	}
	if existing, found, err := pgGetPlatformArtifactReleaseByIdempotency(ctx, tx, platformsafety.ReleaseLaneKey(a.ArtifactKind, a.ScopeKey, req.ReleaseChannel), req.IdempotencyKey); err != nil {
		return err
	} else if found {
		if err := load(existing.ID); err != nil {
			return err
		}
	}
	lkg, err := pgGetPlatformLKGForUpdate(ctx, tx, a.ArtifactKind, a.ScopeKey)
	if err != nil {
		return err
	}
	if lkg != nil {
		state.PlatformLKGSnapshots = append(state.PlatformLKGSnapshots, *lkg)
		if err := load(lkg.VerifiedByReleaseID); err != nil {
			return err
		}
	}
	return validateDNSPublicationGuard(state, a, req, s.platformArtifactSigningKeyring(), time.Now().UTC())
}

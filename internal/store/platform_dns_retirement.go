package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func dnsRetirementMember(full model.PlatformArtifact) (string, error) {
	if _, err := platformconfig.ValidateReleaseComposition(full); err != nil {
		return "", ErrConflict
	}
	var set platformconfig.ReleaseSet
	raw, err := json.Marshal(full.Content)
	if err != nil || json.Unmarshal(raw, &set) != nil {
		return "", ErrConflict
	}
	for index, kind := range set.ArtifactKinds {
		if kind == model.PlatformArtifactKindDNSAnswerBundle {
			return set.ArtifactIDs[index], nil
		}
	}
	return "", ErrConflict
}

func validateProducerDNSRetirement(state *model.State, previous platformproducer.Policy, successor, full model.PlatformArtifact, keys bundleauth.Keyring) error {
	next, err := platformproducer.Decode(successor)
	if err != nil {
		return ErrConflict
	}
	dnsID, err := dnsRetirementMember(full)
	if err != nil {
		return err
	}
	inputs := make([]model.PlatformArtifact, 0, 4)
	for _, reference := range []struct{ id, digest, kind string }{
		{previous.StaticIntentArtifactID, previous.StaticIntentDigest, model.PlatformArtifactKindPlatformIntent},
		{previous.DNSPolicyArtifactID, previous.DNSPolicyDigest, model.PlatformArtifactKindPolicySnapshot},
		{next.DNSPolicyArtifactID, next.DNSPolicyDigest, model.PlatformArtifactKindPolicySnapshot},
		{dnsID, "", model.PlatformArtifactKindDNSAnswerBundle},
	} {
		index := platformArtifactIndex(state.PlatformArtifacts, reference.id)
		if index < 0 {
			return ErrConflict
		}
		artifact := state.PlatformArtifacts[index]
		if artifact.ArtifactKind != reference.kind || artifact.ScopeKey != "global" || reference.digest != "" && artifact.ContentHash != reference.digest || artifact.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(artifact, keys).Pass {
			return ErrConflict
		}
		inputs = append(inputs, artifact)
	}
	child := inputs[3]
	if child.Metadata["release_set_generation"] != full.Generation {
		return ErrConflict
	}
	for _, key := range []string{"intent_digest", "policy_digest", "compiler_version", "input_snapshot_digest", "intent_generation", "policy_generation"} {
		if full.Metadata[key] == "" || child.Metadata[key] != full.Metadata[key] {
			return ErrConflict
		}
	}
	if platformproducer.ValidateDNSSelectorRetirement(previous, next, inputs[0], inputs[1], inputs[2], child) != nil {
		return ErrConflict
	}
	return nil
}

func pgLoadDNSRetirementBaseline(ctx context.Context, tx *sql.Tx, state *model.State, parentID string) error {
	index := platformArtifactIndex(state.PlatformArtifacts, parentID)
	if index < 0 {
		return ErrConflict
	}
	id, err := dnsRetirementMember(state.PlatformArtifacts[index])
	if err != nil {
		return err
	}
	child, err := pgGetPlatformArtifactForUpdate(ctx, tx, id, true)
	if err != nil {
		return err
	}
	state.PlatformArtifacts = append(state.PlatformArtifacts, child)
	return nil
}

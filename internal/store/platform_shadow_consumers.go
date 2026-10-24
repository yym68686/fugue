package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

func validateShadowObservation(state *model.State, set model.PlatformExpectedConsumerSet, h platformcontrol.PlatformConsumerHeartbeatEnvelope, keys bundleauth.Keyring) error {
	if h.Component != model.PlatformConsumerComponentDNSServer || h.ArtifactKind != model.PlatformArtifactKindDNSAnswerBundle || !strings.HasPrefix(h.ScopeKey, "authority-cell:cell-") || h.ApplyStatus != "staged" || h.ProbeStatus != "shadow_validated" || h.LastError != "" || set.ArtifactReleaseID == "" {
		return ErrInvalidInput
	}
	var parent model.PlatformArtifact
	var release model.PlatformArtifactRelease
	for _, a := range state.PlatformArtifacts {
		if a.ID == set.ReleaseSetID {
			parent = a
			break
		}
	}
	for _, r := range state.PlatformArtifactReleases {
		if r.ID == set.ArtifactReleaseID {
			release = r
			break
		}
	}
	lane, found := platformReleaseLaneByKey(state.PlatformReleaseLanes, release.LaneKey)
	if !dnsOnlyPublication(parent) || parent.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(parent, keys).Pass || parent.ScopeKey != set.ScopeKey || platformcontrol.ValidateDeclaredTrafficConsumerSet(parent, set) != nil ||
		release.ArtifactID != parent.ID || release.ArtifactKind != parent.ArtifactKind || release.ScopeKey != parent.ScopeKey || release.ReleaseChannel != "shadow" || release.Status != model.PlatformArtifactReleaseStatusActive || release.LaneKey != platformsafety.ReleaseLaneKey(parent.ArtifactKind, parent.ScopeKey, "shadow") || !found || lane.Frozen || lane.ActiveReleaseID != release.ID || lane.FencingToken != release.FencingToken || h.FencingToken != release.FencingToken {
		return ErrConflict
	}
	ids, ok := parent.Content["artifact_ids"].([]any)
	if !ok || len(ids) != 1 {
		return ErrConflict
	}
	id, ok := ids[0].(string)
	if !ok {
		return ErrConflict
	}
	index := platformArtifactIndex(state.PlatformArtifacts, id)
	if index < 0 {
		return ErrConflict
	}
	child := state.PlatformArtifacts[index]
	if child.ArtifactKind != set.ArtifactKind || child.ScopeKey != parent.ScopeKey || child.Generation != set.ExpectedGeneration || child.GenerationSequence != h.GenerationSequence || child.Metadata["release_set_generation"] != parent.Generation || child.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(child, keys).Pass {
		return ErrConflict
	}
	for _, key := range []string{"intent_digest", "policy_digest", "compiler_version", "input_snapshot_digest", "intent_generation", "policy_generation"} {
		if parent.Metadata[key] == "" || parent.Metadata[key] != child.Metadata[key] {
			return ErrConflict
		}
	}
	for _, other := range state.ExpectedConsumerSets {
		if other.ReleaseSetID == set.ReleaseSetID && other.ArtifactKind == set.ArtifactKind && other.ScopeKey == set.ScopeKey && other.ArtifactReleaseID == set.ArtifactReleaseID && other.Revision > set.Revision {
			return ErrConflict
		}
	}
	return nil
}

func acceptShadowObservation(state *model.State, claims platformcontrol.PlatformComponentIdentityClaims, set model.PlatformExpectedConsumerSet, h platformcontrol.PlatformConsumerHeartbeatEnvelope, at time.Time, keys bundleauth.Keyring) (model.PlatformConsumerInstance, error) {
	if err := validateShadowObservation(state, set, h, keys); err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	bound, err := platformcontrol.BindPlatformConsumerHeartbeatToExpectedSet(claims, set, h)
	if err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	index := -1
	var cursor *platformcontrol.PlatformConsumerHeartbeatCursor
	for i, previous := range state.PlatformShadowConsumers {
		if previous.ConsumerID == bound.ConsumerID && previous.ArtifactKind == bound.ArtifactKind && previous.ScopeKey == bound.ScopeKey {
			index = i
			cursor, err = platformcontrol.PlatformConsumerHeartbeatCursorFromInstance(previous)
			if err != nil {
				return model.PlatformConsumerInstance{}, err
			}
			break
		}
	}
	consumer, _, err := platformcontrol.VerifyTrustedPlatformConsumerHeartbeat(claims, set, bound, cursor, at, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{})
	if err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	consumer.ID = "shadow_" + platformConsumerInstanceID(consumer.ConsumerID, consumer.ArtifactKind, consumer.ScopeKey)
	raw, err := json.Marshal(consumer)
	if err != nil || len(raw) > 32768 {
		return model.PlatformConsumerInstance{}, ErrInvalidInput
	}
	if index < 0 {
		state.PlatformShadowConsumers = append(state.PlatformShadowConsumers, consumer)
	} else {
		state.PlatformShadowConsumers[index] = consumer
	}
	return consumer, nil
}

func (s *Store) AcceptPlatformShadowConsumerHeartbeat(claims platformcontrol.PlatformComponentIdentityClaims, h platformcontrol.PlatformConsumerHeartbeatEnvelope, at time.Time, audit bundleauth.Keyring) (model.PlatformConsumerInstance, error) {
	if s.db != nil {
		return s.pgAcceptPlatformShadowConsumerHeartbeat(claims, h, at, audit)
	}
	var out model.PlatformConsumerInstance
	err := s.withLockedState(true, func(state *model.State) error {
		var set model.PlatformExpectedConsumerSet
		for _, v := range state.ExpectedConsumerSets {
			if v.ID == h.ExpectedConsumerSetID {
				set = v
				break
			}
		}
		if set.ID == "" {
			return ErrNotFound
		}
		var err error
		out, err = acceptShadowObservation(state, claims, set, h, at, s.platformArtifactSigningKeyring())
		if err != nil {
			return err
		}
		if err = appendPlatformConsumerHeartbeatAuditEventToState(state, out, audit); err != nil {
			return err
		}
		return validateShadowObservation(state, set, h, s.platformArtifactSigningKeyring())
	})
	return out, err
}

func (s *Store) ListPlatformShadowConsumers(kind, scope string) ([]model.PlatformConsumerInstance, error) {
	out := []model.PlatformConsumerInstance{}
	if s.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rows, err := s.db.QueryContext(ctx, `SELECT body FROM fugue_platform_shadow_consumers WHERE artifact_kind=$1 AND scope_key=$2 ORDER BY consumer_id`, kind, scope)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			var v model.PlatformConsumerInstance
			if err = rows.Scan(&raw); err != nil {
				return nil, err
			}
			if len(raw) > 65536 || json.Unmarshal(raw, &v) != nil {
				return nil, ErrConflict
			}
			out = append(out, v)
		}
		return out, rows.Err()
	}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, v := range state.PlatformShadowConsumers {
			if v.ArtifactKind == kind && v.ScopeKey == scope {
				out = append(out, v)
			}
		}
		return nil
	})
	return out, err
}

func (s *Store) pgAcceptPlatformShadowConsumerHeartbeat(claims platformcontrol.PlatformComponentIdentityClaims, h platformcontrol.PlatformConsumerHeartbeatEnvelope, at time.Time, audit bundleauth.Keyring) (model.PlatformConsumerInstance, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	defer tx.Rollback()
	if err = pgLockPromotionScope(ctx, tx, claims.ScopeKey, true); err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	// Serialize first inserts as well as updates without touching the serving row.
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "fugue-shadow/"+claims.ConsumerID()+"/"+claims.ScopeKey); err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	set, err := scanPlatformExpectedConsumerSet(tx.QueryRowContext(ctx, `SELECT id,release_set_id,artifact_release_id,artifact_kind,scope_key,scope_json,expected_generation,topology_revision,revision,requires_consumers,required_cardinality,optional_cardinality,heartbeat_deadline,convergence_deadline,consumers_json,created_at,updated_at FROM fugue_platform_expected_consumer_sets WHERE id=$1 FOR SHARE`, h.ExpectedConsumerSetID))
	if err != nil {
		return model.PlatformConsumerInstance{}, mapDBErr(err)
	}
	parent, err := pgGetPlatformArtifactForUpdate(ctx, tx, set.ReleaseSetID, true)
	if err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	release, err := pgGetPlatformArtifactRelease(ctx, tx, set.ArtifactReleaseID, false)
	if err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	lane, err := pgGetPlatformReleaseLaneForUpdate(ctx, tx, release.LaneKey)
	if err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	state := &model.State{PlatformArtifacts: []model.PlatformArtifact{parent}, PlatformArtifactReleases: []model.PlatformArtifactRelease{release}, PlatformReleaseLanes: []model.PlatformReleaseLane{lane}, ExpectedConsumerSets: []model.PlatformExpectedConsumerSet{set}}
	ids, ok := parent.Content["artifact_ids"].([]any)
	if !ok || len(ids) != 1 {
		return model.PlatformConsumerInstance{}, ErrConflict
	}
	childID, ok := ids[0].(string)
	if !ok {
		return model.PlatformConsumerInstance{}, ErrConflict
	}
	child, err := pgGetPlatformArtifactForUpdate(ctx, tx, childID, true)
	if err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	state.PlatformArtifacts = append(state.PlatformArtifacts, child)
	var newer bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_platform_expected_consumer_sets WHERE release_set_id=$1 AND artifact_release_id=$2 AND artifact_kind=$3 AND scope_key=$4 AND revision>$5)`, set.ReleaseSetID, set.ArtifactReleaseID, set.ArtifactKind, set.ScopeKey, set.Revision).Scan(&newer); err != nil {
		return model.PlatformConsumerInstance{}, err
	}
	if newer {
		return model.PlatformConsumerInstance{}, ErrConflict
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT body FROM fugue_platform_shadow_consumers WHERE consumer_id=$1 AND artifact_kind=$2 AND scope_key=$3 FOR UPDATE`, claims.ConsumerID(), h.ArtifactKind, claims.ScopeKey).Scan(&raw)
	if err == nil {
		var previous model.PlatformConsumerInstance
		if len(raw) > 65536 || json.Unmarshal(raw, &previous) != nil {
			return model.PlatformConsumerInstance{}, ErrConflict
		}
		state.PlatformShadowConsumers = append(state.PlatformShadowConsumers, previous)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return model.PlatformConsumerInstance{}, err
	}
	out, err := acceptShadowObservation(state, claims, set, h, at, s.platformArtifactSigningKeyring())
	if err != nil {
		return out, err
	}
	raw, err = json.Marshal(out)
	if err != nil {
		return out, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_shadow_consumers (consumer_id,artifact_kind,scope_key,body) VALUES ($1,$2,$3,$4) ON CONFLICT (consumer_id,artifact_kind,scope_key) DO UPDATE SET body=EXCLUDED.body`, out.ConsumerID, out.ArtifactKind, out.ScopeKey, raw)
	if err != nil {
		return out, err
	}
	if err = s.pgAppendPlatformConsumerHeartbeatAuditEventTx(ctx, tx, out, audit); err != nil {
		return out, err
	}
	if err = validateShadowObservation(state, set, h, s.platformArtifactSigningKeyring()); err != nil {
		return out, err
	}
	return out, tx.Commit()
}

// A shadow record can supply executable capability only for the exact selected
// candidate. It is never appended to serving convergence or LKG inputs.
func trafficCapabilityObservations(state *model.State, parent, child model.PlatformArtifact, set model.PlatformExpectedConsumerSet, now time.Time) []model.PlatformConsumerInstance {
	if !dnsOnlyPublication(parent) {
		return state.PlatformConsumerInstances
	}
	out := append([]model.PlatformConsumerInstance(nil), state.PlatformConsumerInstances...)
	lane, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, platformsafety.ReleaseLaneKey(parent.ArtifactKind, parent.ScopeKey, "shadow"))
	if !ok || lane.Frozen {
		return out
	}
	var release model.PlatformArtifactRelease
	for _, r := range state.PlatformArtifactReleases {
		if r.ID == lane.ActiveReleaseID {
			release = r
			break
		}
	}
	if release.ArtifactID != parent.ID || release.Status != model.PlatformArtifactReleaseStatusActive || release.FencingToken != lane.FencingToken {
		return out
	}
	for _, v := range state.PlatformShadowConsumers {
		if v.ArtifactKind != child.ArtifactKind || v.ScopeKey != parent.ScopeKey || v.ReleaseSetID != parent.ID || v.GenerationSequence != child.GenerationSequence || v.DesiredGeneration != child.Generation || v.FencingToken != release.FencingToken || v.ApplyStatus != "staged" || v.ProbeStatus != "shadow_validated" {
			continue
		}
		var expected model.PlatformExpectedConsumerSet
		for _, e := range state.ExpectedConsumerSets {
			if e.ID == v.ExpectedConsumerSetID {
				expected = e
				break
			}
		}
		if expected.ArtifactReleaseID != release.ID || expected.ReleaseSetID != parent.ID || expected.ArtifactKind != child.ArtifactKind || expected.ScopeKey != set.ScopeKey || platformcontrol.ValidateDeclaredTrafficConsumerSet(parent, expected) != nil {
			continue
		}
		stale := false
		for _, e := range state.ExpectedConsumerSets {
			if e.ArtifactReleaseID == expected.ArtifactReleaseID && e.ArtifactKind == expected.ArtifactKind && e.ScopeKey == expected.ScopeKey && e.Revision > expected.Revision {
				stale = true
			}
		}
		if stale {
			continue
		}
		fresh := false
		for _, member := range expected.Consumers {
			if member.ConsumerID == v.ConsumerID && trafficCapabilityFactFresh(member, v, now) {
				fresh = true
			}
		}
		if !fresh {
			continue
		}

		replaced := false
		for i, old := range out {
			if old.ConsumerID == v.ConsumerID && old.ArtifactKind == v.ArtifactKind && old.ScopeKey == v.ScopeKey {
				out[i] = v
				replaced = true
				break
			}
		}
		if !replaced {
			out = append(out, v)
		}
	}
	return out
}

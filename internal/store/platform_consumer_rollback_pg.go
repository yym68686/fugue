package store

import (
	"context"
	"database/sql"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

// The caller already owns the exclusive scope lock. Loading the same immutable
// releases, members and expectations as the file store keeps rollback proof
// independent of HTTP preflight and serializes it with configuration writers.
func (s *Store) pgConsumerTrafficRollbackCursor(ctx context.Context, tx *sql.Tx, previous model.PlatformConsumerInstance, cursor *platformcontrol.PlatformConsumerHeartbeatCursor, set model.PlatformExpectedConsumerSet, heartbeat platformcontrol.PlatformConsumerHeartbeatEnvelope) (*platformcontrol.PlatformConsumerHeartbeatCursor, error) {
	parent, err := pgGetPlatformArtifactForUpdate(ctx, tx, set.ReleaseSetID, true)
	if err != nil {
		return nil, mapDBErr(err)
	}
	state, err := s.pgFullReleaseSetSnapshot(ctx, tx, parent)
	if err != nil {
		return nil, err
	}
	oldSet, err := scanPlatformExpectedConsumerSet(tx.QueryRowContext(ctx, `SELECT id, release_set_id, artifact_release_id, artifact_kind, scope_key, scope_json, expected_generation, topology_revision, revision, requires_consumers, required_cardinality, optional_cardinality, heartbeat_deadline, convergence_deadline, consumers_json, created_at, updated_at FROM fugue_platform_expected_consumer_sets WHERE id=$1`, previous.ExpectedConsumerSetID))
	if err != nil {
		return nil, mapDBErr(err)
	}
	state.ExpectedConsumerSets = append(state.ExpectedConsumerSets, oldSet)
	old, err := pgGetPlatformArtifactRelease(ctx, tx, oldSet.ArtifactReleaseID, false)
	if err != nil {
		return nil, err
	}
	state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, old)
	message, err := scanPlatformReleaseMessage(tx.QueryRowContext(ctx, `SELECT id, release_id, artifact_id, artifact_kind, scope_key, scope_json, generation, release_channel, message_type, created_at, expires_at, ack_count FROM fugue_platform_release_messages WHERE release_id=$1 AND message_type=$2 ORDER BY created_at ASC LIMIT 1`, set.ArtifactReleaseID, model.PlatformReleaseMessageTypeRollback))
	if err != nil {
		return nil, platformcontrol.ErrPlatformConsumerHeartbeatGenerationBack
	}
	state.PlatformReleaseMessages = append(state.PlatformReleaseMessages, message)
	for _, r := range state.PlatformArtifactReleases {
		if r.Status != model.PlatformArtifactReleaseStatusActive || r.ReleaseChannel == model.PlatformArtifactReleaseChannelShadow || platformArtifactIndex(state.PlatformArtifacts, r.ArtifactID) >= 0 {
			continue
		}
		a, err := pgGetPlatformArtifactForUpdate(ctx, tx, r.ArtifactID, true)
		if err != nil {
			return nil, err
		}
		state.PlatformArtifacts = append(state.PlatformArtifacts, a)
	}
	return consumerCursorForTrafficRollback(state, previous, cursor, set, heartbeat, s.platformArtifactSigningKeyring())
}

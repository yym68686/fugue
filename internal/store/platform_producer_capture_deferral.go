package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// This optional schema is intentionally NOT in startup migrations. Installation
// and enrollment of every producer replica need their own bounded rollout and
// resource/availability approval. Merely publishing this code enables nothing.
const producerCaptureDeferralSchema = `
CREATE TABLE fugue_producer_capture_deferrals (
 operation_id text PRIMARY KEY, record_json jsonb NOT NULL,
 requested_by_type text NOT NULL, requested_by_id text NOT NULL,
 version bigint NOT NULL CHECK(version>0),
 CHECK(octet_length(record_json::text)<=65536)
);
CREATE TABLE fugue_producer_capture_deferral_scopes (
 scope_key text PRIMARY KEY,
 operation_id text NOT NULL REFERENCES fugue_producer_capture_deferrals(operation_id)
);
CREATE TABLE fugue_producer_capture_participants (
 participant_id text NOT NULL, scope_key text NOT NULL,
 protocol text NOT NULL, enrolled_at timestamptz NOT NULL, closed_at timestamptz,
 PRIMARY KEY(participant_id,scope_key)
);
CREATE TABLE fugue_producer_capture_work (
 ticket_id text PRIMARY KEY, scope_key text NOT NULL, policy_json jsonb NOT NULL,
 participant_id text NOT NULL, started_at timestamptz NOT NULL,
 FOREIGN KEY(participant_id,scope_key)
 REFERENCES fugue_producer_capture_participants(participant_id,scope_key)
);
CREATE INDEX fugue_producer_capture_work_scope ON fugue_producer_capture_work(scope_key);
CREATE INDEX fugue_producer_capture_work_participant ON fugue_producer_capture_work(participant_id,scope_key);
`

// ProducerCaptureWork is an admission ticket, never proof of a successful
// capture or commit. An abandoned ticket remains occupied after process/PG
// restart: it cannot be declared complete by a lease timeout. A draining control
// can expire without reaching active, with its original absolute deadline.
type ProducerCaptureWork struct {
	ID            string
	ParticipantID string
	Scope         string
	Policy        model.PlatformPublicationPrecondition
}

func captureDeferralAdmin(p model.Principal) bool {
	return p.ActorID != "" && p.HasScope("platform.admin")
}

func captureDeferralProducer(p model.Principal) bool {
	return captureDeferralAdmin(p) && p.ActorType == model.ActorTypeBootstrap && p.ActorID == platformproducer.Actor
}

// All record CAS, scope claims and work admission/completion use one short
// transaction barrier. No session lock, long transaction, lease expiry, network
// capture, artifact write or pending recovery is held behind a drain.
func (s *Store) captureDeferralTransaction(ctx context.Context, apply func(context.Context, *sql.Tx) error) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("capture deferral requires installed PostgreSQL storage: %w", ErrConflict)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := setCompilerInputAdmissionTimeouts(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `SELECT set_config('work_mem','1MB',true),set_config('temp_file_limit','0',true)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fugue.producer.capture-deferral/v1',0))`); err != nil {
		return err
	}
	if err := apply(ctx, tx); err != nil {
		return err
	}
	return tx.Commit() // ambiguous commit never grants capture admission
}

func captureDeferralNow(ctx context.Context, tx *sql.Tx) (time.Time, error) {
	var at time.Time
	err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&at)
	return at.UTC(), err
}

func loadCaptureDeferral(ctx context.Context, tx *sql.Tx, id string) (platformproducer.CaptureDeferralRecord, int64, error) {
	var raw []byte
	var version int64
	err := tx.QueryRowContext(ctx, `SELECT record_json,version FROM fugue_producer_capture_deferrals WHERE operation_id=$1`, id).Scan(&raw, &version)
	var record platformproducer.CaptureDeferralRecord
	if err != nil {
		return record, 0, err
	}
	if len(raw) > 65536 || version < 1 || json.Unmarshal(raw, &record) != nil || record.Request.OperationID != id || record.Validate() != nil {
		return record, 0, ErrConflict
	}
	return record, version, nil
}

func saveCaptureDeferral(ctx context.Context, tx *sql.Tx, record platformproducer.CaptureDeferralRecord, version int64) error {
	raw, err := json.Marshal(record)
	if err != nil || len(raw) > 65536 {
		return ErrInvalidInput
	}
	result, err := tx.ExecContext(ctx, `UPDATE fugue_producer_capture_deferrals SET record_json=$1,version=version+1 WHERE operation_id=$2 AND version=$3`, raw, record.Request.OperationID, version)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return ErrConflict
	}
	if record.EndedAt != nil {
		_, err = tx.ExecContext(ctx, `DELETE FROM fugue_producer_capture_deferral_scopes WHERE operation_id=$1`, record.Request.OperationID)
	}
	return err
}

// Shared policy locks have the same order/key as ordinary publication. Read the
// exact lane/release/hash/fence and verify the actual signed policy; an intent
// declaration or cached caller tuple is insufficient. No compiler input is read.
func (s *Store) captureDeferralObservation(ctx context.Context, tx *sql.Tx, request platformproducer.CaptureDeferralRequest) (platformproducer.CaptureDeferralObservation, error) {
	observation := platformproducer.CaptureDeferralObservation{Authorities: map[string]model.PlatformPublicationPrecondition{}, InFlight: map[string]uint64{}}
	authorities := slices.Clone(request.Authorities)
	slices.SortFunc(authorities, func(a, b platformproducer.CaptureDeferralAuthority) int { return strings.Compare(a.Scope, b.Scope) })
	for _, binding := range authorities {
		if err := pgLockPromotionScope(ctx, tx, binding.Scope, false); err != nil {
			return observation, err
		}
	}
	for _, binding := range authorities {
		var current model.PlatformPublicationPrecondition
		err := tx.QueryRowContext(ctx, `SELECT a.id,a.content_hash,r.id,r.fencing_token
 FROM fugue_platform_release_lanes l
 JOIN fugue_platform_artifact_releases r ON r.id=l.active_release_id
 JOIN fugue_platform_artifacts a ON a.id=r.artifact_id
 WHERE l.lane_key=$1 AND NOT l.frozen AND r.status='active'
 AND r.lane_key=l.lane_key AND r.fencing_token=l.fencing_token
 AND r.artifact_kind=l.artifact_kind AND r.scope_key=l.scope_key
 AND r.release_channel=l.release_channel AND r.generation=a.generation
 AND a.artifact_kind=l.artifact_kind AND a.scope_key=l.scope_key AND a.status='validated'
 AND octet_length(a.content_json::text)<=1048576
 FOR SHARE OF l,r,a`,
			platformsafety.ReleaseLaneKey(model.PlatformArtifactKindPolicySnapshot, binding.Scope, "shadow")).Scan(&current.ArtifactID, &current.ContentHash, &current.ReleaseID, &current.FencingToken)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return observation, err
		}
		if err == nil {
			a, err := pgGetPlatformArtifactForUpdate(ctx, tx, current.ArtifactID, false)
			if err != nil {
				return observation, err
			}
			if _, err := platformproducer.Decode(a); err == nil && a.ID == current.ArtifactID && a.ContentHash == current.ContentHash && a.ScopeKey == binding.Scope && s.VerifyPlatformArtifactIntegrity(a) == nil {
				observation.Authorities[binding.Scope] = current
			}
		}
		var count uint64
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT 1 FROM fugue_producer_capture_work WHERE scope_key=$1 LIMIT 4097) w`, binding.Scope).Scan(&count); err != nil {
			return observation, err
		}
		if count > 4096 {
			return observation, fmt.Errorf("capture work bound exceeded: %w", ErrConflict)
		}
		observation.InFlight[binding.Scope] = count
	}
	var err error
	observation.At, err = captureDeferralNow(ctx, tx)
	return observation, err
}

// PrepareProducerCaptureDeferral has no HTTP/config/startup caller. A trusted
// administrative principal alone does not approve an availability restriction;
// the external enable protocol must first establish approval, all-replica
// enrollment and complete resource/exit budgets. It is deliberately not supplied
// here, so local adapter tests cannot be mistaken for production enablement.
func (s *Store) PrepareProducerCaptureDeferral(ctx context.Context, request platformproducer.CaptureDeferralRequest, principal model.Principal) (platformproducer.CaptureDeferralRecord, error) {
	var record platformproducer.CaptureDeferralRecord
	if !captureDeferralAdmin(principal) {
		return record, ErrInvalidInput
	}
	// Validate all identities/bounds before queries or locks, not just on insert.
	validation := platformproducer.CaptureDeferralObservation{At: request.RequestedAt, Authorities: map[string]model.PlatformPublicationPrecondition{}, InFlight: map[string]uint64{}}
	for _, a := range request.Authorities {
		validation.Authorities[a.Scope], validation.InFlight[a.Scope] = a.Policy, 0
	}
	if _, err := platformproducer.PrepareCaptureDeferral(request, nil, validation); err != nil {
		return record, err
	}
	err := s.captureDeferralTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var retained int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT 1 FROM fugue_producer_capture_deferrals LIMIT 4097) d`).Scan(&retained); err != nil {
			return err
		}
		if retained >= 4096 {
			return fmt.Errorf("capture deferral tombstone budget exhausted: %w", ErrConflict)
		}
		if _, _, err := loadCaptureDeferral(ctx, tx, request.OperationID); !errors.Is(err, sql.ErrNoRows) {
			if err != nil {
				return err
			}
			return ErrConflict
		}
		observation, err := s.captureDeferralObservation(ctx, tx, request)
		if err != nil {
			return err
		}
		record, err = platformproducer.PrepareCaptureDeferral(request, nil, observation)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_producer_capture_deferrals(operation_id,record_json,version,requested_by_type,requested_by_id) VALUES($1,$2,1,$3,$4)`, request.OperationID, raw, principal.ActorType, principal.ActorID); err != nil {
			return err
		}
		for _, a := range request.Authorities {
			if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_producer_capture_deferral_scopes(scope_key,operation_id) VALUES($1,$2)`, a.Scope, request.OperationID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return platformproducer.CaptureDeferralRecord{}, err
	}
	return record, nil
}

func (s *Store) UpdateProducerCaptureDeferral(ctx context.Context, operationID string, release bool, principal model.Principal) (platformproducer.CaptureDeferralRecord, error) {
	var record platformproducer.CaptureDeferralRecord
	if !captureDeferralAdmin(principal) || operationID == "" || len(operationID) > 128 {
		return record, ErrInvalidInput
	}
	err := s.captureDeferralTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		previous, version, err := loadCaptureDeferral(ctx, tx, operationID)
		if err != nil {
			return err
		}
		if release {
			at, err := captureDeferralNow(ctx, tx)
			if err != nil {
				return err
			}
			record, err = platformproducer.ReleaseCaptureDeferral(previous, at)
			if err != nil {
				return err
			}
		} else {
			observation, err := s.captureDeferralObservation(ctx, tx, previous.Request)
			if err != nil {
				return err
			}
			record, err = platformproducer.AdvanceCaptureDeferral(previous, observation)
			if err != nil {
				return err
			}
		}
		if previous.Phase == record.Phase {
			// No WAL on unchanged polling; preserve prior observed time, and hence
			// the original monotonic floor, instead of renewing a live control.
			record = previous
			return nil
		}
		return saveCaptureDeferral(ctx, tx, record, version)
	})
	if err != nil {
		return platformproducer.CaptureDeferralRecord{}, err
	}
	return record, nil
}

func (s *Store) BeginProducerCapture(ctx context.Context, participantID, scope string, policy model.PlatformPublicationPrecondition, principal model.Principal) (ProducerCaptureWork, bool, error) {
	var work ProducerCaptureWork
	deferred := false
	if !captureDeferralProducer(principal) || !validCaptureParticipantID(participantID) || !platformproducer.IsPolicyScope(scope) {
		return work, false, ErrInvalidInput
	}
	err := s.captureDeferralTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var registered bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_producer_capture_participants WHERE participant_id=$1 AND scope_key=$2 AND protocol=$3 AND closed_at IS NULL)`, participantID, scope, producerCaptureParticipantProtocol).Scan(&registered); err != nil {
			return err
		}
		if !registered {
			return fmt.Errorf("capture participant is not enrolled: %w", ErrConflict)
		}
		var id string
		err := tx.QueryRowContext(ctx, `SELECT operation_id FROM fugue_producer_capture_deferral_scopes WHERE scope_key=$1`, scope).Scan(&id)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			previous, version, err := loadCaptureDeferral(ctx, tx, id)
			if err != nil {
				return err
			}
			observation, err := s.captureDeferralObservation(ctx, tx, previous.Request)
			if err != nil {
				return err
			}
			record, err := platformproducer.AdvanceCaptureDeferral(previous, observation)
			if err != nil {
				return err
			}
			if previous.Phase != record.Phase {
				if err := saveCaptureDeferral(ctx, tx, record, version); err != nil {
					return err
				}
			}
			deferred, err = record.DefersNewInput(scope, policy, observation.At)
			if err != nil || deferred {
				return err
			}
		}
		observation, err := s.captureDeferralObservation(ctx, tx, platformproducer.CaptureDeferralRequest{Authorities: []platformproducer.CaptureDeferralAuthority{{Scope: scope, Policy: policy}}})
		if err != nil {
			return err
		}
		if current, ok := observation.Authorities[scope]; !ok || current != policy || observation.InFlight[scope] >= 4096 {
			return ErrConflict
		}
		work = ProducerCaptureWork{ID: model.NewID("producer-capture"), ParticipantID: participantID, Scope: scope, Policy: policy}
		raw, err := json.Marshal(policy)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_producer_capture_work(ticket_id,scope_key,policy_json,participant_id,started_at) VALUES($1,$2,$3,$4,$5)`, work.ID, scope, raw, participantID, observation.At)
		return err
	})
	if err != nil {
		return ProducerCaptureWork{}, false, err
	}
	return work, deferred, nil
}

// Finish is called only after capture/compile/persistence/publication returns.
// Cancellation, an error or a failed/ambiguous Finish never fabricates zero
// in-flight. The adapter does not cancel or retry any already admitted work.
func (s *Store) FinishProducerCapture(ctx context.Context, work ProducerCaptureWork, principal model.Principal) error {
	if !captureDeferralProducer(principal) || work.ID == "" || len(work.ID) > 128 || !validCaptureParticipantID(work.ParticipantID) || !platformproducer.IsPolicyScope(work.Scope) {
		return ErrInvalidInput
	}
	return s.captureDeferralTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		raw, err := json.Marshal(work.Policy)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM fugue_producer_capture_work WHERE ticket_id=$1 AND scope_key=$2 AND policy_json=$3::jsonb AND participant_id=$4`, work.ID, work.Scope, raw, work.ParticipantID)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil || n != 1 {
			return ErrConflict
		}
		return nil
	})
}

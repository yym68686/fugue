package store

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

const producerCaptureParticipantProtocol = "fugue.producer.capture-participant/v1"

// ProducerCaptureParticipant names one process boot and its registered capture
// scopes. It is not a runtime identity attestation or proof that a rollout has
// enrolled every producer. There is no lease or automatic stale-member removal.
type ProducerCaptureParticipant struct {
	ID     string
	Scopes []string
}

func validCaptureParticipantID(id string) bool {
	return id != "" && len(id) <= 128 && strings.TrimSpace(id) == id && !strings.ContainsAny(id, "\x00\r\n\t ")
}

func captureParticipantScopes(p ProducerCaptureParticipant) ([]string, error) {
	if !validCaptureParticipantID(p.ID) || len(p.Scopes) == 0 || len(p.Scopes) > 32 {
		return nil, ErrInvalidInput
	}
	scopes := slices.Clone(p.Scopes)
	slices.Sort(scopes)
	for i, scope := range scopes {
		if !platformproducer.IsPolicyScope(scope) || i > 0 && scopes[i-1] == scope {
			return nil, ErrInvalidInput
		}
	}
	return scopes, nil
}

// RegisterProducerCaptureParticipant is a durable enrollment, not an enable
// operation. Callers must install the admission boundary before any capture can
// start, use a fresh process-boot ID, and retain the returned identity for every
// ticket and exit. This method has no production/startup caller. External proof
// of the complete replica set and older unenrolled work is still required.
func (s *Store) RegisterProducerCaptureParticipant(ctx context.Context, participant ProducerCaptureParticipant, principal model.Principal) error {
	scopes, err := captureParticipantScopes(participant)
	if err != nil || !captureDeferralProducer(principal) {
		return ErrInvalidInput
	}
	return s.captureDeferralTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var rows, sameBoot int
		if err := tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE participant_id=$1) FROM (SELECT participant_id FROM fugue_producer_capture_participants LIMIT 4097) p`, participant.ID).Scan(&rows, &sameBoot); err != nil {
			return err
		}
		if rows+len(scopes) > 4096 || sameBoot != 0 {
			return fmt.Errorf("capture participant retained identity or budget conflict: %w", ErrConflict)
		}
		at, err := captureDeferralNow(ctx, tx)
		if err != nil {
			return err
		}
		for _, scope := range scopes {
			if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_producer_capture_participants(participant_id,scope_key,protocol,enrolled_at) VALUES($1,$2,$3,$4)`, participant.ID, scope, producerCaptureParticipantProtocol, at); err != nil {
				return err
			}
		}
		return nil
	})
}

// Read the complete retained scope set under the shared admission barrier.
// A closed boot remains readable for idempotent exit, but is never reopened.
func loadCaptureParticipant(ctx context.Context, tx *sql.Tx, id string) (ProducerCaptureParticipant, bool, error) {
	p := ProducerCaptureParticipant{ID: id}
	rows, err := tx.QueryContext(ctx, `SELECT scope_key,protocol,closed_at IS NOT NULL FROM fugue_producer_capture_participants WHERE participant_id=$1 ORDER BY scope_key LIMIT 33`, id)
	if err != nil {
		return p, false, err
	}
	defer rows.Close()
	closed := false
	for rows.Next() {
		var scope, protocol string
		var rowClosed bool
		if err := rows.Scan(&scope, &protocol, &rowClosed); err != nil {
			return p, false, err
		}
		if protocol != producerCaptureParticipantProtocol || !platformproducer.IsPolicyScope(scope) || len(p.Scopes) == 32 {
			return p, false, ErrConflict
		}
		p.Scopes = append(p.Scopes, scope)
		closed = closed || rowClosed
	}
	if err := rows.Err(); err != nil {
		return p, false, err
	}
	if len(p.Scopes) == 0 {
		return p, false, ErrConflict
	}
	return p, closed, nil
}

// GetProducerCaptureParticipant is a bounded roster read, including closed
// tombstones. Exit uses this instead of a process cache that may have missed an
// enrollment commit. It does not clear tickets or certify runtime coverage.
func (s *Store) GetProducerCaptureParticipant(ctx context.Context, id string, principal model.Principal) (ProducerCaptureParticipant, error) {
	var p ProducerCaptureParticipant
	if !validCaptureParticipantID(id) || !captureDeferralProducer(principal) {
		return p, ErrInvalidInput
	}
	err := s.captureDeferralTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		p, _, err = loadCaptureParticipant(ctx, tx, id)
		return err
	})
	if err != nil {
		return ProducerCaptureParticipant{}, err
	}
	return p, nil
}

// AddProducerCaptureParticipantScopes extends an already registered open boot
// before it admits capture in newly discovered scopes. It is idempotent across
// an uncertain commit; it neither creates a new boot nor reopens a closed one.
// The original global row bound and per-boot scope bound still apply.
func (s *Store) AddProducerCaptureParticipantScopes(ctx context.Context, additions ProducerCaptureParticipant, principal model.Principal) (ProducerCaptureParticipant, error) {
	var current ProducerCaptureParticipant
	scopes, err := captureParticipantScopes(additions)
	if err != nil || !captureDeferralProducer(principal) {
		return current, ErrInvalidInput
	}
	err = s.captureDeferralTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var closed bool
		var err error
		current, closed, err = loadCaptureParticipant(ctx, tx, additions.ID)
		if err != nil {
			return err
		}
		if closed {
			return ErrConflict
		}
		var missing []string
		for _, scope := range scopes {
			if !slices.Contains(current.Scopes, scope) {
				missing = append(missing, scope)
			}
		}
		if len(current.Scopes)+len(missing) > 32 {
			return ErrConflict
		}
		if len(missing) == 0 {
			return nil // no WAL and no renewal of the original enrollment
		}
		var retained int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT 1 FROM fugue_producer_capture_participants LIMIT 4097) p`).Scan(&retained); err != nil {
			return err
		}
		if retained+len(missing) > 4096 {
			return ErrConflict
		}
		at, err := captureDeferralNow(ctx, tx)
		if err != nil {
			return err
		}
		for _, scope := range missing {
			if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_producer_capture_participants(participant_id,scope_key,protocol,enrolled_at) VALUES($1,$2,$3,$4)`, current.ID, scope, producerCaptureParticipantProtocol, at); err != nil {
				return err
			}
		}
		current.Scopes = append(current.Scopes, missing...)
		slices.Sort(current.Scopes)
		return nil
	})
	if err != nil {
		return ProducerCaptureParticipant{}, err
	}
	return current, nil
}

// CloseProducerCaptureParticipant is called after the process has stopped new
// admission and joined all its capture attempts. The same transaction barrier
// also excludes late Begin calls. Any unfinished ticket prevents closure, even
// after connection/PG/process loss. A missing heartbeat is never completion.
// Closed rows remain tombstones and cannot be reopened with the same boot ID.
func (s *Store) CloseProducerCaptureParticipant(ctx context.Context, participant ProducerCaptureParticipant, principal model.Principal) error {
	scopes, err := captureParticipantScopes(participant)
	if err != nil || !captureDeferralProducer(principal) {
		return ErrInvalidInput
	}
	return s.captureDeferralTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var retained int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT 1 FROM fugue_producer_capture_participants WHERE participant_id=$1 LIMIT 33) p`, participant.ID).Scan(&retained); err != nil {
			return err
		}
		if retained != len(scopes) {
			return ErrConflict
		}
		at, err := captureDeferralNow(ctx, tx)
		if err != nil {
			return err
		}
		for _, scope := range scopes {
			var occupied bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_producer_capture_work WHERE participant_id=$1 AND scope_key=$2 LIMIT 1)`, participant.ID, scope).Scan(&occupied); err != nil {
				return err
			}
			if occupied {
				return fmt.Errorf("capture participant still has admitted work: %w", ErrConflict)
			}
			result, err := tx.ExecContext(ctx, `UPDATE fugue_producer_capture_participants SET closed_at=COALESCE(closed_at,$1) WHERE participant_id=$2 AND scope_key=$3 AND protocol=$4`, at, participant.ID, scope, producerCaptureParticipantProtocol)
			if err != nil {
				return err
			}
			if n, err := result.RowsAffected(); err != nil || n != 1 {
				return ErrConflict
			}
		}
		return nil
	})
}

package productioncontrol

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

//go:embed journal.sql
var schema string

var ErrRequestConflict = errors.New("production request ID was previously used with different content")

// ApplySchema is for a schema migrator, not for the runtime writer. A runtime
// process connects with a separate principal that has no DDL or DELETE access.
func ApplySchema(ctx context.Context, owner *sql.DB) error {
	tx, err := owner.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// The schema owner can run from multiple replicas; serialize version and
	// backfill checks before any DDL changes or data reads.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fugue.production.schema',0))`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("apply production control schema: %w", err)
	}
	return tx.Commit()
}

type Journal struct {
	db      *sql.DB
	trusted Approvers
	now     func() time.Time
}

func NewJournal(db *sql.DB, trusted Approvers) (*Journal, error) {
	if db == nil {
		return nil, errors.New("production control database is required")
	}
	keys := make(Approvers, len(trusted))
	for id, owner := range trusted {
		if !tokenPattern.MatchString(id) || !tokenPattern.MatchString(owner.ClusterID) || len(owner.PublicKey) != ed25519.PublicKeySize {
			return nil, errors.New("invalid production approval trust configuration")
		}
		owner.PublicKey = bytes.Clone(owner.PublicKey)
		keys[id] = owner
	}
	return &Journal{db: db, trusted: keys, now: time.Now}, nil
}

// VerifyIndependentObserverKey prevents an approval authority from being
// configured as the technical witness for the same execution journal.
func (j *Journal) VerifyIndependentObserverKey(key ed25519.PublicKey) error {
	if len(key) != ed25519.PublicKeySize {
		return errors.New("invalid production observer key")
	}
	for _, owner := range j.trusted {
		if bytes.Equal(key, owner.PublicKey) {
			return errors.New("provider observer key must not also authorize production intent")
		}
	}
	return nil
}

type Recorded struct {
	Digest  string
	Created bool
}

// RecordIntent uses the cluster, resource and request ID as the replay key.
// It writes only Fugue's journal and does not queue provider actions.
func (j *Journal) RecordIntent(ctx context.Context, actor string, intent Intent, payload []byte) (Recorded, error) {
	if !tokenPattern.MatchString(actor) {
		return Recorded{}, errors.New("invalid authenticated production intent actor")
	}
	if err := intent.VerifyPayload(payload); err != nil {
		return Recorded{}, err
	}
	intentDigest, err := intent.Digest()
	if err != nil {
		return Recorded{}, err
	}
	envelope, err := json.Marshal(intent)
	if err != nil {
		return Recorded{}, err
	}
	return j.record(ctx, intentDigest, func(tx *sql.Tx) (bool, error) {
		result, err := tx.ExecContext(ctx, `INSERT INTO fugue_control.intents
			(digest, cluster_id, resource_id, request_id, envelope, payload)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT DO NOTHING`,
			intentDigest, intent.ClusterID, intent.ResourceID, intent.RequestID, envelope, payload)
		if err != nil {
			return false, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return false, err
		}
		if n == 0 {
			var existing string
			var existingEnvelope, existingPayload []byte
			if err := tx.QueryRowContext(ctx, `SELECT digest, envelope, payload FROM fugue_control.intents
				WHERE cluster_id=$1 AND resource_id=$2 AND request_id=$3`,
				intent.ClusterID, intent.ResourceID, intent.RequestID).Scan(&existing, &existingEnvelope, &existingPayload); err != nil {
				return false, err
			}
			if existing != intentDigest || !bytes.Equal(existingEnvelope, envelope) || !bytes.Equal(existingPayload, payload) {
				return false, ErrRequestConflict
			}
			return false, nil
		}
		return true, appendAudit(ctx, tx, "intent.recorded", intentDigest, intentDigest, actor)
	})
}

// RecordApproval verifies the out-of-band key and expiry before storing the
// approval. The future executor must verify them again before dispatch;
// existence in the journal is not live authorization.
func (j *Journal) RecordApproval(ctx context.Context, approval Approval) (Recorded, error) {
	if !digestPattern.MatchString(approval.IntentDigest) || len(approval.Signature) != ed25519.SignatureSize {
		return Recorded{}, errors.New("invalid production approval envelope")
	}
	envelope, err := json.Marshal(approval)
	if err != nil {
		return Recorded{}, err
	}
	approvalDigest := recordedApprovalDigest(envelope)
	return j.record(ctx, approvalDigest, func(tx *sql.Tx) (bool, error) {
		intent, _, err := readIntent(ctx, tx, approval.IntentDigest)
		if err != nil {
			return false, err
		}
		if err := approval.Verify(intent, j.trusted, j.now()); err != nil {
			return false, err
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO fugue_control.approvals
			(digest, intent_digest, envelope) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
			approvalDigest, approval.IntentDigest, envelope)
		if err != nil {
			return false, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return false, err
		}
		if n == 0 {
			var existing []byte
			if err := tx.QueryRowContext(ctx, `SELECT envelope FROM fugue_control.approvals WHERE digest=$1 AND intent_digest=$2`,
				approvalDigest, approval.IntentDigest).Scan(&existing); err != nil {
				return false, err
			}
			if !bytes.Equal(existing, envelope) {
				return false, errors.New("persisted production approval differs from the submitted approval")
			}
			return false, nil
		}
		return true, appendAudit(ctx, tx, "approval.recorded", approvalDigest, approval.IntentDigest, approval.KeyID)
	})
}

func recordedApprovalDigest(envelope []byte) string {
	return digest(append([]byte(approvalDomain+".record\x00"), envelope...))
}

func (j *Journal) ReadIntent(ctx context.Context, intentDigest string) (Intent, []byte, error) {
	if !digestPattern.MatchString(intentDigest) {
		return Intent{}, nil, errors.New("invalid production intent digest")
	}
	return readIntent(ctx, j.db, intentDigest)
}

type queryRow interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readIntent(ctx context.Context, db queryRow, intentDigest string) (Intent, []byte, error) {
	var envelope, payload []byte
	var cluster, resource, request string
	var intent Intent
	if err := db.QueryRowContext(ctx, `SELECT envelope, payload, cluster_id, resource_id, request_id
		FROM fugue_control.intents WHERE digest=$1`, intentDigest).
		Scan(&envelope, &payload, &cluster, &resource, &request); err != nil {
		return Intent{}, nil, err
	}
	if err := json.Unmarshal(envelope, &intent); err != nil {
		return Intent{}, nil, fmt.Errorf("invalid persisted production intent: %w", err)
	}
	actual, err := intent.Digest()
	if err != nil || actual != intentDigest || intent.ClusterID != cluster || intent.ResourceID != resource ||
		intent.RequestID != request || intent.VerifyPayload(payload) != nil {
		return Intent{}, nil, errors.New("persisted production intent does not match its binding")
	}
	return intent, payload, nil
}

func (j *Journal) record(ctx context.Context, digest string, insert func(*sql.Tx) (bool, error)) (Recorded, error) {
	// Read Committed gives a separate statement snapshot when reading the
	// winner after a concurrent insert against a uniqueness constraint.
	tx, err := j.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Recorded{}, err
	}
	defer tx.Rollback()
	created, err := insert(tx)
	if err != nil {
		return Recorded{}, err
	}
	if err := tx.Commit(); err != nil {
		return Recorded{}, fmt.Errorf("commit production control journal: %w", err)
	}
	return Recorded{Digest: digest, Created: created}, nil
}

func appendAudit(ctx context.Context, tx *sql.Tx, kind, subject, intent, actor string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO fugue_control.audit
		(kind, subject_digest, intent_digest, actor) VALUES ($1, $2, $3, $4)`, kind, subject, intent, actor)
	return err
}

package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
)

// PlatformInputReadFailure describes the original rejected read, not a replay.
// Labels come only from fixed call sites; physical identities are hashed. It
// contains no input bodies, SQL, raw errors, credentials or authority.
type PlatformInputReadFailure struct {
	Phase         string   `json:"phase"`
	Check         string   `json:"check,omitempty"`
	Relation      string   `json:"relation,omitempty"`
	KeyHash       string   `json:"key_hash,omitempty"`
	ChangedFields []string `json:"changed_fields,omitempty"`
	ErrorClass    string   `json:"error_class"`
	SQLState      string   `json:"sql_state,omitempty"`
}

type platformInputReadObservationKey struct{}

// CheckPlatformInputReadObserved preserves the error and all admission checks.
// Its request-local observation is populated synchronously at the failing call.
func (s *Store) CheckPlatformInputReadObserved(ctx context.Context, selections []PlatformInputReadSelection) (PlatformInputReadFailure, error) {
	failure := PlatformInputReadFailure{}
	err := s.CheckPlatformInputRead(context.WithValue(ctx, platformInputReadObservationKey{}, &failure), selections)
	return failure, err
}

func observePlatformInputReadFailure(ctx context.Context, phase, check string, tuple *compilerInputActivationTuple, changed []string, err error) {
	if err == nil {
		return
	}
	failure, _ := ctx.Value(platformInputReadObservationKey{}).(*PlatformInputReadFailure)
	if failure == nil || failure.Phase != "" {
		return
	}
	failure.Phase, failure.Check = phase, check
	failure.ErrorClass = "unclassified"
	var databaseError interface{ SQLState() string }
	switch {
	case errors.As(err, &databaseError):
		failure.ErrorClass = "database"
		switch state := databaseError.SQLState(); state {
		case "55P03", "57014", "40001", "40P01", "08000", "08003", "08006", "57P01", "53300", "53100", "53200":
			failure.SQLState = state
		default:
			failure.SQLState = "other"
		}
	case errors.Is(err, context.DeadlineExceeded):
		failure.ErrorClass = "deadline"
	case errors.Is(err, context.Canceled):
		failure.ErrorClass = "canceled"
	case errors.Is(err, sql.ErrNoRows), errors.Is(err, ErrNotFound):
		failure.ErrorClass = "not_found"
	case errors.Is(err, ErrConflict):
		failure.ErrorClass = "conflict"
	case errors.Is(err, ErrInvalidInput):
		failure.ErrorClass = "invalid_input"
	}
	if tuple != nil {
		failure.Relation = tuple.table
		digest := sha256.Sum256([]byte(tuple.key))
		failure.KeyHash = "sha256:" + hex.EncodeToString(digest[:])
	}
	failure.ChangedFields = append([]string(nil), changed...)
}

func compilerInputTupleChangedFields(before, after compilerInputActivationTuple) []string {
	fields := []string{}
	if before.xmin != after.xmin {
		fields = append(fields, "xmin")
	}
	if before.ctid != after.ctid {
		fields = append(fields, "ctid")
	}
	if before.relation != after.relation {
		fields = append(fields, "relation")
	}
	if before.filenode != after.filenode {
		fields = append(fields, "filenode")
	}
	return fields
}

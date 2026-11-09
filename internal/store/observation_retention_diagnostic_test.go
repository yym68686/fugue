package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestObservationRetentionDiagnosticsKeepJoinedFailuresAndNoPrivateMessages(t *testing.T) {
	private := "private-chain-body-and-SQL"
	err := &retentionFailure{stage: "heartbeat_history", cause: errors.Join(
		fmt.Errorf("%s: %w", private, context.DeadlineExceeded),
		&pgconn.PgError{Code: "55P03", Message: private, Detail: private},
		&observationRetentionIntegrityError{category: "anchor_integrity"},
		&observationRetentionIntegrityError{category: "prefix_integrity"},
		&pgconn.PgError{Code: "55P03", Message: private},
		errors.New(private),
	)}
	want := []string{"anchor_integrity", "deadline", "lock_unavailable", "prefix_integrity", "unclassified"}
	got := observationRetentionFailureCategories(err)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("joined failures lost: %v", got)
	}
	s := &Store{}
	s.retention.lastError, s.retention.lastFailureStage, s.retention.lastFailureCategories = true, "heartbeat_history", got
	raw, e := json.Marshal(s.retentionObservation())
	if e != nil || strings.Contains(string(raw), private) || !strings.Contains(string(raw), `"last_failure_categories"`) {
		t.Fatal("private error escaped fixed observation")
	}
}

func TestObservationRetentionDiagnosticsDoNotInventUnknownDatabaseCauses(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want []string
	}{
		{nil, nil},
		{context.Canceled, []string{"canceled"}},
		{&pgconn.PgError{Code: "57014"}, []string{"statement_canceled"}},
		{&pgconn.PgError{Code: "53100"}, []string{"disk_full"}},
		{&pgconn.PgError{Code: "42501"}, []string{"permission_denied"}},
		{&pgconn.PgError{Code: "40001"}, []string{"transaction_conflict"}},
		{&pgconn.PgError{Code: "08006"}, []string{"connection_failure"}},
		{&pgconn.PgError{Code: "XX000", Message: "timeout"}, []string{"unclassified"}},
		{errors.New("permission denied: could be arbitrary text"), []string{"unclassified"}},
	} {
		if got := observationRetentionFailureCategories(tc.err); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("classification %T: %v != %v", tc.err, got, tc.want)
		}
	}
}

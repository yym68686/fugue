// Package storesource is the single legacy data-owner bridge for backup
// materialization. It narrows the monolithic store to two read methods and
// returns only a run plus the already redacted backend generation.
package storesource

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"

	"fugue/internal/backupmaterializer/legacysource"
	"fugue/internal/model"
	"fugue/internal/store"
)

var (
	ErrConfig = errors.New("backup materializer store source configuration invalid")

	canonicalRunID = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
)

// ReadStore is deliberately limited to the two existing read-only operations
// needed for a materializer snapshot. Mutation methods on *store.Store are not
// reachable through Reader.
type ReadStore interface {
	GetBackupRun(string, string, bool) (model.BackupRun, error)
	GetBackupBackendObservation(string, string, bool) (store.BackupBackendObservation, error)
}

type Reader struct {
	store ReadStore
}

func New(readStore ReadStore) (*Reader, error) {
	if nilInterface(readStore) {
		return nil, ErrConfig
	}
	return &Reader{store: readStore}, nil
}

func (reader *Reader) ReadSnapshot(ctx context.Context, runID string) (legacysource.Snapshot, error) {
	if ctx == nil || reader == nil || nilInterface(reader.store) || ctx.Err() != nil {
		return legacysource.Snapshot{}, legacysource.ErrSnapshotUnavailable
	}
	if !canonicalRunID.MatchString(runID) {
		return legacysource.Snapshot{}, legacysource.ErrSnapshotConflict
	}
	run, err := reader.store.GetBackupRun(runID, "", true)
	if err != nil {
		return legacysource.Snapshot{}, mapStoreError(err)
	}
	if ctx.Err() != nil {
		return legacysource.Snapshot{}, legacysource.ErrSnapshotUnavailable
	}
	if run.ID != runID || strings.TrimSpace(run.BackendID) != run.BackendID || run.BackendID == "" ||
		strings.TrimSpace(run.TenantID) != run.TenantID {
		return legacysource.Snapshot{}, legacysource.ErrSnapshotConflict
	}
	backend, err := reader.store.GetBackupBackendObservation(run.BackendID, "", true)
	if err != nil {
		return legacysource.Snapshot{}, mapStoreError(err)
	}
	if ctx.Err() != nil {
		return legacysource.Snapshot{}, legacysource.ErrSnapshotUnavailable
	}
	if backend.BackendID != run.BackendID || strings.TrimSpace(backend.BackendID) != backend.BackendID ||
		strings.TrimSpace(backend.TenantID) != backend.TenantID ||
		(backend.TenantID != "" && backend.TenantID != run.TenantID) ||
		strings.TrimSpace(backend.Generation) != backend.Generation || backend.Generation == "" {
		return legacysource.Snapshot{}, legacysource.ErrSnapshotConflict
	}
	return legacysource.Snapshot{Run: run, BackendGeneration: backend.Generation}, nil
}

func (reader *Reader) String() string {
	return "backup materializer read-only store source [REDACTED]"
}

func (reader *Reader) GoString() string {
	return reader.String()
}

func mapStoreError(err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return legacysource.ErrSnapshotNotFound
	case errors.Is(err, store.ErrInvalidInput), errors.Is(err, store.ErrConflict):
		return legacysource.ErrSnapshotConflict
	default:
		return legacysource.ErrSnapshotUnavailable
	}
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

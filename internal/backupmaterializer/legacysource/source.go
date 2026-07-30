// Package legacysource translates one bounded legacy backup snapshot into the
// redacted desired input consumed by the materializer HTTP boundary. It holds
// only a snapshot callback, not a datastore, signer, network client, or
// mutation capability.
package legacysource

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"fugue/internal/backupadapter"
	"fugue/internal/backupmaterializer/httpapi"
	"fugue/internal/backupmaterializeridentity"
	"fugue/internal/model"
)

var (
	ErrConfig              = errors.New("backup materializer legacy source configuration invalid")
	ErrSnapshotNotFound    = errors.New("backup materializer legacy snapshot not found")
	ErrSnapshotConflict    = errors.New("backup materializer legacy snapshot inconsistent")
	ErrSnapshotUnavailable = errors.New("backup materializer legacy snapshot unavailable")

	canonicalRunID = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
)

// Snapshot contains only the legacy run and the data owner's irreversible
// backend generation. Physical backend configuration and credentials must not
// cross this callback boundary.
type Snapshot struct {
	Run               model.BackupRun
	BackendGeneration string
}

// SnapshotReader performs the bounded data-owner read for one exact run.
// Implementations classify failures with the three ErrSnapshot sentinels;
// Source deliberately discards any attached error detail.
type SnapshotReader func(context.Context, string) (Snapshot, error)

type Source struct {
	read SnapshotReader
}

func New(read SnapshotReader) (*Source, error) {
	if read == nil {
		return nil, ErrConfig
	}
	return &Source{read: read}, nil
}

func (source *Source) ReadDesiredInput(
	ctx context.Context,
	request httpapi.ReadRequest,
) (httpapi.DesiredInput, error) {
	if ctx == nil || source == nil || source.read == nil || ctx.Err() != nil {
		return httpapi.DesiredInput{}, httpapi.ErrInputUnavailable
	}
	if !canonicalRunID.MatchString(request.RunID) ||
		backupmaterializeridentity.ServiceAccountNameForCell(request.CellKey) == "" {
		return httpapi.DesiredInput{}, httpapi.ErrInputConflict
	}
	snapshot, err := source.read(ctx, request.RunID)
	if err != nil {
		return httpapi.DesiredInput{}, mapSnapshotError(err)
	}
	if ctx.Err() != nil {
		return httpapi.DesiredInput{}, httpapi.ErrInputUnavailable
	}
	if snapshot.Run.ID != request.RunID || strings.TrimSpace(snapshot.Run.TenantID) != snapshot.Run.TenantID {
		return httpapi.DesiredInput{}, httpapi.ErrInputConflict
	}
	run := model.NormalizeBackupRun(snapshot.Run)
	spec, err := backupadapter.BuildShadowSpec(run, snapshot.BackendGeneration)
	if err != nil || spec.RunID != request.RunID {
		return httpapi.DesiredInput{}, httpapi.ErrInputConflict
	}
	if spec.CellKey != request.CellKey {
		return httpapi.DesiredInput{}, httpapi.ErrInputNotFound
	}
	return httpapi.DesiredInput{Spec: spec, TenantID: run.TenantID}, nil
}

func mapSnapshotError(err error) error {
	switch {
	case errors.Is(err, ErrSnapshotNotFound):
		return httpapi.ErrInputNotFound
	case errors.Is(err, ErrSnapshotConflict):
		return httpapi.ErrInputConflict
	default:
		return httpapi.ErrInputUnavailable
	}
}

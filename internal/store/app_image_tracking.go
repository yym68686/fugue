package store

import (
	"context"
	"database/sql"
	"time"

	"fugue/internal/model"
	"fugue/internal/store/internal/imagetracking"
)

// ImageTrackingReader is the evidence-only view for other domains.
type ImageTrackingReader interface {
	GetAppImageTracking(tenantID string, admin bool, appID string) (model.AppImageTracking, error)
	ListAppImageTrackingChecks(model.AppImageTrackingCheckFilter) ([]model.AppImageTrackingCheck, error)
}

// ImageTrackingRepository is wired into the image tracking command owners.
// It exposes neither the SQL connection nor any JSON or deployment transaction.
type ImageTrackingRepository interface {
	ImageTrackingReader
	UpsertAppImageTracking(model.AppImageTracking) (model.AppImageTracking, error)
	ListAppImageTrackings(model.AppImageTrackingFilter) ([]model.AppImageTracking, error)
	RecordAppImageTrackingCheck(id, digest, deliveryID, event, lastError string) (model.AppImageTracking, error)
	RecordAppImageTrackingQueued(id, digest, operationID, deliveryID, event string) (model.AppImageTracking, error)
	CreateAppImageTrackingCheck(model.AppImageTrackingCheck) (model.AppImageTrackingCheck, error)
	QueueAppImageTrackingImport(app model.App, tracking model.AppImageTracking, actorType, actorID string) (model.Operation, error)
}

type imageTrackingReadView struct{ ImageTrackingReader }

func (s *Store) ReadImageTracking() ImageTrackingReader {
	return imageTrackingReadView{s.ImageTracking()}
}

func (s *Store) ImageTracking() ImageTrackingRepository {
	repo := s.imageTracking()
	return &repo
}

func (s *Store) imageTracking() imagetracking.Repository {
	errs := imagetracking.Errors{InvalidInput: ErrInvalidInput, NotFound: ErrNotFound, MapDatabase: mapDBErr}
	operations := imagetracking.OperationWriter{CloneSpec: cloneAppSpec, Create: s.CreateOperation}
	if s.usingDatabase() {
		return imagetracking.NewPostgres(s.db, func(ctx context.Context, tx *sql.Tx, id string) (model.App, error) {
			return s.pgGetAppTx(ctx, tx, id, false)
		}, operations, errs)
	}
	return imagetracking.NewFile(func(write bool, apply func(*imagetracking.Rows) error) error {
		return s.withLockedState(write, func(state *model.State) error {
			rows := imageTrackingRows(state)
			rows.LookupApp = func(id string) (model.App, bool) {
				i := findApp(state, id)
				if i < 0 {
					return model.App{}, false
				}
				return state.Apps[i], true
			}
			if err := apply(&rows); err != nil {
				return err
			}
			if write {
				applyImageTrackingRows(state, rows)
			}
			return nil
		})
	}, operations, errs)
}

func imageTrackingRows(state *model.State) imagetracking.Rows {
	return imagetracking.Rows{Trackings: state.AppImageTrackings, Checks: state.AppImageTrackingChecks}
}

func applyImageTrackingRows(state *model.State, rows imagetracking.Rows) {
	state.AppImageTrackings, state.AppImageTrackingChecks = rows.Trackings, rows.Checks
}

func initializeImageTracking(state *model.State) {
	rows := imageTrackingRows(state)
	rows.Initialize()
	applyImageTrackingRows(state, rows)
}

func deleteTenantImageTracking(state *model.State, id string) {
	rows := imageTrackingRows(state)
	rows.DeleteTenant(id)
	applyImageTrackingRows(state, rows)
}

func deleteAppImageTracking(state *model.State, id string) {
	rows := imageTrackingRows(state)
	rows.DeleteApp(id)
	applyImageTrackingRows(state, rows)
}

func updateAppImageTrackingDeployedInState(state *model.State, op model.Operation, now time.Time) {
	rows := imageTrackingRows(state)
	imagetracking.RecordDeployment(&rows, op, now)
	applyImageTrackingRows(state, rows)
}

func (s *Store) pgUpdateAppImageTrackingDeployedTx(ctx context.Context, tx *sql.Tx, op model.Operation, now time.Time) error {
	repo := s.imageTracking()
	return repo.RecordDeploymentTx(ctx, tx, op, now)
}

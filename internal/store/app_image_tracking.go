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
// It exposes neither the SQL connection nor any deployment transaction.
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
	return imagetracking.NewPostgres(s.db, func(ctx context.Context, tx *sql.Tx, id string) (model.App, error) {
		return s.pgGetAppTx(ctx, tx, id, false)
	}, operations, errs)
}

func (s *Store) pgUpdateAppImageTrackingDeployedTx(ctx context.Context, tx *sql.Tx, op model.Operation, now time.Time) error {
	repo := s.imageTracking()
	return repo.RecordDeploymentTx(ctx, tx, op, now)
}

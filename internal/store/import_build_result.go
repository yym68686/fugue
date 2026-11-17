package store

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/model"
)

// RecordImportBuildResult saves a verified build independently of deployment.
// It changes only the current execution's operation, never the serving app.
// A failed deploy admission must not discard the only durable build result.
func (s *Store) RecordImportBuildResult(expected model.Operation, spec model.AppSpec, source model.AppSource, origin *model.AppSource) (model.Operation, error) {
	model.NormalizeOperationSourceState(&expected)
	if expected.Type != model.OperationTypeImport || spec.Image == "" || source.ResolvedImageRef == "" {
		return model.Operation{}, ErrInvalidInput
	}
	apply := func(op model.Operation) (model.Operation, error) {
		if op.ID != expected.ID || op.AppID != expected.AppID || op.TenantID != expected.TenantID || op.Type != model.OperationTypeImport ||
			(op.Status != model.OperationStatusPending && op.Status != model.OperationStatusRunning) ||
			!sameOperationStartedAt(op.StartedAt, expected.StartedAt) {
			return model.Operation{}, ErrConflict
		}
		for _, field := range []struct {
			name              string
			current, expected any
		}{
			{"spec", op.DesiredSpec, expected.DesiredSpec}, {"base", op.ConfigBaseSpec, expected.ConfigBaseSpec},
			{"source", op.DesiredSource, expected.DesiredSource}, {"origin", op.DesiredOriginSource, expected.DesiredOriginSource},
		} {
			if !finalizationIntentEqual(field.current, field.expected) {
				return model.Operation{}, fmt.Errorf("%w: import %s intent changed", ErrConflict, field.name)
			}
		}
		op.DesiredSpec = cloneAppSpec(&spec)
		op.DesiredSource = cloneAppSource(&source)
		op.DesiredOriginSource = cloneAppSource(origin)
		op.ResultMessage = "import image verified; build result saved independently of deployment"
		op.UpdatedAt = time.Now().UTC()
		return op, nil
	}
	if !s.usingDatabase() {
		var saved model.Operation
		err := s.withLockedState(true, func(state *model.State) error {
			i := findOperation(state, expected.ID)
			if i < 0 {
				return ErrNotFound
			}
			var err error
			saved, err = apply(state.Operations[i])
			if err == nil {
				state.Operations[i] = saved
			}
			return err
		})
		return saved, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Operation{}, err
	}
	defer tx.Rollback()
	op, err := s.pgGetOperationTx(ctx, tx, expected.ID, true)
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	op, err = apply(op)
	if err != nil {
		return model.Operation{}, err
	}
	if err = s.pgUpdateOperationTx(ctx, tx, op); err != nil {
		return model.Operation{}, err
	}
	return op, tx.Commit()
}

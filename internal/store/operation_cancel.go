package store

import (
	"fugue/internal/model"
	"fugue/internal/operationcancel"
)

// OperationCancellation binds the API's write port to an authenticated actor.
// Its scope is rechecked against the locked rows, not the HTTP read snapshot.
func (s *Store) OperationCancellation(actor model.Principal) operationcancel.Writer {
	return operationCancellation{store: s, actor: actor}
}

type operationCancellation struct {
	store *Store
	actor model.Principal
}

func (c operationCancellation) Cancel(id, message string) (model.Operation, error) {
	if !operationcancel.MayRequest(c.actor) {
		return model.Operation{}, operationcancel.ErrNotVisible
	}
	op, err := c.store.pgCancelOperation(id, message, &c.actor)
	if err != nil {
		return op, err
	}
	if op.Type == model.OperationTypeMigrate {
		if err := c.store.recordMigrationFailureLedger(op, op.ResultMessage, model.OperationEvidenceSourceAPI); err != nil {
			return op, err
		}
	}
	return op, nil
}

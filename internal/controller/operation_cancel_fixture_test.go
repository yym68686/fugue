package controller

import (
	"fugue/internal/model"
	"fugue/internal/store"
)

// Controller integration tests simulate the external actor through the same
// scoped cancellation port as HTTP rather than granting the Controller a
// generic operation writer.
func cancelFixtureOperation(st *store.Store, op model.Operation, reason string) (model.Operation, error) {
	return st.OperationCancellation(model.Principal{
		TenantID: op.TenantID,
		Scopes:   map[string]struct{}{"app.write": {}},
	}).Cancel(op.ID, reason)
}

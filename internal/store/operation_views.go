package store

import "fugue/internal/operationview"

// Embed the read interface, not the Store. Go's dynamic method set must not
// expose Store commands to code that only composes operation views.
type operationViews struct{ operationview.Repository }

func (s *Store) OperationViews() operationview.Repository {
	return operationViews{Repository: s}
}

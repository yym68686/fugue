package store

import "fugue/internal/operationview/contract"

// Embed the read interface, not the Store. Go's dynamic method set must not
// expose Store commands to code that only composes operation views.
type operationViews struct{ contract.Repository }

func (s *Store) OperationViews() contract.Repository {
	return operationViews{Repository: s}
}

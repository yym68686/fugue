package store

import (
	"errors"

	localpvinventory "fugue/internal/localpvsafety/inventory"
	"fugue/internal/model"
	inventoryrepo "fugue/internal/store/internal/localpvinventory"
)

func (s *Store) localPVInventory() inventoryrepo.Repository {
	return inventoryrepo.NewPostgres(s.db)
}

// LocalPVInventoryReporter binds the authenticated report command to the
// snapshot transaction, without exposing an unvalidated snapshot writer.
func (s *Store) LocalPVInventoryReporter() localpvinventory.Reporter {
	repo := s.localPVInventory()
	return localpvinventory.NewReporter(func(in model.LocalPVInventory) (model.LocalPVInventory, error) {
		out, err := repo.Upsert(in)
		if errors.Is(err, inventoryrepo.ErrMissingIdentity) {
			return out, ErrInvalidInput
		}
		return out, mapDBErr(err)
	})
}

func (s *Store) ListLocalPVInventories(filter model.LocalPVInventoryFilter) ([]model.LocalPVInventory, error) {
	out, err := s.localPVInventory().List(filter)
	return out, mapDBErr(err)
}

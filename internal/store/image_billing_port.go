package store

import (
	"strings"

	"fugue/internal/imagebilling/contract"
	"fugue/internal/model"
)

func (s *Store) ImageStorageBilling(tenantID string) contract.Repository {
	// The ledger write already canonicalizes tenant IDs. Bind both inventory
	// reads to the same ID so whitespace cannot turn a real inventory into zero.
	return imageStorageBilling{store: s, tenantID: strings.TrimSpace(tenantID)}
}

// Do not embed Store: returning it behind an interface would allow callers to
// recover unrelated actions via a type assertion.
type imageStorageBilling struct {
	store    *Store
	tenantID string
}

func (r imageStorageBilling) Apps() ([]model.App, error) {
	if r.tenantID == "" {
		return nil, ErrInvalidInput
	}
	return r.store.ListAppsMetadata(r.tenantID, false)
}

func (r imageStorageBilling) RetentionOperations() ([]model.Operation, error) {
	if r.tenantID == "" {
		return nil, ErrInvalidInput
	}
	return r.store.ListImageRetentionOperations(r.tenantID, false)
}

func (r imageStorageBilling) CommitStorage(gibibytes int64) error {
	_, err := r.store.syncTenantBillingImageStorage(r.tenantID, gibibytes)
	return err
}

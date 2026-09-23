package store

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"fugue/internal/model"
	"k8s.io/apimachinery/pkg/util/validation"
)

// AssignManagedPostgresCredentialSecret commits only the credential resource
// identity. It cannot change a password, endpoint, image or serving settings.
// A stale snapshot cannot establish intent for a concurrently changed service.
func (s *Store) AssignManagedPostgresCredentialSecret(expected model.BackingService, name string) (model.BackingService, error) {
	if expected.ID == "" || name == "" || len(validation.IsDNS1123Subdomain(name)) != 0 {
		return model.BackingService{}, ErrInvalidInput
	}
	assign := func(current model.BackingService) (model.BackingService, error) {
		if isDeletedBackingService(current) || !isManagedPostgresService(current) || current.Spec.Postgres == nil || expected.Spec.Postgres == nil {
			return model.BackingService{}, ErrInvalidInput
		}
		want, have := *expected.Spec.Postgres, *current.Spec.Postgres
		want.CredentialSecretName, have.CredentialSecretName = "", ""
		if current.TenantID != expected.TenantID || current.ProjectID != expected.ProjectID || current.OwnerAppID != expected.OwnerAppID || !reflect.DeepEqual(want, have) {
			return model.BackingService{}, ErrConflict
		}
		if current.Spec.Postgres.CredentialSecretName != "" {
			if current.Spec.Postgres.CredentialSecretName != name {
				return model.BackingService{}, ErrConflict
			}
			return current, nil
		}
		current = cloneBackingService(current)
		current.Spec.Postgres.CredentialSecretName = name
		current.UpdatedAt = time.Now().UTC()
		return current, nil
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return model.BackingService{}, err
		}
		defer tx.Rollback()
		current, err := s.pgGetBackingServiceTx(ctx, tx, expected.ID, true)
		if err != nil {
			return model.BackingService{}, mapDBErr(err)
		}
		busy, err := s.pgHasInFlightManagedPostgresExclusiveMutationTx(ctx, tx, "", current.ID)
		if err != nil {
			return model.BackingService{}, err
		}
		if busy {
			return model.BackingService{}, ErrConflict
		}
		next, err := assign(current)
		if err != nil {
			return model.BackingService{}, err
		}
		if current.Spec.Postgres.CredentialSecretName == "" {
			if err := s.pgUpdateBackingServiceTx(ctx, tx, next); err != nil {
				return model.BackingService{}, err
			}
		}
		if err := tx.Commit(); err != nil {
			return model.BackingService{}, fmt.Errorf("commit postgres credential identity: %w", err)
		}
		return next, nil
	}
	var result model.BackingService
	err := s.withLockedState(true, func(state *model.State) error {
		index := findBackingService(state, expected.ID)
		if index < 0 {
			return ErrNotFound
		}
		if hasInFlightManagedPostgresExclusiveMutationForService(state.Operations, expected.ID) {
			return ErrConflict
		}
		next, err := assign(state.BackingServices[index])
		if err != nil {
			return err
		}
		state.BackingServices[index] = next
		result = cloneBackingService(next)
		return nil
	})
	return result, err
}

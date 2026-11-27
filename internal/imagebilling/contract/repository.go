// Package contract is the restricted storage surface for image-storage billing.
package contract

import "fugue/internal/model"

// Repository is bound to the tenant selected by the caller.
type Repository interface {
	Apps() ([]model.App, error)
	RetentionOperations() ([]model.Operation, error)
	CommitStorage(gibibytes int64) error
}

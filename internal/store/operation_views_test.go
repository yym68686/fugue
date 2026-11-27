package store

import (
	"testing"

	"fugue/internal/model"
)

func TestOperationViewPortCannotAcquireCoreWrites(t *testing.T) {
	reads := (&Store{}).OperationViews()
	if _, ok := reads.(interface {
		CancelOperation(string, string) (model.Operation, error)
	}); ok {
		t.Fatal("read-only port exposes cancellation")
	}
	if _, ok := reads.(interface {
		TryClaimPendingOperation(string) (model.Operation, bool, error)
	}); ok {
		t.Fatal("read-only port exposes queue claims")
	}
}

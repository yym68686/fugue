package store

import (
	"fugue/internal/model"
	"path/filepath"
	"testing"
)

func TestReserveImportIdempotencyReclaimsDeletedResultOnce(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("Example")
	if err != nil {
		t.Fatal(err)
	}
	scope := model.IdempotencyScopeAppImportGitHub
	if _, _, err := s.ReserveIdempotencyRecord(scope, tenant.ID, "retry", "same-request"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteIdempotencyRecord(scope, tenant.ID, "retry", "deleted-app", "deleted-operation"); err != nil {
		t.Fatal(err)
	}
	record, fresh, err := s.ReserveIdempotencyRecord(scope, tenant.ID, "retry", "same-request")
	if err != nil || !fresh || record.AppID != "" || record.Status != model.IdempotencyStatusPending {
		t.Fatalf("stale result not reset: %+v fresh=%v err=%v", record, fresh, err)
	}
	if _, fresh, err := s.ReserveIdempotencyRecord(scope, tenant.ID, "retry", "same-request"); err != nil || fresh {
		t.Fatalf("concurrent duplicate acquired reservation: fresh=%v err=%v", fresh, err)
	}
	if _, _, err := s.ReserveIdempotencyRecord(scope, tenant.ID, "retry", "different-request"); err != ErrIdempotencyMismatch {
		t.Fatalf("request mismatch lost: %v", err)
	}
}

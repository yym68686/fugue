package appmail

import (
	"context"
	"errors"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

type leaseFixture struct {
	binding               model.AppMailBinding
	app                   model.App
	readErr, checkErr     error
	allow                 bool
	claims, checks, reaps int
	dnsWrites             int
	result                store.AppMailReconcileResult
}

func (f *leaseFixture) GetAppMetadata(string) (model.App, error) { return f.app, f.readErr }
func (f *leaseFixture) ClaimAppMailReconciliation(context.Context) (store.AppMailReconcileLease, bool, error) {
	f.claims++
	return store.AppMailReconcileLease{Binding: f.binding, ID: "synthetic"}, f.allow, nil
}
func (f *leaseFixture) CheckAppMailReconciliationLease(context.Context, store.AppMailReconcileLease) error {
	f.checks++
	return f.checkErr
}
func (f *leaseFixture) RevokeAppMailReconciliation(context.Context, store.AppMailReconcileLease) error {
	f.reaps++
	return nil
}
func (f *leaseFixture) SaveAppMailReconciliationDNS(context.Context, store.AppMailReconcileLease, []model.AppMailDNSRecord) error {
	f.dnsWrites++
	return nil
}
func (f *leaseFixture) FinishAppMailReconciliation(_ context.Context, _ store.AppMailReconcileLease, result store.AppMailReconcileResult) (bool, error) {
	f.result = result
	return true, nil
}

func TestLeasedReconcilerOnlyUsesOwnedCurrentClaim(t *testing.T) {
	networkFailure := errors.New("synthetic provider failure")
	for _, test := range []struct {
		name                       string
		standby, revoked, missing  bool
		readErr, checkErr, syncErr error
		wantChecks, wantReaps      int
		wantProvider, wantDNS      int
		wantResult                 store.AppMailReconcileResult
		wantErr                    error
	}{
		{name: "standby", standby: true},
		{name: "expired", checkErr: store.ErrConflict, wantChecks: 1, wantResult: store.AppMailReconcileDesiredChanged},
		{name: "provider unavailable", syncErr: networkFailure, wantChecks: 1, wantProvider: 1, wantResult: store.AppMailReconcileProviderUnavailable, wantErr: networkFailure},
		{name: "transient metadata", readErr: networkFailure, wantResult: store.AppMailReconcileMetadataUnavailable, wantErr: networkFailure},
		{name: "owner gone", missing: true, wantReaps: 1, wantResult: store.AppMailReconcileDesiredChanged},
		{name: "persisted tombstone", revoked: true, readErr: networkFailure, wantProvider: 1, wantChecks: 1, wantResult: store.AppMailReconcileSynced},
		{name: "persist DNS through claim", wantChecks: 1, wantProvider: 1, wantDNS: 1, wantResult: store.AppMailReconcileSynced},
	} {
		t.Run(test.name, func(t *testing.T) {
			binding := testBinding("test")
			fixture := &leaseFixture{binding: binding, app: testApp(binding), allow: !test.standby, readErr: test.readErr, checkErr: test.checkErr}
			if test.missing {
				fixture.readErr = store.ErrNotFound
			}
			if test.revoked {
				fixture.binding.State = "revoked"
			}
			r, err := NewLeasedReconciler(fixture, Provider{}, "apps.example.test")
			if err != nil {
				t.Fatal(err)
			}
			r.recheck = func() error { return nil }
			calls := 0
			r.sync = func(_ context.Context, actual model.AppMailBinding, write func([]model.AppMailDNSRecord) error) error {
				calls++
				if actual.State != fixture.binding.State || actual.Generation != fixture.binding.Generation {
					t.Fatalf("wrong provider effects: %+v", actual)
				}
				if test.syncErr != nil {
					return test.syncErr
				}
				if actual.State == "active" {
					return write(nil)
				}
				return nil
			}
			claimed, err := r.RunOnce(t.Context())
			if !errors.Is(err, test.wantErr) || claimed == test.standby || calls != test.wantProvider ||
				fixture.checks != test.wantChecks || fixture.reaps != test.wantReaps ||
				fixture.dnsWrites != test.wantDNS || fixture.result != test.wantResult {
				t.Fatalf("claimed=%t err=%v provider=%d fixture=%+v", claimed, err, calls, fixture)
			}
		})
	}
}

func TestLeasedReconcilerRequiresBaseDomainBeforeClaim(t *testing.T) {
	fixture := &leaseFixture{allow: true}
	if _, err := NewLeasedReconciler(fixture, Provider{}, " .. "); err == nil || fixture.claims != 0 {
		t.Fatalf("unsafe blank domain: claims=%d err=%v", fixture.claims, err)
	}
}

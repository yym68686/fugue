package store

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAppSSHObservationRejectsWrongIdentityAndRecordsFailure(test *testing.T) {
	stateStore, app, _ := setupSSHIdentityStore(test)
	identity, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID)
	if err != nil {
		test.Fatal(err)
	}
	_, endpoint, err := stateStore.UpsertAppSSHConfig(app.ID, AppSSHUpdate{Enabled: true, Hostname: "ssh.example", PublicPortStart: 23000, PublicPortEnd: 23001, AuthorizedKeys: []string{testSSHPublicKey}})
	if err != nil {
		test.Fatal(err)
	}
	now := time.Now().UTC()
	observation := model.AppSSHObservation{ID: model.NewID("sshobs"), TenantID: app.TenantID, AppID: app.ID, IdentityID: identity.ID, EndpointID: endpoint.ID, RouteDigest: model.AppSSHRouteDigest(endpoint), Scope: "public_endpoint", Observer: "observer", Target: model.AppSSHPublicAddress(endpoint), Algorithm: identity.Algorithm, Fingerprint: identity.Fingerprint, KeyExchangeVerified: true, IdentityMatched: true, ObservedAt: now, ExpiresAt: now.Add(time.Minute), Reason: "identity_verified"}
	wrong := observation
	wrong.IdentityID = "other-identity"
	if err := stateStore.RecordAppSSHObservation(wrong); !errors.Is(err, ErrConflict) {
		test.Fatal("wrong identity observation accepted", err)
	}
	wrong = observation
	wrong.Fingerprint = "SHA256:another"
	if err := stateStore.RecordAppSSHObservation(wrong); !errors.Is(err, ErrInvalidInput) {
		test.Fatal("mismatched positive evidence accepted", err)
	}
	if err := stateStore.RecordAppSSHObservation(observation); err != nil {
		test.Fatal(err)
	}
	failure := observation
	failure.ID = model.NewID("sshobs")
	failure.ObservedAt = now.Add(time.Millisecond)
	failure.IdentityMatched = false
	failure.KeyExchangeVerified = false
	failure.Algorithm = ""
	failure.Fingerprint = ""
	failure.Reason = "target_unreachable"
	if err := stateStore.RecordAppSSHObservation(failure); err != nil {
		test.Fatal(err)
	}
	latest, err := stateStore.LatestAppSSHObservation(app.TenantID, app.ID)
	if err != nil || latest.ID != failure.ID || latest.IdentityMatched {
		test.Fatal("failed observation was hidden by older success", latest, err)
	}
	if _, err := stateStore.LatestAppSSHObservation("tenant_other", app.ID); !errors.Is(err, ErrNotFound) {
		test.Fatal("observation leaked across tenants", err)
	}
}

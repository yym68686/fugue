package store

import (
	"errors"
	"sync"
	"testing"

	"fugue/internal/model"
)

func managedSSHSpec(identityID string) model.ManagedSSHIntentSpec {
	return model.ManagedSSHIntentSpec{Enabled: true, IdentityID: identityID, Container: "workspace", User: "app", WorkingDirectory: "/workspace", AuthorizedKeyIDs: []string{}, AllowPTY: true, AllowSFTP: true, MaxConnections: 8, MaxConnectionsPerIP: 4, MaxChannels: 4}
}

func verifyManagedSSHIntentCAS(test *testing.T, stateStore *Store, app model.App, identityID string) {
	test.Helper()
	spec := managedSSHSpec(identityID)
	for _, expected := range []uint64{0, 1} {
		var group sync.WaitGroup
		results := make(chan error, 10)
		for index := 0; index < cap(results); index++ {
			group.Add(1)
			go func() {
				defer group.Done()
				independent := stateStore

				_, err := independent.PutManagedSSHIntent(app.TenantID, app.ID, expected, spec)
				results <- err
			}()
		}
		group.Wait()
		close(results)
		accepted := 0
		for err := range results {
			if err == nil {
				accepted++
			} else if !errors.Is(err, ErrConflict) {
				test.Fatal(err)
			}
		}
		if accepted != 1 {
			test.Fatalf("CAS admitted %d writes", accepted)
		}
	}
	current, err := stateStore.GetManagedSSHIntent(app.TenantID, app.ID)
	if err != nil || current.Generation != 2 {
		test.Fatal("generation did not advance", err)
	}
	if _, err := stateStore.GetManagedSSHIntent("other-tenant", app.ID); !errors.Is(err, ErrNotFound) {
		test.Fatal("intent crossed tenant boundary", err)
	}
	if _, err := stateStore.PutManagedSSHIntent("other-tenant", app.ID, 2, spec); !errors.Is(err, ErrNotFound) {
		test.Fatal("cross-tenant write admitted", err)
	}
	wrongIdentity := spec
	wrongIdentity.IdentityID = "identity-other"
	if _, err := stateStore.PutManagedSSHIntent(app.TenantID, app.ID, 2, wrongIdentity); !errors.Is(err, ErrConflict) {
		test.Fatal("identity replacement admitted", err)
	}
	missingKey := spec
	missingKey.AuthorizedKeyIDs = []string{"deleted-or-other-tenant"}
	if _, err := stateStore.PutManagedSSHIntent(app.TenantID, app.ID, 2, missingKey); !errors.Is(err, ErrInvalidInput) {
		test.Fatal("non-current key admitted", err)
	}
	missingKey.Enabled = false
	disabled, err := stateStore.PutManagedSSHIntent(app.TenantID, app.ID, 2, missingKey)
	if err != nil || disabled.Spec.Enabled || !disabled.CreatedAt.Equal(current.CreatedAt) {
		test.Fatal("revocation depended on obsolete keys", err)
	}
}

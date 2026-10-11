package store

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"fugue/internal/model"
)

func TestManagedSSHBindingIndependentOfApplicationSSH(test *testing.T) {
	for _, existingRoute := range []bool{false, true} {
		stateStore, app, _ := setupSSHIdentityStore(test)
		identity, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID)
		if err != nil {
			test.Fatal(err)
		}
		if existingRoute {
			_, _, err := stateStore.UpsertAppSSHConfig(app.ID, AppSSHUpdate{Enabled: true, Hostname: "ssh.example", PublicPortStart: 26000, PublicPortEnd: 27000, AuthorizedKeys: []string{testSSHPublicKey}})
			if err != nil {
				test.Fatal(err)
			}
		}
		verifyManagedSSHBindingStore(test, stateStore, app, identity.ID, existingRoute)
	}
}

func verifyManagedSSHBindingStore(test *testing.T, stateStore *Store, app model.App, identityID string, existingRoute bool) {
	test.Helper()
	before, err := stateStore.GetApp(app.ID)
	if err != nil {
		test.Fatal(err)
	}
	digest := ""
	start, end := 26000, 27000
	var legacy model.AppSSHEndpoint
	if existingRoute {
		legacy, err = stateStore.GetAppSSHEndpoint(app.ID)
		if err != nil {
			test.Fatal(err)
		}
		digest = model.AppSSHRouteDigest(legacy)
		start, end = legacy.PublicPort, legacy.PublicPort+10
		if _, err := stateStore.PrepareManagedSSHBinding(app.TenantID, app.ID, identityID, "stale-route", "ssh.example", start, end); !errors.Is(err, ErrConflict) {
			test.Fatal("unreviewed legacy route imported", err)
		}
	}
	var group sync.WaitGroup
	bindings := make(chan model.ManagedSSHPublicBinding, 8)
	failures := make(chan error, 8)
	for worker := 0; worker < cap(bindings); worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			independent := stateStore
			if !stateStore.usingDatabase() {
				independent = New(stateStore.path)
			}
			binding, err := independent.PrepareManagedSSHBinding(app.TenantID, app.ID, identityID, digest, "ssh.example", start, end)
			bindings <- binding
			failures <- err
		}()
	}
	group.Wait()
	close(bindings)
	close(failures)
	for err := range failures {
		if err != nil {
			test.Fatal(err)
		}
	}
	var expected model.ManagedSSHPublicBinding
	for binding := range bindings {
		if expected.ID == "" {
			expected = binding
		}
		if !reflect.DeepEqual(binding, expected) {
			test.Fatal("concurrent preparation changed public binding")
		}
	}
	if existingRoute && (expected.PublicPort != legacy.PublicPort || expected.Hostname != legacy.Hostname || expected.LegacyRouteDigest != digest) {
		test.Fatal("migration changed the reviewed public address")
	}
	after, err := stateStore.GetApp(app.ID)
	if err != nil || !reflect.DeepEqual(after.Spec, before.Spec) {
		test.Fatal("binding preparation edited application config", err)
	}
	if _, err := stateStore.GetManagedSSHBinding("other-tenant", app.ID); !errors.Is(err, ErrNotFound) {
		test.Fatal("binding crossed tenant boundary", err)
	}
	if existingRoute {
		if _, err := stateStore.RotateAppSSHPort(app.ID, start, end); err == nil {
			test.Fatal("legacy port rotation rewrote managed address")
		}
		if _, _, err := stateStore.DisableAppSSH(app.ID); err != nil {
			test.Fatal(err)
		}
		if stateStore.usingDatabase() {
			if _, err := stateStore.db.Exec(`UPDATE fugue_app_ssh_endpoints SET released_at=now()-interval '2 hours' WHERE app_id=$1`, app.ID); err != nil {
				test.Fatal(err)
			}
		}
	}
	stored, err := stateStore.GetManagedSSHBinding(app.TenantID, app.ID)
	if err != nil || !reflect.DeepEqual(expected, stored) {
		test.Fatal("application SSH update changed independent binding", err)
	}
	other, err := stateStore.CreateApp(app.TenantID, app.ProjectID, model.NewID("another"), "", model.AppSpec{Image: app.Spec.Image, Replicas: 1})
	if err != nil {
		test.Fatal(err)
	}
	_, allocated, err := stateStore.UpsertAppSSHConfig(other.ID, AppSSHUpdate{Enabled: true, Hostname: "ssh.example", PublicPortStart: start, PublicPortEnd: end, AuthorizedKeys: []string{testSSHPublicKey}})
	if err != nil || allocated.PublicPort == expected.PublicPort {
		test.Fatal("native allocator reused a managed port", err)
	}
	rotated, err := stateStore.RotateAppSSHPort(other.ID, start, end)
	if err != nil || rotated.PublicPort == allocated.PublicPort || rotated.PublicPort == expected.PublicPort {
		test.Fatal("native rotation reused current or managed port", err)
	}
	if stateStore.usingDatabase() {
		if _, err := stateStore.db.Exec(`UPDATE fugue_app_ssh_endpoints SET public_port=$1 WHERE app_id=$2`, expected.PublicPort, other.ID); err == nil {
			test.Fatal("older writer bypassed independent port reservation")
		}
		if _, err := stateStore.db.Exec(`UPDATE fugue_managed_ssh_bindings SET public_port=public_port+1,body=jsonb_set(body,'{public_port}',to_jsonb(public_port+1)) WHERE app_id=$1`, app.ID); err == nil {
			test.Fatal("binding mutated without an explicit migration")
		}
	}
	verifyMixedSSHPortAllocation(test, stateStore, app)
}

func verifyMixedSSHPortAllocation(test *testing.T, stateStore *Store, owner model.App) {
	test.Helper()
	type operation struct {
		app        model.App
		identityID string
	}
	operations := make([]operation, 8)
	for index := range operations {
		app, err := stateStore.CreateApp(owner.TenantID, owner.ProjectID, model.NewID("port-allocation"), "", model.AppSpec{Image: owner.Spec.Image, Replicas: 1})
		if err != nil {
			test.Fatal(err)
		}
		operations[index].app = app
		if index%2 == 0 {
			identity, err := stateStore.PrepareAppSSHIdentity(app.TenantID, app.ID)
			if err != nil {
				test.Fatal(err)
			}
			operations[index].identityID = identity.ID
		}
	}
	ports := make(chan int, len(operations))
	failures := make(chan error, len(operations))
	var group sync.WaitGroup
	for _, operation := range operations {
		group.Add(1)
		go func() {
			defer group.Done()
			independent := stateStore
			if !stateStore.usingDatabase() {
				independent = New(stateStore.path)
			}
			if operation.identityID != "" {
				binding, err := independent.PrepareManagedSSHBinding(operation.app.TenantID, operation.app.ID, operation.identityID, "", "ssh.example", 40000, 41000)
				ports <- binding.PublicPort
				failures <- err
			} else {
				_, endpoint, err := independent.UpsertAppSSHConfig(operation.app.ID, AppSSHUpdate{Enabled: true, Hostname: "ssh.example", PublicPortStart: 40000, PublicPortEnd: 41000, AuthorizedKeys: []string{testSSHPublicKey}})
				ports <- endpoint.PublicPort
				failures <- err
			}
		}()
	}
	group.Wait()
	close(ports)
	close(failures)
	for err := range failures {
		if err != nil {
			test.Fatal("mixed allocation failed", err)
		}
	}
	seen := map[int]bool{}
	for port := range ports {
		if port < 40000 || port > 41000 || seen[port] {
			test.Fatal("mixed writers assigned overlapping public ports", port)
		}
		seen[port] = true
	}
}

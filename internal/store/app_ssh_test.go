package store

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

const testSSHPublicKey = "ssh-ed25519 AQIDBAUGBwg= laptop"
const testOtherSSHPublicKey = "ssh-ed25519 CQoLDA0ODxA= workstation"

func TestAppSSHKeyCRUDAndTenantIsolation(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenantA, err := s.CreateTenant("tenant-a")
	if err != nil {
		t.Fatalf("create tenant a: %v", err)
	}
	tenantB, err := s.CreateTenant("tenant-b")
	if err != nil {
		t.Fatalf("create tenant b: %v", err)
	}

	keyA, err := s.CreateSSHKey(tenantA.ID, "laptop", testSSHPublicKey)
	if err != nil {
		t.Fatalf("create ssh key a: %v", err)
	}
	if _, err := s.CreateSSHKey(tenantA.ID, "duplicate", testSSHPublicKey); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected duplicate active key conflict, got %v", err)
	}
	if _, err := s.CreateSSHKey(tenantB.ID, "same-fingerprint-other-tenant", testSSHPublicKey); err != nil {
		t.Fatalf("same key should be allowed in another tenant: %v", err)
	}

	keysA, err := s.ListSSHKeys(tenantA.ID, false)
	if err != nil {
		t.Fatalf("list tenant a keys: %v", err)
	}
	if len(keysA) != 1 || keysA[0].ID != keyA.ID {
		t.Fatalf("expected only tenant a key, got %+v", keysA)
	}
	allKeys, err := s.ListSSHKeys("", true)
	if err != nil {
		t.Fatalf("list all keys: %v", err)
	}
	if len(allKeys) != 2 {
		t.Fatalf("expected platform admin to see two keys, got %+v", allKeys)
	}

	deleted, err := s.DeleteSSHKey(keyA.ID)
	if err != nil {
		t.Fatalf("delete key: %v", err)
	}
	if deleted.ID != keyA.ID {
		t.Fatalf("unexpected deleted key: %+v", deleted)
	}
	keysA, err = s.ListSSHKeys(tenantA.ID, false)
	if err != nil {
		t.Fatalf("list tenant a keys after delete: %v", err)
	}
	if len(keysA) != 0 {
		t.Fatalf("expected tenant a key to be deleted, got %+v", keysA)
	}
}

func TestAppSSHConcurrentPortAllocationIsUnique(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("acme")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "us-runtime", model.RuntimeTypeManagedShared, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}

	const appCount = 8
	apps := make([]model.App, appCount)
	for i := 0; i < appCount; i++ {
		app, err := s.CreateApp(tenant.ID, project.ID, "agent-"+string(rune('a'+i)), "", model.AppSpec{
			Image:     "example/agent:ssh",
			RuntimeID: runtimeObj.ID,
			Replicas:  1,
		})
		if err != nil {
			t.Fatalf("create app %d: %v", i, err)
		}
		apps[i] = app
	}

	endpoints := make(chan model.AppSSHEndpoint, appCount)
	errs := make(chan error, appCount)
	var wg sync.WaitGroup
	for _, app := range apps {
		app := app
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, endpoint, err := s.UpsertAppSSHConfig(app.ID, AppSSHUpdate{
				Enabled:         true,
				AuthorizedKeys:  []string{testSSHPublicKey},
				Hostname:        "ssh.fugue.pro",
				PublicPortStart: 25000,
				PublicPortEnd:   25000 + appCount - 1,
			})
			if err != nil {
				errs <- err
				return
			}
			endpoints <- endpoint
		}()
	}
	wg.Wait()
	close(errs)
	close(endpoints)
	for err := range errs {
		t.Fatalf("enable ssh concurrently: %v", err)
	}
	seen := map[int]struct{}{}
	for endpoint := range endpoints {
		if endpoint.PublicPort < 25000 || endpoint.PublicPort >= 25000+appCount {
			t.Fatalf("port outside expected range: %+v", endpoint)
		}
		if _, exists := seen[endpoint.PublicPort]; exists {
			t.Fatalf("duplicate public port allocated: %d", endpoint.PublicPort)
		}
		seen[endpoint.PublicPort] = struct{}{}
	}
	if len(seen) != appCount {
		t.Fatalf("expected %d unique ports, got %d", appCount, len(seen))
	}
}

func TestAppSSHReleasedPortCooldown(t *testing.T) {
	now := time.Now().UTC()
	oldRelease := now.Add(-2 * time.Hour)
	state := &model.State{
		AppSSHEndpoints: []model.AppSSHEndpoint{
			{
				AppID:      "app_recent",
				PublicPort: 26000,
				Status:     model.AppSSHEndpointStatusReleased,
				ReleasedAt: &now,
			},
			{
				AppID:      "app_old",
				PublicPort: 26001,
				Status:     model.AppSSHEndpointStatusReleased,
				ReleasedAt: &oldRelease,
			},
		},
	}
	port, err := allocateAppSSHPublicPortState(state, 26000, 26001, 0)
	if err != nil {
		t.Fatalf("allocate after cooldown: %v", err)
	}
	if port != 26001 {
		t.Fatalf("expected old released port to be reused first, got %d", port)
	}
}

func TestAppSSHAllocatesPortsAndRoutesByEdgeGroup(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("acme")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "us-runtime", model.RuntimeTypeManagedOwned, "", map[string]string{
		runtimepkg.LocationCountryCodeLabelKey: "us",
	})
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	key, err := s.CreateSSHKey(tenant.ID, "laptop", testSSHPublicKey)
	if err != nil {
		t.Fatalf("create ssh key: %v", err)
	}
	appA, err := s.CreateApp(tenant.ID, project.ID, "agent-a", "", model.AppSpec{
		Image:     "example/agent:ssh",
		RuntimeID: runtimeObj.ID,
		Replicas:  1,
	})
	if err != nil {
		t.Fatalf("create app a: %v", err)
	}
	appB, err := s.CreateApp(tenant.ID, project.ID, "agent-b", "", model.AppSpec{
		Image:     "example/agent:ssh",
		RuntimeID: runtimeObj.ID,
		Replicas:  1,
	})
	if err != nil {
		t.Fatalf("create app b: %v", err)
	}

	updatedA, endpointA, err := s.UpsertAppSSHConfig(appA.ID, AppSSHUpdate{
		Enabled:          true,
		AuthorizedKeyIDs: []string{key.ID},
		Hostname:         "ssh.fugue.pro",
		PublicPortStart:  23000,
		PublicPortEnd:    23001,
	})
	if err != nil {
		t.Fatalf("enable app a ssh: %v", err)
	}
	if endpointA.PublicPort != 23000 || endpointA.EdgeGroupID != "edge-group-country-us" {
		t.Fatalf("unexpected endpoint a: %+v", endpointA)
	}
	if updatedA.Spec.SSH == nil || len(updatedA.Spec.SSH.AuthorizedKeys) != 1 || updatedA.Spec.SSH.AuthorizedKeys[0] != key.PublicKey {
		t.Fatalf("expected app spec to include resolved authorized key, got %+v", updatedA.Spec.SSH)
	}

	_, endpointB, err := s.UpsertAppSSHConfig(appB.ID, AppSSHUpdate{
		Enabled:         true,
		AuthorizedKeys:  []string{testOtherSSHPublicKey},
		Hostname:        "ssh.fugue.pro",
		PublicPortStart: 23000,
		PublicPortEnd:   23001,
	})
	if err != nil {
		t.Fatalf("enable app b ssh: %v", err)
	}
	if endpointB.PublicPort != 23001 {
		t.Fatalf("expected second app to get next public port, got %+v", endpointB)
	}

	routes, err := s.ListEdgeSSHRoutes(AppSSHRouteOptions{EdgeGroupID: "edge-group-country-us"})
	if err != nil {
		t.Fatalf("list routes: %v", err)
	}
	if len(routes) != 2 {
		t.Fatalf("expected two routes for us edge group, got %+v", routes)
	}
	filtered, err := s.ListEdgeSSHRoutes(AppSSHRouteOptions{EdgeGroupID: "edge-group-country-de"})
	if err != nil {
		t.Fatalf("list filtered routes: %v", err)
	}
	if len(filtered) != 0 {
		t.Fatalf("expected no routes for de edge group, got %+v", filtered)
	}
}

func TestAppSSHMarksExternalOwnedRuntimeUnsupported(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("acme")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "external", model.RuntimeTypeExternalOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "agent", "", model.AppSpec{
		Image:     "example/agent:ssh",
		RuntimeID: runtimeObj.ID,
		Replicas:  1,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, endpoint, err := s.UpsertAppSSHConfig(app.ID, AppSSHUpdate{
		Enabled:         true,
		AuthorizedKeys:  []string{testSSHPublicKey},
		Hostname:        "ssh.fugue.pro",
		PublicPortStart: 24000,
		PublicPortEnd:   24000,
	})
	if err != nil {
		t.Fatalf("enable ssh: %v", err)
	}
	if endpoint.Status != model.AppSSHEndpointStatusUnsupported {
		t.Fatalf("expected unsupported endpoint, got %+v", endpoint)
	}
	routes, err := s.ListEdgeSSHRoutes(AppSSHRouteOptions{})
	if err != nil {
		t.Fatalf("list routes: %v", err)
	}
	if len(routes) != 0 {
		t.Fatalf("unsupported endpoint must not publish routes, got %+v", routes)
	}
}

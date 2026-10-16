package store

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func TestMachineSharedPoolPreservesDisabledBuilds(t *testing.T) {
	t.Parallel()
	testMachineSharedPoolPreservesDisabledBuilds(t, "")
}

func TestMachineSharedPoolPreservesDisabledBuildsPostgres(t *testing.T) {
	address := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set disposable local PostgreSQL URL")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("requires a disposable loopback fugue_test database")
	}
	testMachineSharedPoolPreservesDisabledBuilds(t, address)
}

func testMachineSharedPoolPreservesDisabledBuilds(t *testing.T, address string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.json")
	s := New(path, address)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if s.db != nil {
		defer s.db.Close()
	}
	tenant, err := s.CreateTenant("Workload Isolation")
	if err != nil {
		t.Fatal(err)
	}
	_, secret, err := s.CreateNodeKey(tenant.ID, "worker")
	if err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{runtimepkg.SharedPoolLabelKey: runtimepkg.SharedPoolLabelValue, runtimepkg.BuildNodeLabelKey: runtimepkg.BuildNodeLabelValue}
	_, runtime, err := s.BootstrapClusterNode(secret, "worker-storage", "https://worker.example.test", labels, "worker", "fingerprint")
	if err != nil {
		t.Fatal(err)
	}
	runtime, err = s.SetRuntimePoolMode(runtime.ID, model.RuntimePoolModeInternalShared)
	if err != nil {
		t.Fatal(err)
	}
	machine, err := s.GetMachineByClusterNodeName(runtime.ClusterNodeName)
	if err != nil {
		t.Fatal(err)
	}
	policy := machine.Policy
	policy.AllowBuilds = false
	if _, err := s.SetMachinePolicyByClusterNodeName(runtime.ClusterNodeName, policy); err != nil {
		t.Fatal(err)
	}
	// Replay runtime registration with stale build labels and a pool refresh.
	// Neither operational fact is allowed to re-enable explicit build intent.
	if _, _, err := s.BootstrapClusterNode(secret, "worker-storage", "https://worker.example.test", labels, "worker", "fingerprint"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetRuntimePoolMode(runtime.ID, model.RuntimePoolModeInternalShared); err != nil {
		t.Fatal(err)
	}
	reopened := New(path, address)
	if err := reopened.Init(); err != nil {
		t.Fatal(err)
	}
	if reopened.db != nil {
		defer reopened.db.Close()
	}
	machine, err = reopened.GetMachineByClusterNodeName(runtime.ClusterNodeName)
	if err != nil {
		t.Fatal(err)
	}
	if machine.Policy.AllowBuilds || !machine.Policy.AllowAppRuntime || !machine.Policy.AllowSharedPool || machine.RuntimeID != runtime.ID || machine.TenantID != tenant.ID {
		t.Fatalf("shared runtime overwrote build isolation or ownership: %+v", machine)
	}
}

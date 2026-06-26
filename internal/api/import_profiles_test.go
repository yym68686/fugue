package api

import (
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestNormalizeAppFilesUsesDefaultConfigPath(t *testing.T) {
	files, err := normalizeAppFiles("providers: []", nil)
	if err != nil {
		t.Fatalf("normalize app files: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].Path != "/home/api.yaml" {
		t.Fatalf("unexpected file path: %s", files[0].Path)
	}
	if files[0].Mode != 0o600 {
		t.Fatalf("unexpected file mode: %d", files[0].Mode)
	}
}

func TestNormalizeAppFilesAllowsEmptyInput(t *testing.T) {
	files, err := normalizeAppFiles("", nil)
	if err != nil {
		t.Fatalf("normalize app files: %v", err)
	}
	if files != nil {
		t.Fatalf("expected nil files, got %#v", files)
	}
}

func TestNormalizeGenericPostgresSpecDefaultsToAppScopedUserForCNPG(t *testing.T) {
	spec, err := normalizeGenericPostgresSpec("fugue-web", nil)
	if err != nil {
		t.Fatalf("normalize postgres spec: %v", err)
	}
	if spec.Image != "" {
		t.Fatalf("expected generic managed postgres image to stay empty, got %q", spec.Image)
	}
	if spec.User != "fugue_web" {
		t.Fatalf("expected app-scoped user fugue_web, got %q", spec.User)
	}
}

func TestNormalizeGenericPostgresSpecStripsOfficialPostgresImage(t *testing.T) {
	spec, err := normalizeGenericPostgresSpec("fugue-web", &model.AppPostgresSpec{
		Image: "postgres:17.6-alpine",
	})
	if err != nil {
		t.Fatalf("normalize postgres spec: %v", err)
	}
	if spec.Image != "" {
		t.Fatalf("expected official postgres image to be stripped, got %q", spec.Image)
	}
}

func TestNormalizeGenericPostgresSpecKeepsCNPGImage(t *testing.T) {
	spec, err := normalizeGenericPostgresSpec("fugue-web", &model.AppPostgresSpec{
		Image: "ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie",
	})
	if err != nil {
		t.Fatalf("normalize postgres spec: %v", err)
	}
	if spec.Image != "ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie" {
		t.Fatalf("expected CNPG image to be preserved, got %q", spec.Image)
	}
}

func TestNormalizeGenericPostgresSpecKeepsManagedPostgresOverrides(t *testing.T) {
	resources := &model.ResourceSpec{
		CPUMilliCores:   300,
		MemoryMebibytes: 768,
	}
	spec, err := normalizeGenericPostgresSpec("fugue-web", &model.AppPostgresSpec{
		RuntimeID:                        "runtime_primary",
		FailoverTargetRuntimeID:          "runtime_failover",
		PrimaryNodeName:                  "node-a",
		PrimaryPlacementPendingRebalance: true,
		StorageSize:                      "5Gi",
		StorageClassName:                 "fast-rwo",
		Instances:                        2,
		SynchronousReplicas:              1,
		Resources:                        resources,
	})
	if err != nil {
		t.Fatalf("normalize postgres spec: %v", err)
	}
	if spec.RuntimeID != "runtime_primary" {
		t.Fatalf("expected runtime override to be preserved, got %q", spec.RuntimeID)
	}
	if spec.FailoverTargetRuntimeID != "runtime_failover" {
		t.Fatalf("expected failover runtime override to be preserved, got %q", spec.FailoverTargetRuntimeID)
	}
	if spec.PrimaryNodeName != "node-a" {
		t.Fatalf("expected primary node override to be preserved, got %q", spec.PrimaryNodeName)
	}
	if !spec.PrimaryPlacementPendingRebalance {
		t.Fatal("expected primary placement rebalance override to be preserved")
	}
	if spec.StorageSize != "5Gi" {
		t.Fatalf("expected storage size override 5Gi, got %q", spec.StorageSize)
	}
	if spec.StorageClassName != "fast-rwo" {
		t.Fatalf("expected storage class override fast-rwo, got %q", spec.StorageClassName)
	}
	if spec.Instances != 2 {
		t.Fatalf("expected instances override 2, got %d", spec.Instances)
	}
	if spec.SynchronousReplicas != 1 {
		t.Fatalf("expected synchronous replicas override 1, got %d", spec.SynchronousReplicas)
	}
	if spec.Resources == nil || spec.Resources.CPUMilliCores != 300 || spec.Resources.MemoryMebibytes != 768 {
		t.Fatalf("expected resource override to be preserved, got %+v", spec.Resources)
	}
	if spec.Resources == resources {
		t.Fatal("expected resources override to be cloned")
	}
}

func TestNormalizeGenericPostgresSpecRejectsReservedCNPGUser(t *testing.T) {
	_, err := normalizeGenericPostgresSpec("fugue-web", &model.AppPostgresSpec{
		User: "postgres",
	})
	if err == nil {
		t.Fatal("expected reserved user error")
	}
	if !strings.Contains(err.Error(), `managed CNPG postgres user "postgres" is reserved`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

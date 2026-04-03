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

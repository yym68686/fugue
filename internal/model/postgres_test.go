package model

import (
	"strings"
	"testing"
)

func TestDefaultManagedPostgresUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		want    string
	}{
		{
			name:    "cnpg uses app scoped user",
			appName: "fugue-web",
			want:    "fugue_web",
		},
		{
			name:    "leading digit is prefixed",
			appName: "123-demo",
			want:    "app_123_demo",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := DefaultManagedPostgresUser(tc.appName); got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

func TestValidateManagedPostgresUser(t *testing.T) {
	t.Parallel()

	err := ValidateManagedPostgresUser("fugue-web", AppPostgresSpec{
		User: "postgres",
	})
	if err == nil {
		t.Fatal("expected reserved user error")
	}
	if !strings.Contains(err.Error(), `managed CNPG postgres user "postgres" is reserved`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeManagedPostgresImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		image string
		want  string
	}{
		{
			name:  "blank stays blank",
			image: "",
			want:  "",
		},
		{
			name:  "official postgres image is stripped",
			image: "postgres:16-alpine",
			want:  "",
		},
		{
			name:  "docker hub postgres image is stripped",
			image: "docker.io/library/postgres:17",
			want:  "",
		},
		{
			name:  "cnpg image is preserved",
			image: "ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie",
			want:  "ghcr.io/cloudnative-pg/postgresql:18.3-system-trixie",
		},
		{
			name:  "other custom image is preserved",
			image: "ghcr.io/example/custom-postgres:latest",
			want:  "ghcr.io/example/custom-postgres:latest",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := NormalizeManagedPostgresImage(tc.image); got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

func TestCloneAppPostgresSpecDetachesBootstrapAndRuntimeResources(t *testing.T) {
	t.Parallel()

	original := &AppPostgresSpec{
		Resources:        &ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 512},
		RuntimeResources: &ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 768},
	}
	cloned := CloneAppPostgresSpec(original)
	cloned.Resources.CPUMilliCores = 200
	cloned.RuntimeResources.MemoryMebibytes = 1024

	if original.Resources.CPUMilliCores != 100 || original.RuntimeResources.MemoryMebibytes != 768 {
		t.Fatalf("postgres resource clone aliases original: original=%+v clone=%+v", original, cloned)
	}
}

func TestManagedPostgresRuntimeResourcesFallsBackToBootstrap(t *testing.T) {
	t.Parallel()

	bootstrap := ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 512}
	runtimeTarget := ResourceSpec{CPUMilliCores: 250, MemoryMebibytes: 768}
	if got := ManagedPostgresRuntimeResources(AppPostgresSpec{Resources: &bootstrap}); got != bootstrap {
		t.Fatalf("expected bootstrap fallback, got %+v", got)
	}
	if got := ManagedPostgresRuntimeResources(AppPostgresSpec{Resources: &bootstrap, RuntimeResources: &runtimeTarget}); got != runtimeTarget {
		t.Fatalf("expected runtime target, got %+v", got)
	}
}

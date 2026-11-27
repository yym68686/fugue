package backupusage

import (
	"fmt"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestBackupUsageTenantObjectOwnershipIsCanonical(t *testing.T) {
	tests := map[string]bool{
		"apps/tenant_a/project/app/run/database.dump":                  true,
		"data-workspaces/tenant_a/project/workspace/run/manifest.json": true,
		"apps/tenant_b/project/app/run/database.dump":                  false,
		"control-plane/2026/07/30/12/run/control-plane.dump":           false,
		"tenant_a/arbitrary": false,
	}
	for key, want := range tests {
		t.Run(fmt.Sprintf("%t-%s", want, strings.ReplaceAll(key, "/", "-")), func(t *testing.T) {
			if got := backupUsageTenantOwnsLogicalObject("tenant_a", key); got != want {
				t.Fatalf("ownership for %q = %t, want %t", key, got, want)
			}
		})
	}
}

func TestBackupUsageR2NamespaceMustRoundTripExactly(t *testing.T) {
	tests := []struct {
		name    string
		backend model.DataBackend
		want    bool
	}{
		{name: "safe prefix", backend: model.DataBackend{Endpoint: "https://r2.example.test", Prefix: "backup-root"}, want: true},
		{name: "empty prefix", backend: model.DataBackend{Endpoint: "https://r2.example.test"}, want: true},
		{name: "missing endpoint", backend: model.DataBackend{Prefix: "backup-root"}, want: false},
		{name: "cleaned prefix", backend: model.DataBackend{Endpoint: "https://r2.example.test", Prefix: "backup-root/../other-root"}, want: false},
		{name: "dot prefix", backend: model.DataBackend{Endpoint: "https://r2.example.test", Prefix: "."}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NamespaceOf(tt.backend).Measurable(); got != tt.want {
				t.Fatalf("measurable = %t, want %t", got, tt.want)
			}
		})
	}
}

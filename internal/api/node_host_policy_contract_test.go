package api

import (
	"strings"
	"testing"

	"fugue/internal/nodehostpolicy"
)

func TestNodeHostPolicyInstallScriptWiring(t *testing.T) {
	server := &Server{}
	const base = "https://control.example.test"
	updater, join := server.nodeUpdaterInstallScript(base), server.joinClusterInstallScript(base)
	for name, script := range map[string]string{"updater": updater, "join": join} {
		if !strings.Contains(script, nodehostpolicy.MemorySafetyShell()) ||
			strings.Contains(script, "__FUGUE_HOST_MEMORY_SAFETY_LIBRARY__") {
			t.Fatalf("%s installer did not embed the exact memory safety policy", name)
		}
	}
	if !strings.Contains(updater, nodehostpolicy.JournaldShell()) ||
		strings.Contains(updater, "__FUGUE_HOST_JOURNALD_POLICY_LIBRARY__") {
		t.Fatal("node updater did not embed the exact journald policy")
	}
}

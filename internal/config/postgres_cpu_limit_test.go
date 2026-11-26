package config

import "testing"

func TestManagedPostgresCPULimitEnrollment(t *testing.T) {
	const env = "FUGUE_MANAGED_POSTGRES_AUTO_CPU_LIMIT_UPSCALE_TARGETS_JSON"
	const valid = `{"service_id":"svc","namespace":"ns","cluster":"pg","cluster_uid":"uid","system_id":"123","max_limit_millicores":500}`
	t.Setenv(env, "")
	if got := ControllerFromEnv().ManagedPostgresInPlaceResize.AutoCPULimitUpscaleTargets; len(got) != 0 {
		t.Fatal("limit upscale must be opt in")
	}
	t.Setenv(env, "["+valid+"]")
	got := ControllerFromEnv().ManagedPostgresInPlaceResize.AutoCPULimitUpscaleTargets
	if len(got) != 1 || got[0].ServiceID != "svc" || got[0].MaxLimitMilliCores != 500 {
		t.Fatalf("wrong enrollment: %v", got)
	}
	for _, invalid := range []string{"{", "[{}]", "[" + valid + "," + valid + "]", `[{"service_id":"svc","namespace":"ns","cluster":"pg","cluster_uid":"uid","system_id":"123","max_limit_millicores":0}]`} {
		if got := parseManagedPostgresCPULimitTargets(invalid); len(got) != 0 {
			t.Fatalf("accepted invalid enrollment %q: %v", invalid, got)
		}
	}
}

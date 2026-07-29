package api

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func writeHostMemorySafetyFakeCommand(t *testing.T, dir, name, body string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
		t.Fatalf("write fake command %s: %v", name, err)
	}
}

func hostMemorySafetyHarness(t *testing.T, memTotalKiB int64, k3sVersion, swaps string) (string, map[string]string) {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	tmpDir := t.TempDir()
	binDir := filepath.Join(tmpDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("create fake bin: %v", err)
	}
	writeHostMemorySafetyFakeCommand(t, binDir, "id", `if [ "${1:-}" = "-u" ]; then echo 0; else exit 1; fi`)
	writeHostMemorySafetyFakeCommand(t, binDir, "systemctl", `
case "${1:-}" in
  is-active|is-enabled) exit 1 ;;
esac
exit 0
`)
	writeHostMemorySafetyFakeCommand(t, binDir, "modprobe", "exit 0")
	writeHostMemorySafetyFakeCommand(t, binDir, "mkswap", "exit 0")
	writeHostMemorySafetyFakeCommand(t, binDir, "swapon", "exit 0")
	writeHostMemorySafetyFakeCommand(t, binDir, "swapoff", "exit 0")

	meminfo := filepath.Join(tmpDir, "meminfo")
	if err := os.WriteFile(meminfo, []byte(fmt.Sprintf("MemTotal:       %d kB\n", memTotalKiB)), 0o600); err != nil {
		t.Fatalf("write meminfo: %v", err)
	}
	procSwaps := filepath.Join(tmpDir, "swaps")
	if swaps == "" {
		swaps = "Filename\tType\tSize\tUsed\tPriority\n"
	}
	if err := os.WriteFile(procSwaps, []byte(swaps), 0o600); err != nil {
		t.Fatalf("write swaps: %v", err)
	}
	cgroup := filepath.Join(tmpDir, "cgroup.controllers")
	if err := os.WriteFile(cgroup, []byte("cpu memory io\n"), 0o600); err != nil {
		t.Fatalf("write cgroup controllers: %v", err)
	}
	systemdDir := filepath.Join(tmpDir, "systemd")
	if err := os.MkdirAll(systemdDir, 0o755); err != nil {
		t.Fatalf("create systemd runtime dir: %v", err)
	}

	env := map[string]string{
		"PATH":                                binDir + ":" + os.Getenv("PATH"),
		"FUGUE_HOST_ZRAM_MEMINFO":             meminfo,
		"FUGUE_HOST_ZRAM_PROC_SWAPS":          procSwaps,
		"FUGUE_HOST_ZRAM_CGROUP_CONTROLLERS":  cgroup,
		"FUGUE_HOST_ZRAM_SYSTEMD_RUNTIME_DIR": systemdDir,
		"FUGUE_HOST_ZRAM_K3S_VERSION":         k3sVersion,
		"FUGUE_HOST_ZRAM_SYS_BLOCK":           filepath.Join(tmpDir, "sys", "block", "zram0"),
		"FUGUE_HOST_ZRAM_DEVICE":              filepath.Join(tmpDir, "dev", "zram0"),
		"FUGUE_HOST_ZRAM_HELPER":              filepath.Join(tmpDir, "usr", "local", "sbin", "fugue-host-zram"),
		"FUGUE_HOST_ZRAM_ENV_FILE":            filepath.Join(tmpDir, "etc", "fugue", "host-zram.env"),
		"FUGUE_HOST_ZRAM_UNIT_FILE":           filepath.Join(tmpDir, "etc", "systemd", "system", "fugue-host-zram.service"),
		"FUGUE_HOST_ZRAM_HOST_ETC":            filepath.Join(tmpDir, "host-etc"),
		"FUGUE_HOST_ZRAM_HOST_USR_LIB":        filepath.Join(tmpDir, "host-usr-lib"),
	}
	return tmpDir, env
}

func runHostMemorySafetyShell(t *testing.T, env map[string]string, body string) (string, error) {
	t.Helper()
	script := "set -euo pipefail\n" + hostMemorySafetyShellLibrary() + "\n" + body + "\n"
	cmd := exec.Command("bash")
	cmd.Stdin = strings.NewReader(script)
	cmd.Env = os.Environ()
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	output, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(output)), err
}

func TestHostZRAMPlanCompatibilityAndSizing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		memTotalKiB  int64
		k3sVersion   string
		swaps        string
		extraEnv     map[string]string
		wantEligible string
		wantState    string
		wantSize     string
		wantReason   string
	}{
		{
			name:         "eight GiB gets two GiB",
			memTotalKiB:  8 * 1024 * 1024,
			k3sVersion:   "v1.35.4+k3s1",
			wantEligible: "true",
			wantState:    "planned",
			wantSize:     "2147483648",
		},
		{
			name:         "large node is capped at four GiB",
			memTotalKiB:  64 * 1024 * 1024,
			k3sVersion:   "k3s version v1.35.4+k3s1",
			wantEligible: "true",
			wantState:    "planned",
			wantSize:     "4294967296",
		},
		{
			name:         "small node is skipped",
			memTotalKiB:  2 * 1024 * 1024,
			k3sVersion:   "v1.35.4+k3s1",
			wantEligible: "false",
			wantState:    "skipped",
			wantSize:     "0",
			wantReason:   "below the automatic zram threshold",
		},
		{
			name:         "old kubelet is skipped",
			memTotalKiB:  8 * 1024 * 1024,
			k3sVersion:   "v1.33.9+k3s1",
			wantEligible: "false",
			wantState:    "skipped",
			wantSize:     "0",
			wantReason:   "1.34 or newer",
		},
		{
			name:        "foreign swap is not modified",
			memTotalKiB: 8 * 1024 * 1024,
			k3sVersion:  "v1.35.4+k3s1",
			swaps: "Filename\tType\tSize\tUsed\tPriority\n" +
				"/swapfile file 1048572 0 -2\n",
			wantEligible: "false",
			wantState:    "skipped",
			wantSize:     "0",
			wantReason:   "non-Fugue swap",
		},
		{
			name:         "operator can disable automation",
			memTotalKiB:  8 * 1024 * 1024,
			k3sVersion:   "v1.35.4+k3s1",
			extraEnv:     map[string]string{"FUGUE_HOST_ZRAM_MODE": "off"},
			wantEligible: "false",
			wantState:    "disabled",
			wantSize:     "0",
			wantReason:   "disabled by",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, env := hostMemorySafetyHarness(t, tc.memTotalKiB, tc.k3sVersion, tc.swaps)
			for key, value := range tc.extraEnv {
				env[key] = value
			}
			output, err := runHostMemorySafetyShell(t, env, `
if fugue_host_zram_plan; then rc=0; else rc=$?; fi
printf '%s|%s|%s|%s|%s\n' "${rc}" "${FUGUE_HOST_ZRAM_ELIGIBLE}" "${FUGUE_HOST_ZRAM_STATE}" "${FUGUE_HOST_ZRAM_SIZE_BYTES}" "${FUGUE_HOST_ZRAM_REASON}"
`)
			if err != nil {
				t.Fatalf("run plan: %v\n%s", err, output)
			}
			parts := strings.SplitN(output, "|", 5)
			if len(parts) != 5 {
				t.Fatalf("unexpected plan output %q", output)
			}
			if parts[1] != tc.wantEligible || parts[2] != tc.wantState || parts[3] != tc.wantSize {
				t.Fatalf("unexpected plan output %q", output)
			}
			if tc.wantReason != "" && !strings.Contains(parts[4], tc.wantReason) {
				t.Fatalf("expected reason containing %q, got %q", tc.wantReason, parts[4])
			}
		})
	}
}

func TestHostZRAMK3SConfigPatchIsIdempotent(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	initial := "server: \"https://example.invalid\"\nkubelet-arg:\n  - \"system-reserved=memory=1Gi\"\n  - \"fail-swap-on=true\"\nnode-name: \"worker-1\"\n"
	if err := os.WriteFile(configPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	output, err := runHostMemorySafetyShell(t, nil, fmt.Sprintf(`
if fugue_k3s_config_ensure_fail_swap_on_false %q; then first=changed; else first=unchanged; fi
if fugue_k3s_config_ensure_fail_swap_on_false %q; then second=changed; else second=unchanged; fi
printf '%%s|%%s\n' "${first}" "${second}"
`, configPath, configPath))
	if err != nil {
		t.Fatalf("patch k3s config: %v\n%s", err, output)
	}
	if output != "changed|unchanged" {
		t.Fatalf("unexpected idempotence result %q", output)
	}
	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	got := string(content)
	if strings.Count(got, "fail-swap-on=false") != 1 || strings.Contains(got, "fail-swap-on=true") {
		t.Fatalf("unexpected fail-swap-on config:\n%s", got)
	}
	for _, want := range []string{"system-reserved=memory=1Gi", `node-name: "worker-1"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("patched config lost %q:\n%s", want, got)
		}
	}
}

func TestHostZRAMPlanRefusesAnotherZRAMManager(t *testing.T) {
	t.Parallel()

	_, env := hostMemorySafetyHarness(t, 8*1024*1024, "v1.35.4+k3s1", "")
	foreignConfig := filepath.Join(env["FUGUE_HOST_ZRAM_HOST_ETC"], "default", "zramswap")
	if err := os.MkdirAll(filepath.Dir(foreignConfig), 0o755); err != nil {
		t.Fatalf("create foreign config dir: %v", err)
	}
	if err := os.WriteFile(foreignConfig, []byte("ALGO=zstd\n"), 0o600); err != nil {
		t.Fatalf("write foreign zram config: %v", err)
	}
	output, err := runHostMemorySafetyShell(t, env, `
if fugue_host_zram_plan; then rc=0; else rc=$?; fi
printf '%s|%s|%s\n' "${rc}" "${FUGUE_HOST_ZRAM_STATE}" "${FUGUE_HOST_ZRAM_REASON}"
`)
	if err != nil {
		t.Fatalf("run foreign manager plan: %v\n%s", err, output)
	}
	if !strings.HasPrefix(output, "1|skipped|") || !strings.Contains(output, "another zram manager") {
		t.Fatalf("expected foreign zram manager refusal, got %q", output)
	}
}

func TestHostZRAMStageFailureRollsBackManagedFiles(t *testing.T) {
	t.Parallel()

	_, env := hostMemorySafetyHarness(t, 8*1024*1024, "v1.35.4+k3s1", "")
	binDir := strings.Split(env["PATH"], ":")[0]
	writeHostMemorySafetyFakeCommand(t, binDir, "systemctl", `
case "${1:-}" in
  is-active|is-enabled) exit 1 ;;
  daemon-reload) exit 0 ;;
  enable) exit 1 ;;
  disable) exit 0 ;;
esac
exit 0
`)
	output, err := runHostMemorySafetyShell(t, env, `
fugue_host_zram_plan
if fugue_host_zram_stage; then rc=0; else rc=$?; fi
printf '%s|%s|%s\n' "${rc}" "${FUGUE_HOST_ZRAM_STATE}" "${FUGUE_HOST_ZRAM_REASON}"
`)
	if err != nil {
		t.Fatalf("run stage rollback: %v\n%s", err, output)
	}
	if !strings.HasPrefix(output, "1|failed|") {
		t.Fatalf("expected failed stage, got %q", output)
	}
	for _, key := range []string{"FUGUE_HOST_ZRAM_HELPER", "FUGUE_HOST_ZRAM_ENV_FILE", "FUGUE_HOST_ZRAM_UNIT_FILE"} {
		if _, statErr := os.Stat(env[key]); !os.IsNotExist(statErr) {
			t.Fatalf("expected %s to be rolled back, stat error=%v", env[key], statErr)
		}
	}
}

func TestHostZRAMActivationVerificationFailureRollsBackManagedFiles(t *testing.T) {
	t.Parallel()

	tmpDir, env := hostMemorySafetyHarness(t, 8*1024*1024, "v1.35.4+k3s1", "")
	binDir := strings.Split(env["PATH"], ":")[0]
	serviceState := filepath.Join(tmpDir, "service-active")
	env["FUGUE_TEST_SERVICE_STATE"] = serviceState
	writeHostMemorySafetyFakeCommand(t, binDir, "systemctl", `
case "${1:-}" in
  is-active) if [ -e "${FUGUE_TEST_SERVICE_STATE}" ]; then exit 0; else exit 1; fi ;;
  is-enabled) exit 1 ;;
  daemon-reload|enable|disable) exit 0 ;;
  start|restart) touch "${FUGUE_TEST_SERVICE_STATE}"; exit 0 ;;
  stop) rm -f "${FUGUE_TEST_SERVICE_STATE}"; exit 0 ;;
esac
exit 0
`)
	output, err := runHostMemorySafetyShell(t, env, `
fugue_host_zram_plan
fugue_host_zram_stage
if fugue_host_zram_activate; then rc=0; else rc=$?; fi
printf '%s|%s|%s\n' "${rc}" "${FUGUE_HOST_ZRAM_STATE}" "${FUGUE_HOST_ZRAM_REASON}"
`)
	if err != nil {
		t.Fatalf("run activation rollback: %v\n%s", err, output)
	}
	if !strings.HasPrefix(output, "1|failed|") || !strings.Contains(output, "did not become active") {
		t.Fatalf("expected failed activation verification, got %q", output)
	}
	for _, key := range []string{"FUGUE_HOST_ZRAM_HELPER", "FUGUE_HOST_ZRAM_ENV_FILE", "FUGUE_HOST_ZRAM_UNIT_FILE"} {
		if _, statErr := os.Stat(env[key]); !os.IsNotExist(statErr) {
			t.Fatalf("expected %s to be rolled back, stat error=%v", env[key], statErr)
		}
	}
}

func TestHostMemorySafetyShellHasValidBashSyntax(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	cmd := exec.Command("bash", "-n")
	cmd.Stdin = strings.NewReader(hostMemorySafetyShellLibrary())
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bash -n host memory safety library: %v\n%s", err, output)
	}
}

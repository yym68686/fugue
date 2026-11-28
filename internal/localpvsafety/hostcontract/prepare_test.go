package hostcontract

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const prepareScript = "../../../scripts/prepare_fugue_lvm_localpv_node.sh"

func TestPrepareLocalPVNodeRolePolicyDryRun(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	scriptPath := prepareScript
	for _, tt := range []struct {
		name     string
		args     []string
		wantOK   bool
		wantText string
		envRoles string
	}{
		{
			name:     "storage agent allowed",
			args:     []string{"--size-gib", "1", "--node-role", "storage-agent", "--dry-run"},
			wantOK:   true,
			wantText: "LocalPV preallocation dry-run",
		},
		{
			name:     "edge refused",
			args:     []string{"--size-gib", "1", "--node-role", "edge", "--dry-run"},
			wantText: "disabled for edge, DNS, and control-plane-only roles",
		},
		{
			name:     "control plane refused",
			args:     []string{"--size-gib", "1", "--node-role", "control-plane-only", "--dry-run"},
			wantText: "disabled for edge, DNS, and control-plane-only roles",
		},
		{
			name:     "env dns refused",
			args:     []string{"--size-gib", "1", "--dry-run"},
			envRoles: "dns",
			wantText: "disabled for edge, DNS, and control-plane-only roles",
		},
		{
			name:     "explicit maintenance override allowed",
			args:     []string{"--size-gib", "1", "--node-role", "edge", "--allow-localpv", "--dry-run"},
			wantOK:   true,
			wantText: "allow_localpv=true",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cmd := exec.Command("bash", append([]string{scriptPath}, tt.args...)...)
			if tt.envRoles != "" {
				cmd.Env = append(os.Environ(), "FUGUE_NODE_ROLES="+tt.envRoles)
			}
			output, err := cmd.CombinedOutput()
			if tt.wantOK && err != nil {
				t.Fatalf("expected success, got %v\n%s", err, output)
			}
			if !tt.wantOK && err == nil {
				t.Fatalf("expected failure, got success\n%s", output)
			}
			if !strings.Contains(string(output), tt.wantText) {
				t.Fatalf("expected output to contain %q, got\n%s", tt.wantText, output)
			}
		})
	}
}

func TestPrepareLocalPVLoopServiceChecksAssociationOutput(t *testing.T) {
	t.Parallel()

	scriptPath := prepareScript
	raw, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read LocalPV preparation script: %v", err)
	}
	script := string(raw)
	want := `python3 "${LOCALPV_UNIT_RENDERER}" render "${IMAGE_PATH}" "${VG_NAME}"`
	if !strings.Contains(script, want) {
		t.Fatalf("LocalPV loop service must attach when losetup -j returns success with empty output; missing %q", want)
	}
	if strings.Contains(script, "losetup -j ${IMAGE_PATH} >/dev/null || losetup --find --show ${IMAGE_PATH}") {
		t.Fatal("LocalPV loop service must not use losetup -j exit status as the association test")
	}
}

func TestPrepareLocalPVNodeRecoversExistingBackingMetadataWithoutReinitializingIt(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	scriptPath := prepareScript
	raw, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("read LocalPV preparation script: %v", err)
	}
	harness := string(raw)
	rootGuard := `if [[ "${EUID}" -ne 0 ]]; then
  echo "run as root" >&2
  exit 1
fi`
	if !strings.Contains(harness, rootGuard) {
		t.Fatal("LocalPV preparation script root guard changed; update executable harness deliberately")
	}
	harness = strings.Replace(harness, rootGuard, ":", 1)
	const servicePath = `SERVICE_PATH="/etc/systemd/system/fugue-lvm-localpv-loop.service"`
	if !strings.Contains(harness, servicePath) {
		t.Fatal("LocalPV preparation service path changed; update executable harness deliberately")
	}
	harness = strings.Replace(harness, servicePath, `SERVICE_PATH="${TEST_SERVICE_PATH}"`, 1)

	tests := []struct {
		name             string
		imageExists      bool
		vgOnline         bool
		loopAssociated   bool
		pvPresent        bool
		pvVG             string
		wantOK           bool
		wantOutput       string
		wantActions      []string
		forbiddenActions []string
	}{
		{
			name:        "existing backing PV belongs to requested VG",
			imageExists: true,
			pvPresent:   true,
			pvVG:        "fugue-vg",
			wantOK:      true,
			wantActions: []string{"losetup --find --show", "pvs /dev/loop-test", "pvs --noheadings -o vg_name /dev/loop-test", "systemctl enable --now fugue-lvm-localpv-loop.service"},
			forbiddenActions: []string{
				"pvcreate ",
				"vgcreate ",
				"losetup -d ",
			},
		},
		{
			name:        "existing backing PV belongs to another VG",
			imageExists: true,
			pvPresent:   true,
			pvVG:        "other-vg",
			wantOutput:  "belongs to volume group other-vg, expected fugue-vg",
			wantActions: []string{"losetup --find --show", "pvs /dev/loop-test", "pvs --noheadings -o vg_name /dev/loop-test", "losetup -d /dev/loop-test"},
			forbiddenActions: []string{
				"pvcreate ",
				"vgcreate ",
				"systemctl ",
			},
		},
		{
			name:        "new empty image is initialized",
			pvPresent:   false,
			wantOK:      true,
			wantActions: []string{"fallocate -l 1G", "losetup --find --show", "pvs /dev/loop-test", "pvcreate -ff -y /dev/loop-test", "vgcreate fugue-vg /dev/loop-test", "systemctl enable --now fugue-lvm-localpv-loop.service"},
		},
		{
			name:        "existing non-PV image is never overwritten",
			imageExists: true,
			pvPresent:   false,
			wantOutput:  "is not an LVM physical volume; refusing to initialize or overwrite it",
			wantActions: []string{"losetup --find --show", "pvs /dev/loop-test", "losetup -d /dev/loop-test"},
			forbiddenActions: []string{
				"pvcreate ",
				"vgcreate ",
				"systemctl ",
			},
		},
		{
			name:             "online VG is never rebuilt",
			imageExists:      true,
			vgOnline:         true,
			loopAssociated:   true,
			wantOK:           true,
			wantActions:      []string{"vgs fugue-vg", "systemctl enable --now fugue-lvm-localpv-loop.service"},
			forbiddenActions: []string{"fallocate ", "losetup ", "pvs ", "pvcreate ", "vgcreate "},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			binDir := filepath.Join(tmpDir, "bin")
			if err := os.MkdirAll(binDir, 0o755); err != nil {
				t.Fatalf("create fake command directory: %v", err)
			}
			actionsPath := filepath.Join(tmpDir, "actions.log")
			imagePath := filepath.Join(tmpDir, "fugue-vg.img")
			serviceFile := filepath.Join(tmpDir, "fugue-lvm-localpv-loop.service")
			if tt.imageExists {
				if err := os.WriteFile(imagePath, []byte("existing LVM metadata"), 0o600); err != nil {
					t.Fatalf("write existing LocalPV image: %v", err)
				}
			}

			writeFakeCommand(t, binDir, "vgs", `#!/bin/sh
printf '%s\n' "vgs $*" >>"${TEST_ACTIONS}"
if [ "${1:-}" = "fugue-vg" ]; then
  [ "${TEST_VG_ONLINE:-false}" = "true" ]
  exit $?
fi
printf '%s\n' 'fugue-vg 1073741824 1073741824'
`)
			writeFakeCommand(t, binDir, "pvs", `#!/bin/sh
printf '%s\n' "pvs $*" >>"${TEST_ACTIONS}"
if [ "${1:-}" = "--noheadings" ]; then
  printf '%s\n' "${TEST_PV_VG:-}"
  exit 0
fi
[ "${TEST_PV_PRESENT:-false}" = "true" ]
`)
			writeFakeCommand(t, binDir, "losetup", `#!/bin/sh
printf '%s\n' "losetup $*" >>"${TEST_ACTIONS}"
case "${1:-}" in
  -j)
    if [ "${TEST_LOOP_ASSOCIATED:-false}" = "true" ]; then
      printf '/dev/loop-test: []: (%s)\n' "${2:-}"
    fi
    ;;
  --find)
    printf '%s\n' /dev/loop-test
    ;;
  -d)
    ;;
  *)
    exit 1
    ;;
esac
`)
			writeFakeCommand(t, binDir, "fallocate", `#!/bin/sh
printf '%s\n' "fallocate $*" >>"${TEST_ACTIONS}"
target=''
for arg in "$@"; do
  target="${arg}"
done
: >"${target}"
`)
			for _, name := range []string{"pvcreate", "vgcreate", "systemctl"} {
				writeFakeCommand(t, binDir, name, `#!/bin/sh
printf '%s\n' "$(basename "$0") $*" >>"${TEST_ACTIONS}"
`)
			}

			// Recreate the installation tree so this exercises the real script's
			// default renderer lookup, rather than an override for the test.
			scriptsDir := filepath.Join(tmpDir, "scripts")
			if err := os.Mkdir(scriptsDir, 0o700); err != nil {
				t.Fatal(err)
			}
			installInternal, err := filepath.Abs("../..")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(installInternal, filepath.Join(tmpDir, "internal")); err != nil {
				t.Fatal(err)
			}
			testScript := filepath.Join(scriptsDir, "prepare-localpv-test.sh")
			if err := os.WriteFile(testScript, []byte(harness), 0o700); err != nil {
				t.Fatalf("write LocalPV preparation harness: %v", err)
			}
			cmd := exec.Command("bash", testScript, "--size-gib", "1", "--node-role", "storage-agent", "--image-path", imagePath)
			cmd.Env = append(os.Environ(),
				"PATH="+binDir+":"+os.Getenv("PATH"),
				"TEST_ACTIONS="+actionsPath,
				"TEST_SERVICE_PATH="+serviceFile,
				"TEST_VG_ONLINE="+fmt.Sprintf("%t", tt.vgOnline),
				"TEST_LOOP_ASSOCIATED="+fmt.Sprintf("%t", tt.loopAssociated),
				"TEST_PV_PRESENT="+fmt.Sprintf("%t", tt.pvPresent),
				"TEST_PV_VG="+tt.pvVG,
				"FUGUE_NODE_ROLES=",
				"FUGUE_LOCALPV_UNIT_RENDERER=",
			)
			output, runErr := cmd.CombinedOutput()
			if tt.wantOK && runErr != nil {
				t.Fatalf("LocalPV preparation harness failed: %v\n%s", runErr, output)
			}
			if !tt.wantOK && runErr == nil {
				t.Fatalf("LocalPV preparation harness unexpectedly succeeded\n%s", output)
			}
			if tt.wantOutput != "" && !strings.Contains(string(output), tt.wantOutput) {
				t.Fatalf("LocalPV preparation output missing %q:\n%s", tt.wantOutput, output)
			}
			actionsRaw, err := os.ReadFile(actionsPath)
			if err != nil {
				t.Fatalf("read fake command actions: %v", err)
			}
			actions := string(actionsRaw)
			for _, want := range tt.wantActions {
				if !strings.Contains(actions, want) {
					t.Fatalf("LocalPV actions missing %q:\n%s", want, actions)
				}
			}
			for _, forbidden := range tt.forbiddenActions {
				for _, action := range strings.Split(actions, "\n") {
					if strings.HasPrefix(action, forbidden) {
						t.Fatalf("LocalPV actions unexpectedly include %q:\n%s", action, actions)
					}
				}
			}
			if tt.wantOK {
				serviceRaw, err := os.ReadFile(serviceFile)
				if err != nil {
					t.Fatalf("read generated LocalPV service: %v", err)
				}
				if !strings.Contains(string(serviceRaw), `association=$$(losetup -j "$$image"); if [ -z "$$association" ]`) {
					t.Fatalf("generated LocalPV service does not test association output:\n%s", serviceRaw)
				}
			}
		})
	}
}

func writeFakeCommand(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o700); err != nil {
		t.Fatalf("write fake %s command: %v", name, err)
	}
}

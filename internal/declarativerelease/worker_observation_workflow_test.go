package declarativerelease

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Exercise the actual shell in the composite action. No Kubernetes, network,
// registry, or actual Go build: prepare's exit status is the injected input.
func TestWorkerObservationCannotTurnFailedPrepareIntoRelease(t *testing.T) {
	raw, err := os.ReadFile("../../.github/actions/deploy-declarative-component/action.yml")
	if err != nil {
		t.Fatal(err)
	}
	var action struct {
		Runs struct {
			Steps []struct{ ID, If, Run, Name, Uses string }
		}
	}
	if err := yaml.Unmarshal(raw, &action); err != nil {
		t.Fatal(err)
	}
	var prepare, observed, uploaded bool
	for _, step := range action.Runs.Steps {
		switch {
		case step.ID == "prepare":
			prepare = true
			for _, rc := range []string{"0", "1", "78"} {
				dir := t.TempDir()
				write := func(name, body string) string {
					p := filepath.Join(dir, name)
					if err := os.WriteFile(p, []byte(body), 0700); err != nil {
						t.Fatal(err)
					}
					return p
				}
				write("go", "#!/bin/sh\nexit 0\n")
				release := write("release", "#!/bin/sh\nexit \"$RC\"\n")
				out := write("outputs", "")
				cmd := exec.Command("bash", "-c", step.Run)
				cmd.Env = []string{"PATH=" + dir + ":" + os.Getenv("PATH"), "RELEASE_TOOL=" + release, "PLAN=p", "RECEIPT=a", "COMPONENT=c", "EXECUTION=e", "GITHUB_OUTPUT=" + out, "RC=" + rc}
				msg, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if (rc == "0") != (err == nil) || (rc == "1" || rc == "78") && (!errors.As(err, &exit) || exit.ExitCode() != map[string]int{"1": 1, "78": 78}[rc]) {
					t.Fatalf("prepare %s: %v %s", rc, err, msg)
				}
				v, err := os.ReadFile(out)
				if err != nil || (rc == "78" && string(v) != "mixed_activation=true\n") || (rc != "78" && len(v) != 0) {
					t.Fatalf("wrong diagnostic routing for %s: %q %v", rc, v, err)
				}
			}
		case step.ID == "mixed_members":
			observed = prepare && step.If == "failure() && steps.prepare.outcome == 'failure' && steps.prepare.outputs.mixed_activation == 'true'" &&
				strings.Count(step.Run, "worker-front-preflight") == 1 && !strings.Contains(step.Run, "guardian-submit")
		case step.Name == "Preserve the read-only mixed Worker observation":
			uploaded = observed && step.If == "failure() && steps.mixed_members.outcome == 'success'" && strings.HasPrefix(step.Uses, "actions/upload-artifact@")
		}
	}
	if !prepare || !observed || !uploaded {
		t.Fatal("mixed-member observation is not bound to a failed prewrite")
	}
}

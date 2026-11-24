package declarativerelease

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestWorkerWaitShellPreservesReceiptAndUnsuccessfulJob(t *testing.T) {
	raw, err := os.ReadFile("../../.github/actions/deploy-declarative-component/action.yml")
	if err != nil {
		t.Fatal(err)
	}
	var action struct {
		Runs struct {
			Steps []struct{ ID, If, Run, Name string }
		}
	}
	if err := yaml.Unmarshal(raw, &action); err != nil {
		t.Fatal(err)
	}
	var submit string
	for _, step := range action.Runs.Steps {
		if step.ID == "execute_guardian" {
			submit = step.Run
		}
	}
	if submit == "" {
		t.Fatal("missing submission step")
	}
	a, b := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	for _, tc := range []string{"wait", "stale-plan", "empty", "double", "different-component", "committed", "missing-receipt"} {
		t.Run(tc, func(t *testing.T) {
			dir := t.TempDir()
			write := func(name, body string) string {
				p := filepath.Join(dir, name)
				if err := os.WriteFile(p, []byte(body), 0700); err != nil {
					t.Fatal(err)
				}
				return p
			}
			write("execution-plan.json", `{"component":"worker","planDigest":"`+a+`"}`)
			out := write("outputs", "")
			release := write("release", "#!/bin/sh\ncat \"$CASE_INPUT\"\nexit 1\n")
			status := map[string]string{"component": "worker", "state": "rollout_pending", "workerDrainPlanDigest": a, "rolloutReceiptDigest": b, "currentRecordDigest": a, "lastSuccessfulLkg": a, "targetRecordDigest": b}
			switch tc {
			case "stale-plan":
				status["workerDrainPlanDigest"] = b
			case "committed":
				status["currentRecordDigest"] = b
			case "different-component":
				status["component"] = "other"
			case "missing-receipt":
				delete(status, "rolloutReceiptDigest")
			}
			raw, err := json.Marshal(status)
			if err != nil {
				t.Fatal(err)
			}
			payload := string(raw)
			if tc == "empty" {
				payload = ""
			}
			if tc == "double" {
				payload += "\n" + payload
			}
			in := write("in", payload)
			result := filepath.Join(dir, "result")
			cmd := exec.Command("bash", "-c", submit)
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "RELEASE_TOOL=" + release, "RESULT=" + result, "EXECUTION=" + dir, "GITHUB_OUTPUT=" + out, "CASE_INPUT=" + in}
			msg, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 {
				t.Fatalf("submit outcome=%v %s", err, msg)
			}
			flag, err := os.ReadFile(out)
			if err != nil || (string(flag) == "worker_drain_wait=true\n") != (tc == "wait") {
				t.Fatalf("unexpected classification %q, %v", flag, err)
			}
			artifact, err := os.ReadFile(result)
			if err != nil || string(artifact) != payload {
				t.Fatal("original receipt was lost")
			}
		})
	}
}

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

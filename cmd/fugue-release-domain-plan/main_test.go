package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"fugue/internal/releasedomain"
)

func rootPath(parts ...string) string {
	return filepath.Join(append([]string{"..", ".."}, parts...)...)
}

func baseCLIArgs(changed, base, target, repeated string) []string {
	args := []string{
		"--ownership", rootPath("deploy", "release-domains", "ownership-v1.yaml"),
		"--changed-files", rootPath("internal", "releasedomain", "testdata", changed),
		"--base-manifest", rootPath("internal", "releasedomain", "testdata", base),
		"--target-manifest", rootPath("internal", "releasedomain", "testdata", target),
		"--repeated-target-manifest", rootPath("internal", "releasedomain", "testdata", repeated),
		"--base-digest", "opaque-base",
		"--target-digest", "opaque-target",
		"--live-digest", "opaque-base",
		"--namespace", "fugue-system",
	}
	bindings := []string{
		"nodeLocalNamespace=kube-system",
		"nodeLocalName=fugue-node-local-dns",
		"nodeLocalUpstreamServiceName=fugue-dns-upstream",
		"nodeLocalActiveName=fugue-node-local-dns-active",
		"dnsName=fugue-dns",
		"apiName=fugue-api",
		"controllerName=fugue-controller",
		"serviceName=fugue",
		"ingressName=fugue",
		"imageCacheName=fugue-image-cache",
		"controlPlanePostgresName=fugue-control-plane-postgres",
		"controlPlanePostgresSecretName=fugue-control-plane-postgres-app",
		"controlPlaneRestoreDrillName=fugue-control-plane-restore-drill",
	}
	for _, binding := range bindings {
		args = append(args, "--binding", binding)
	}
	return args
}

func runPlan(t *testing.T, args []string) (int, releasedomain.Plan, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	exitCode := run(args, &stdout, &stderr)
	var plan releasedomain.Plan
	if stdout.Len() > 0 {
		if err := json.Unmarshal(stdout.Bytes(), &plan); err != nil {
			t.Fatalf("decode plan: %v\n%s", err, stdout.String())
		}
	}
	return exitCode, plan, stderr.String()
}

func TestRunSingleNodeLocalPlan(t *testing.T) {
	args := baseCLIArgs(
		filepath.Join("single-node-local", "changed-files.json"),
		filepath.Join("single-node-local", "base.yaml"),
		filepath.Join("single-node-local", "target.yaml"),
		filepath.Join("single-node-local", "target.yaml"),
	)
	exitCode, plan, stderr := runPlan(t, args)
	if exitCode != 0 || stderr != "" {
		t.Fatalf("exit=%d stderr=%q", exitCode, stderr)
	}
	if plan.Result != releasedomain.OutcomeSingle || plan.SelectedDomain != releasedomain.DomainNodeLocal {
		t.Fatalf("plan = %#v", plan)
	}
	if err := releasedomain.VerifyPlanDigest(plan); err != nil {
		t.Fatalf("plan digest: %v", err)
	}
	if err := releasedomain.VerifyClassificationContextEvidence(plan.Digests.ClassificationContext); err != nil {
		t.Fatalf("classification context: %v", err)
	}
	context := plan.Digests.ClassificationContext
	if context.DefaultNamespace != "fugue-system" || context.IgnoreHelmTestHooks {
		t.Fatalf("classification context = %#v", context)
	}
	if got := context.BindingMap()["releaseNamespace"]; got != "fugue-system" {
		t.Fatalf("releaseNamespace binding = %q", got)
	}
}

func TestRunMultipleIsBlockedWithExitTwo(t *testing.T) {
	args := baseCLIArgs(
		filepath.Join("multiple", "changed-files.json"),
		filepath.Join("multiple", "base.yaml"),
		filepath.Join("multiple", "target.yaml"),
		filepath.Join("multiple", "target.yaml"),
	)
	exitCode, plan, _ := runPlan(t, args)
	if exitCode != 2 || plan.Result != releasedomain.OutcomeMultiple {
		t.Fatalf("exit=%d plan=%#v", exitCode, plan)
	}
}

func TestRunLookupDriftIsUnknown(t *testing.T) {
	args := baseCLIArgs(
		filepath.Join("lookup-drift", "changed-files.json"),
		filepath.Join("lookup-drift", "base.yaml"),
		filepath.Join("lookup-drift", "target-a.yaml"),
		filepath.Join("lookup-drift", "target-b.yaml"),
	)
	exitCode, plan, _ := runPlan(t, args)
	if exitCode != 2 || plan.Result != releasedomain.OutcomeUnknown {
		t.Fatalf("exit=%d plan=%#v", exitCode, plan)
	}
}

func TestRunRejectsIncompleteInvocation(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if exitCode := run([]string{"--base-digest", "x"}, &stdout, &stderr); exitCode != 1 {
		t.Fatalf("exit=%d stderr=%q", exitCode, stderr.String())
	}
}

func TestBuiltBinaryExitContract(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "fugue-release-domain-plan")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build planner binary: %v\n%s", err, output)
	}

	successOutput := filepath.Join(t.TempDir(), "success-plan.json")
	successArgs := append(baseCLIArgs(
		filepath.Join("single-node-local", "changed-files.json"),
		filepath.Join("single-node-local", "base.yaml"),
		filepath.Join("single-node-local", "target.yaml"),
		filepath.Join("single-node-local", "target.yaml"),
	), "--output", successOutput)
	if exitCode, _, stderr := executeBinary(t, binary, successArgs); exitCode != 0 || stderr != "" {
		t.Fatalf("successful binary exit=%d stderr=%q", exitCode, stderr)
	}
	if plan := readPlanFile(t, successOutput); plan.Result != releasedomain.OutcomeSingle {
		t.Fatalf("successful plan result = %s", plan.Result)
	}

	if exitCode, _, stderr := executeBinary(t, binary, []string{"--base-digest", "incomplete"}); exitCode != 1 || stderr == "" {
		t.Fatalf("invalid binary exit=%d stderr=%q", exitCode, stderr)
	}

	for _, invalidArgs := range [][]string{
		append(baseCLIArgs(
			filepath.Join("single-node-local", "changed-files.json"),
			filepath.Join("single-node-local", "base.yaml"),
			filepath.Join("single-node-local", "target.yaml"),
			filepath.Join("single-node-local", "target.yaml"),
		), "--binding", "extra=bad-\xff"),
		append(baseCLIArgs(
			filepath.Join("single-node-local", "changed-files.json"),
			filepath.Join("single-node-local", "base.yaml"),
			filepath.Join("single-node-local", "target.yaml"),
			filepath.Join("single-node-local", "target.yaml"),
		), "--base-digest", "bad-\xff"),
	} {
		if exitCode, stdout, stderr := executeBinary(t, binary, invalidArgs); exitCode != 1 || stdout != "" || stderr == "" {
			t.Fatalf("invalid UTF-8 binary exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
		}
	}

	blockedOutput := filepath.Join(t.TempDir(), "blocked-plan.json")
	blockedArgs := append(baseCLIArgs(
		filepath.Join("multiple", "changed-files.json"),
		filepath.Join("multiple", "base.yaml"),
		filepath.Join("multiple", "target.yaml"),
		filepath.Join("multiple", "target.yaml"),
	), "--output", blockedOutput)
	if exitCode, stdout, stderr := executeBinary(t, binary, blockedArgs); exitCode != 2 || stdout != "" || stderr != "" {
		t.Fatalf("blocked binary exit=%d stdout=%q stderr=%q", exitCode, stdout, stderr)
	}
	blockedPlan := readPlanFile(t, blockedOutput)
	if blockedPlan.Result != releasedomain.OutcomeMultiple {
		t.Fatalf("blocked plan was not persisted before exit 2: %#v", blockedPlan)
	}
	if err := releasedomain.VerifyPlanDigest(blockedPlan); err != nil {
		t.Fatalf("blocked persisted plan digest: %v", err)
	}
}

func executeBinary(t *testing.T, binary string, args []string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	command := exec.Command(binary, args...)
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if err == nil {
		return 0, stdout.String(), stderr.String()
	}
	exitError, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("execute planner binary: %v", err)
	}
	return exitError.ExitCode(), stdout.String(), stderr.String()
}

func readPlanFile(t *testing.T, path string) releasedomain.Plan {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read persisted plan: %v", err)
	}
	var plan releasedomain.Plan
	if err := json.Unmarshal(encoded, &plan); err != nil {
		t.Fatalf("decode persisted plan: %v", err)
	}
	return plan
}

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
		"releaseName=fugue",
		"nodeLocalNamespace=kube-system",
		"nodeLocalName=fugue-node-local-dns",
		"nodeLocalUpstreamServiceName=fugue-dns-upstream",
		"nodeLocalActiveName=fugue-node-local-dns-active",
		"dnsName=fugue-dns",
		"apiName=fugue-api",
		"controllerName=fugue-controller",
		"telemetryAgentName=fugue-telemetry-agent",
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
	if got := context.BindingMap()["releaseName"]; got != "fugue" {
		t.Fatalf("releaseName binding = %q", got)
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

func TestRunMissingReleaseNameBindingFailsClosed(t *testing.T) {
	args := baseCLIArgs(
		filepath.Join("single-node-local", "changed-files.json"),
		filepath.Join("single-node-local", "base.yaml"),
		filepath.Join("single-node-local", "target.yaml"),
		filepath.Join("single-node-local", "target.yaml"),
	)
	filtered := make([]string, 0, len(args)-2)
	for index := 0; index < len(args); index++ {
		if args[index] == "--binding" && index+1 < len(args) && args[index+1] == "releaseName=fugue" {
			index++
			continue
		}
		filtered = append(filtered, args[index])
	}
	exitCode, plan, stderr := runPlan(t, filtered)
	if exitCode != 2 || stderr != "" || plan.Result != releasedomain.OutcomeUnknown || plan.SingleDomainDispatchAllowed() {
		t.Fatalf("exit=%d stderr=%q plan=%#v", exitCode, stderr, plan)
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

func TestRunOutputCannotAliasAnyActualInput(t *testing.T) {
	for _, inputFlag := range []string{
		"--ownership",
		"--changed-files",
		"--base-manifest",
		"--target-manifest",
		"--repeated-target-manifest",
	} {
		t.Run(strings.TrimPrefix(inputFlag, "--"), func(t *testing.T) {
			args, inputs := privatePlannerCLIArgs(t)
			inputPath := inputs[inputFlag]
			before, err := os.ReadFile(inputPath)
			if err != nil {
				t.Fatal(err)
			}

			exitCode, plan, stderr := runPlan(t, append(args, "--output", inputPath))
			if exitCode != 1 || plan.Result != "" || stderr != planPublicationMessage {
				t.Fatalf("exit=%d plan=%#v stderr=%q", exitCode, plan, stderr)
			}
			after, err := os.ReadFile(inputPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, before) {
				t.Fatalf("%s input was changed by aliased output", inputFlag)
			}
			assertNoAtomicOutputTemps(t, filepath.Dir(inputPath))
		})
	}
}

func TestRunFailuresDoNotEchoPrivateInputSentinels(t *testing.T) {
	const sentinel = "PRIVATE-PLANNER-SENTINEL-6f29cb"
	validArgs := baseCLIArgs(
		filepath.Join("single-node-local", "changed-files.json"),
		filepath.Join("single-node-local", "base.yaml"),
		filepath.Join("single-node-local", "target.yaml"),
		filepath.Join("single-node-local", "target.yaml"),
	)
	missingPath := filepath.Join(t.TempDir(), sentinel)
	unsafeOutput := filepath.Join(t.TempDir(), sentinel)
	if err := os.WriteFile(unsafeOutput, []byte("existing-output"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unsafeOutput, 0o644); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		args     []string
		expected string
	}{
		{name: "ownership", args: replaceCLIFlagValue(validArgs, "--ownership", missingPath), expected: planConstructionMessage},
		{name: "changed files", args: replaceCLIFlagValue(validArgs, "--changed-files", missingPath), expected: planConstructionMessage},
		{name: "base manifest", args: replaceCLIFlagValue(validArgs, "--base-manifest", missingPath), expected: planConstructionMessage},
		{name: "target manifest", args: replaceCLIFlagValue(validArgs, "--target-manifest", missingPath), expected: planConstructionMessage},
		{name: "repeated target manifest", args: replaceCLIFlagValue(validArgs, "--repeated-target-manifest", missingPath), expected: planConstructionMessage},
		{name: "output", args: append(append([]string(nil), validArgs...), "--output", unsafeOutput), expected: planPublicationMessage},
		{name: "flag value", args: append(append([]string(nil), validArgs...), "--binding", sentinel), expected: invalidInvocationMessage},
		{name: "flag name", args: []string{"--" + sentinel}, expected: invalidInvocationMessage},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			exitCode := run(test.args, &stdout, &stderr)
			if exitCode != 1 || stdout.Len() != 0 || stderr.String() != test.expected {
				t.Fatalf("exit=%d stdout=%q stderr=%q", exitCode, stdout.String(), stderr.String())
			}
			if strings.Contains(stdout.String(), sentinel) || strings.Contains(stderr.String(), sentinel) {
				t.Fatalf("private sentinel crossed CLI boundary: stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
}

func replaceCLIFlagValue(args []string, flagName, value string) []string {
	replaced := append([]string(nil), args...)
	for index := 0; index+1 < len(replaced); index++ {
		if replaced[index] == flagName {
			replaced[index+1] = value
			return replaced
		}
	}
	return replaced
}

func privatePlannerCLIArgs(t *testing.T) ([]string, map[string]string) {
	t.Helper()
	directory := t.TempDir()
	sources := map[string]string{
		"--ownership":                rootPath("deploy", "release-domains", "ownership-v1.yaml"),
		"--changed-files":            rootPath("internal", "releasedomain", "testdata", "single-node-local", "changed-files.json"),
		"--base-manifest":            rootPath("internal", "releasedomain", "testdata", "single-node-local", "base.yaml"),
		"--target-manifest":          rootPath("internal", "releasedomain", "testdata", "single-node-local", "target.yaml"),
		"--repeated-target-manifest": rootPath("internal", "releasedomain", "testdata", "single-node-local", "target.yaml"),
	}
	inputs := make(map[string]string, len(sources))
	for flagName, source := range sources {
		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatalf("read %s fixture: %v", flagName, err)
		}
		destination := filepath.Join(directory, strings.TrimPrefix(flagName, "--"))
		if err := os.WriteFile(destination, data, 0o600); err != nil {
			t.Fatalf("write %s fixture: %v", flagName, err)
		}
		inputs[flagName] = destination
	}

	args := baseCLIArgs(
		filepath.Join("single-node-local", "changed-files.json"),
		filepath.Join("single-node-local", "base.yaml"),
		filepath.Join("single-node-local", "target.yaml"),
		filepath.Join("single-node-local", "target.yaml"),
	)
	for index := 0; index+1 < len(args); index++ {
		if replacement, ok := inputs[args[index]]; ok {
			args[index+1] = replacement
			index++
		}
	}
	return args, inputs
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

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const canonicalTestOwnership = `apiVersion: release-domain.fugue.dev/v1
kind: ReleaseDomainOwnership
domains: [node-local, authoritative-dns, control-plane, image-cache, backup]
requiredBindings: []
fileRules: []
valueRules: []
objectRules: []
`

func TestRunCanonicalizeManifestWritesPrivateDeterministicOutput(t *testing.T) {
	directory := t.TempDir()
	ownership := writeCanonicalTestFile(t, directory, "ownership.yaml", canonicalTestOwnership, 0o644)
	input := writeCanonicalTestFile(t, directory, "input.yaml", `kind: ConfigMap
apiVersion: v1
metadata:
  name: fixture
  creationTimestamp: null
data: {secret: sentinel-private-render, number: "1"}
status: {ignored: true}
`, 0o600)
	output := filepath.Join(directory, "canonical.yaml")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := runCanonicalizeManifest([]string{
		"--ownership", ownership,
		"--input", input,
		"--namespace", "fugue-system",
		"--output", output,
	}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("exit=%d stderr=%s", exitCode, stderr.String())
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("private render leaked output: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	canonical, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(canonical, []byte("namespace: fugue-system")) ||
		!bytes.Contains(canonical, []byte("sentinel-private-render")) {
		t.Fatalf("canonical output lost manifest data:\n%s", canonical)
	}
	if bytes.Contains(canonical, []byte("creationTimestamp")) || bytes.Contains(canonical, []byte("status:")) {
		t.Fatalf("canonical output retained normalized fields:\n%s", canonical)
	}
	info, err := os.Stat(output)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("output mode=%o, want 600", info.Mode().Perm())
	}

	repeated := filepath.Join(directory, "repeated.yaml")
	exitCode = runCanonicalizeManifest([]string{
		"--ownership", ownership,
		"--input", input,
		"--namespace", "fugue-system",
		"--output", repeated,
	}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("repeated exit=%d stderr=%s", exitCode, stderr.String())
	}
	repeatedData, err := os.ReadFile(repeated)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonical, repeatedData) {
		t.Fatalf("canonical output drifted:\n%s\n%s", canonical, repeatedData)
	}
}

func TestRunCanonicalizeManifestExtractsHelmJSON(t *testing.T) {
	directory := t.TempDir()
	ownership := writeCanonicalTestFile(t, directory, "ownership.yaml", canonicalTestOwnership, 0o644)
	payload, err := json.Marshal(map[string]any{
		"apply_method": "server_side",
		"chart":        map[string]any{},
		"hooks": []any{map[string]any{
			"events": []string{"pre-upgrade"}, "kind": "Secret", "last_run": map[string]any{},
			"manifest": "apiVersion: v1\nkind: Secret\nmetadata: {name: hook}\n", "name": "hook", "path": "templates/hook.yaml",
		}},
		"info":      map[string]any{"last_deployed": "volatile"},
		"manifest":  "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: main}\n",
		"name":      "fugue",
		"namespace": "fugue-system",
		"version":   18,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := writeCanonicalTestFile(t, directory, "helm.json", string(payload), 0o600)
	output := filepath.Join(directory, "canonical.yaml")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := runCanonicalizeManifest([]string{
		"--ownership", ownership,
		"--input", input,
		"--input-format", "helm-release-json",
		"--namespace", "fugue-system",
		"--release-name", "fugue",
		"--release-version", "18",
		"--output", output,
	}, &stdout, &stderr); exitCode != 0 {
		t.Fatalf("exit=%d stderr=%s", exitCode, stderr.String())
	}
	canonical, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"main", "hook"} {
		if !bytes.Contains(canonical, []byte("name: "+name)) {
			t.Fatalf("canonical Helm output omitted %s:\n%s", name, canonical)
		}
	}
	if bytes.Contains(canonical, []byte("volatile")) || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("volatile/private output leaked: canonical=%s stdout=%q stderr=%q", canonical, stdout.String(), stderr.String())
	}

	excludedOutput := filepath.Join(directory, "canonical-without-hooks.yaml")
	stdout.Reset()
	stderr.Reset()
	if exitCode := runCanonicalizeManifest([]string{
		"--ownership", ownership,
		"--input", input,
		"--input-format", "helm-release-json",
		"--exclude-hooks",
		"--namespace", "fugue-system",
		"--release-name", "fugue",
		"--release-version", "18",
		"--output", excludedOutput,
	}, &stdout, &stderr); exitCode != 0 {
		t.Fatalf("exclude hooks exit=%d stderr=%s", exitCode, stderr.String())
	}
	excluded, err := os.ReadFile(excludedOutput)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(excluded, []byte("name: main")) || bytes.Contains(excluded, []byte("name: hook")) {
		t.Fatalf("explicit hook exclusion produced wrong manifest:\n%s", excluded)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("explicit hook exclusion leaked output: stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestRunCanonicalizeManifestRejectsNonBareOrDuplicateExcludeHooks(t *testing.T) {
	directory := t.TempDir()
	ownership := writeCanonicalTestFile(t, directory, "ownership.yaml", canonicalTestOwnership, 0o644)
	input := writeCanonicalTestFile(t, directory, "helm.json", `{"hooks":[],"manifest":"","name":"fugue","namespace":"fugue-system","version":18}`, 0o600)
	for _, hookArguments := range [][]string{
		{"--exclude-hooks", "--exclude-hooks"},
		{"--exclude-hooks=true"},
		{"--exclude-hooks=false"},
		{"-exclude-hooks"},
	} {
		args := []string{
			"--ownership", ownership,
			"--input", input,
			"--input-format", "helm-release-json",
			"--namespace", "fugue-system",
			"--release-name", "fugue",
			"--release-version", "18",
			"--output", filepath.Join(directory, "rejected-output"),
		}
		args = append(args, hookArguments...)
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		if exitCode := runCanonicalizeManifest(args, &stdout, &stderr); exitCode == 0 {
			t.Fatalf("non-bare or duplicate hook policy unexpectedly succeeded: %q", hookArguments)
		}
		if stdout.Len() != 0 {
			t.Fatalf("rejected hook policy leaked stdout: %q", stdout.String())
		}
	}
}

func TestRunCanonicalizeManifestRejectsUnsafeInputAndOutput(t *testing.T) {
	directory := t.TempDir()
	ownership := writeCanonicalTestFile(t, directory, "ownership.yaml", canonicalTestOwnership, 0o644)
	publicInput := writeCanonicalTestFile(t, directory, "public.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fixture}\n", 0o644)
	privateInput := writeCanonicalTestFile(t, directory, "private.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fixture}\n", 0o600)
	for _, test := range []struct {
		name   string
		input  string
		output string
		args   []string
	}{
		{name: "public input", input: publicInput, output: filepath.Join(directory, "public-output")},
		{name: "stdout output", input: publicInput, output: "-"},
		{name: "same input output", input: publicInput, output: publicInput},
		{name: "invalid format", input: publicInput, output: filepath.Join(directory, "format-output"), args: []string{"--input-format", "unknown"}},
		{name: "exclude hooks for manifest", input: privateInput, output: filepath.Join(directory, "manifest-hook-output"), args: []string{"--exclude-hooks"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := []string{"--ownership", ownership, "--input", test.input, "--namespace", "fugue-system", "--output", test.output}
			args = append(args, test.args...)
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if exitCode := runCanonicalizeManifest(args, &stdout, &stderr); exitCode == 0 {
				t.Fatal("unsafe invocation unexpectedly succeeded")
			}
			if stdout.Len() != 0 || strings.Contains(stderr.String(), "sentinel-private-render") {
				t.Fatalf("unsafe invocation leaked private output: stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
	link := filepath.Join(directory, "input-link")
	if err := os.Symlink(privateInput, link); err == nil {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		if exitCode := runCanonicalizeManifest([]string{
			"--ownership", ownership, "--input", link, "--namespace", "fugue-system", "--output", filepath.Join(directory, "link-output"),
		}, &stdout, &stderr); exitCode == 0 {
			t.Fatal("symlink input unexpectedly succeeded")
		}
	}
}

func TestRunCanonicalizeManifestRejectsParentSymlinkAliasToInput(t *testing.T) {
	directory := t.TempDir()
	ownership := writeCanonicalTestFile(t, directory, "ownership.yaml", canonicalTestOwnership, 0o644)
	realDirectory := filepath.Join(directory, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	raw := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: alias-fixture}\n"
	input := writeCanonicalTestFile(t, realDirectory, "input.yaml", raw, 0o600)
	aliasDirectory := filepath.Join(directory, "alias")
	if err := os.Symlink(realDirectory, aliasDirectory); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := runCanonicalizeManifest([]string{
		"--ownership", ownership,
		"--input", filepath.Join(aliasDirectory, "input.yaml"),
		"--namespace", "fugue-system",
		"--output", input,
	}, &stdout, &stderr); exitCode == 0 {
		t.Fatal("parent symlink alias unexpectedly overwrote raw input")
	}
	after, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != raw {
		t.Fatalf("raw input changed through parent alias:\n%s", after)
	}
	ownershipBefore, err := os.ReadFile(ownership)
	if err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if exitCode := runCanonicalizeManifest([]string{
		"--ownership", ownership,
		"--input", input,
		"--namespace", "fugue-system",
		"--output", ownership,
	}, &stdout, &stderr); exitCode == 0 {
		t.Fatal("ownership output alias unexpectedly succeeded")
	}
	ownershipAfter, err := os.ReadFile(ownership)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ownershipBefore, ownershipAfter) {
		t.Fatal("ownership source changed through output alias")
	}
	hardlinkOutput := filepath.Join(directory, "hardlink-output.yaml")
	if err := os.Link(input, hardlinkOutput); err == nil {
		stdout.Reset()
		stderr.Reset()
		if exitCode := runCanonicalizeManifest([]string{
			"--ownership", ownership,
			"--input", input,
			"--namespace", "fugue-system",
			"--output", hardlinkOutput,
		}, &stdout, &stderr); exitCode == 0 {
			t.Fatal("hardlink output alias unexpectedly succeeded")
		}
		after, err := os.ReadFile(input)
		if err != nil {
			t.Fatal(err)
		}
		if string(after) != raw {
			t.Fatal("raw input changed through hardlink output alias")
		}
	}
}

func TestRunCanonicalizeManifestRedactsPrivateParseErrors(t *testing.T) {
	directory := t.TempDir()
	ownership := writeCanonicalTestFile(t, directory, "ownership.yaml", canonicalTestOwnership, 0o644)
	for _, test := range []struct {
		name    string
		input   string
		extra   []string
		secret  string
		message string
	}{
		{
			name:   "YAML scalar",
			input:  "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: invalid}\ndata:\n  value: !!bool sentinel-private-yaml-error\n",
			secret: "sentinel-private-yaml-error", message: canonicalManifestError,
		},
		{
			name:   "Helm field",
			input:  `{"hooks":[],"manifest":"","name":"fugue","namespace":"fugue-system","sentinel-private-helm-error":true,"version":18}`,
			extra:  []string{"--input-format", "helm-release-json", "--release-name", "fugue", "--release-version", "18"},
			secret: "sentinel-private-helm-error", message: canonicalHelmError,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := writeCanonicalTestFile(t, directory, strings.ReplaceAll(test.name, " ", "-")+".input", test.input, 0o600)
			args := []string{
				"--ownership", ownership,
				"--input", input,
				"--namespace", "fugue-system",
				"--output", filepath.Join(directory, strings.ReplaceAll(test.name, " ", "-")+".output"),
			}
			args = append(args, test.extra...)
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if exitCode := runCanonicalizeManifest(args, &stdout, &stderr); exitCode == 0 {
				t.Fatal("invalid private input unexpectedly succeeded")
			}
			assertCanonicalFixedError(t, &stdout, &stderr, test.message, test.secret)
		})
	}
}

func TestRunCanonicalizeManifestUsesFixedNonLeakingFailures(t *testing.T) {
	directory := t.TempDir()
	ownership := writeCanonicalTestFile(t, directory, "ownership.yaml", canonicalTestOwnership, 0o644)
	input := writeCanonicalTestFile(t, directory, "input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fixture}\n", 0o600)

	for _, test := range []struct {
		name      string
		args      []string
		message   string
		sentinels []string
	}{
		{
			name:      "flag",
			args:      []string{"--sentinel-private-canonical-flag"},
			message:   canonicalArgumentsError,
			sentinels: []string{"sentinel-private-canonical-flag"},
		},
		{
			name:    "missing input argument",
			args:    []string{"--ownership", ownership, "--namespace", "fugue-system", "--output", filepath.Join(directory, "output.yaml")},
			message: canonicalArgumentsError,
		},
		{
			name:    "missing output argument",
			args:    []string{"--ownership", ownership, "--input", input, "--namespace", "fugue-system"},
			message: canonicalArgumentsError,
		},
		{
			name: "missing input file",
			args: []string{
				"--ownership", ownership,
				"--input", filepath.Join(directory, "sentinel-private-missing-input"),
				"--namespace", "fugue-system",
				"--output", filepath.Join(directory, "output.yaml"),
			},
			message:   canonicalInputError,
			sentinels: []string{"sentinel-private-missing-input"},
		},
		{
			name: "missing output directory",
			args: []string{
				"--ownership", ownership,
				"--input", input,
				"--namespace", "fugue-system",
				"--output", filepath.Join(directory, "sentinel-private-missing-output", "canonical.yaml"),
			},
			message:   canonicalOutputError,
			sentinels: []string{"sentinel-private-missing-output"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if exitCode := runCanonicalizeManifest(test.args, &stdout, &stderr); exitCode == 0 {
				t.Fatal("unsafe invocation unexpectedly succeeded")
			}
			assertCanonicalFixedError(t, &stdout, &stderr, test.message, test.sentinels...)
		})
	}

	const duplicateKeySentinel = "sentinel-private-duplicate-ownership-key"
	duplicateOwnership := writeCanonicalTestFile(t, directory, "duplicate-ownership.yaml", canonicalTestOwnership+duplicateKeySentinel+": first\n"+duplicateKeySentinel+": second\n", 0o644)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := runCanonicalizeManifest([]string{
		"--ownership", duplicateOwnership,
		"--input", input,
		"--namespace", "fugue-system",
		"--output", filepath.Join(directory, "duplicate-output.yaml"),
	}, &stdout, &stderr); exitCode == 0 {
		t.Fatal("duplicate ownership key unexpectedly succeeded")
	}
	assertCanonicalFixedError(t, &stdout, &stderr, canonicalOwnershipError, duplicateKeySentinel)
}

func assertCanonicalFixedError(t *testing.T, stdout, stderr *bytes.Buffer, message string, sentinels ...string) {
	t.Helper()
	if stdout.Len() != 0 {
		t.Fatalf("failed canonicalizer wrote stdout: %q", stdout.String())
	}
	if got, want := stderr.String(), message+"\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
	for _, sentinel := range sentinels {
		if strings.Contains(stdout.String(), sentinel) || strings.Contains(stderr.String(), sentinel) {
			t.Fatalf("failure output leaked sentinel %q: stdout=%q stderr=%q", sentinel, stdout.String(), stderr.String())
		}
	}
}

func writeCanonicalTestFile(t *testing.T, directory, name, contents string, mode os.FileMode) string {
	t.Helper()
	filename := filepath.Join(directory, name)
	if err := os.WriteFile(filename, []byte(contents), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filename, mode); err != nil {
		t.Fatal(err)
	}
	return filename
}

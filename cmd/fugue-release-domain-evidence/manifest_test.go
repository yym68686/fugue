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
}

func TestRunCanonicalizeManifestRejectsUnsafeInputAndOutput(t *testing.T) {
	directory := t.TempDir()
	ownership := writeCanonicalTestFile(t, directory, "ownership.yaml", canonicalTestOwnership, 0o644)
	publicInput := writeCanonicalTestFile(t, directory, "public.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fixture}\n", 0o644)
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
	privateInput := writeCanonicalTestFile(t, directory, "private.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fixture}\n", 0o600)
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
			secret: "sentinel-private-yaml-error", message: "private rendered manifest is invalid",
		},
		{
			name:   "Helm field",
			input:  `{"hooks":[],"manifest":"","name":"fugue","namespace":"fugue-system","sentinel-private-helm-error":true,"version":18}`,
			extra:  []string{"--input-format", "helm-release-json", "--release-name", "fugue", "--release-version", "18"},
			secret: "sentinel-private-helm-error", message: "private Helm render input is invalid",
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
			if stdout.Len() != 0 || strings.Contains(stderr.String(), test.secret) {
				t.Fatalf("private parse error leaked: stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), test.message) {
				t.Fatalf("redacted error = %q, want %q", stderr.String(), test.message)
			}
		})
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

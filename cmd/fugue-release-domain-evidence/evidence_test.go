package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/releasedomain"
)

type runnerFunc func(context.Context, string, []string, []byte, int64, string, ...string) ([]byte, error)

func (runner runnerFunc) Run(
	ctx context.Context,
	directory string,
	environment []string,
	input []byte,
	outputLimit int64,
	name string,
	args ...string,
) ([]byte, error) {
	return runner(ctx, directory, environment, input, outputLimit, name, args...)
}

func TestChangedLeafPointers(t *testing.T) {
	baseValue, err := decodeYAMLDocument([]byte(`
api:
  image:
    tag: old
list:
  - stable
escaped/key:
  thing~name: old
removed:
  nested: value
unchanged: true
`))
	if err != nil {
		t.Fatal(err)
	}
	targetValue, err := decodeYAMLDocument([]byte(`
api:
  image:
    tag: new
list:
  - stable
  - added
escaped/key:
  thing~name: new
added: {}
unchanged: true
`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := changedLeafPointers(
		yamlDocument{exists: true, value: baseValue},
		yamlDocument{exists: true, value: targetValue},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"/added",
		"/api/image/tag",
		"/escaped~1key/thing~0name",
		"/list/1",
		"/removed/nested",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pointers = %#v, want %#v", got, want)
	}
}

func TestChangedLeafPointersForAddedAndDeletedDocuments(t *testing.T) {
	value, err := decodeYAMLDocument([]byte("root:\n  child: value\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/root/child"}
	for _, test := range []struct {
		name   string
		base   yamlDocument
		target yamlDocument
	}{
		{name: "added", target: yamlDocument{exists: true, value: value}},
		{name: "deleted", base: yamlDocument{exists: true, value: value}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := changedLeafPointers(test.base, test.target)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("pointers = %#v, want %#v", got, want)
			}
		})
	}
}

func TestDecodeYAMLDocumentRejectsMultipleDocumentsAndNonStringKeys(t *testing.T) {
	for _, input := range []string{
		"first: true\n---\nsecond: true\n",
		"1: value\n",
	} {
		if _, err := decodeYAMLDocument([]byte(input)); err == nil {
			t.Fatalf("decodeYAMLDocument(%q) unexpectedly succeeded", input)
		}
	}
}

func TestNormalizeYAMLValueRejectsExcessiveDepth(t *testing.T) {
	var value any = "leaf"
	for index := 0; index < maxYAMLDepth+2; index++ {
		value = map[string]any{"nested": value}
	}
	if _, err := normalizeYAMLValue(value); err == nil {
		t.Fatal("excessively nested YAML unexpectedly succeeded")
	}
}

func TestConsumerEvidenceKeepsOutsideCommandsFailClosed(t *testing.T) {
	graph := &consumerGraph{
		modulePath: "example.test/repository",
		consumers: map[string][]commandConsumer{
			"example.test/repository/internal/shared": {
				{path: "cmd/fugue-api", domain: releasedomain.DomainControlPlane},
				{path: "cmd/operator-tool"},
			},
		},
	}
	domains, outside := consumerEvidence("internal/shared/shared.go", graph)
	if !reflect.DeepEqual(domains, []releasedomain.Domain{releasedomain.DomainControlPlane}) {
		t.Fatalf("domains = %#v", domains)
	}
	if !reflect.DeepEqual(outside, []string{"cmd/operator-tool"}) {
		t.Fatalf("outside = %#v", outside)
	}

	classification := releasedomain.ClassifyFiles([]releasedomain.ChangedFile{{
		Status:           releasedomain.ChangeModified,
		Path:             "internal/shared/shared.go",
		ConsumerDomains:  domains,
		OutsideConsumers: outside,
	}}, &releasedomain.OwnershipSpec{})
	if len(classification.Unknown) != 1 {
		t.Fatalf("classification = %#v", classification)
	}
}

func TestMissingConsumerEvidenceFailsClosed(t *testing.T) {
	domains, outside := consumerEvidence("internal/orphan/orphan.go", &consumerGraph{
		modulePath: "example.test/repository",
		consumers:  map[string][]commandConsumer{},
	})
	change := releasedomain.ChangedFile{
		Status:           releasedomain.ChangeAdded,
		Path:             "internal/orphan/orphan.go",
		ConsumerDomains:  domains,
		OutsideConsumers: outside,
	}
	classification := releasedomain.ClassifyFiles([]releasedomain.ChangedFile{change}, &releasedomain.OwnershipSpec{})
	if len(classification.Unknown) != 1 {
		t.Fatalf("classification = %#v", classification)
	}
}

func TestUnavailableValuesEvidenceIsEmittedFailClosed(t *testing.T) {
	repository := t.TempDir()
	runner := runnerFunc(func(_ context.Context, _ string, environment []string, _ []byte, _ int64, name string, args ...string) ([]byte, error) {
		if name != "git" || len(args) == 0 {
			return nil, errors.New("unexpected command")
		}
		if environmentValue(environment, "GIT_NO_LAZY_FETCH") != "1" || environmentValue(environment, "GIT_NO_REPLACE_OBJECTS") != "1" {
			t.Fatal("Git evidence command is not isolated from lazy fetch or replacement objects")
		}
		switch args[0] {
		case "rev-parse":
			if strings.HasPrefix(args[len(args)-1], "base") {
				return []byte(strings.Repeat("a", 40) + "\n"), nil
			}
			return []byte(strings.Repeat("b", 40) + "\n"), nil
		case "diff":
			if !containsString(args, "--no-renames") || !containsString(args, "--ignore-submodules=none") {
				t.Fatal("git diff did not freeze rename and submodule semantics")
			}
			return []byte("M\x00deploy/helm/fugue/values.yaml\x00"), nil
		case "cat-file":
			if strings.HasPrefix(args[len(args)-1], strings.Repeat("a", 40)+":") {
				return []byte("api:\n  image:\n    tag: base\n"), nil
			}
			return []byte("api: [unterminated\n"), nil
		default:
			return nil, errors.New("unexpected git operation")
		}
	})
	result, warnings, err := produceEvidence(context.Background(), cliOptions{
		repository:     repository,
		baseRevision:   "base",
		targetRevision: "target",
		outputPath:     "-",
		goBinary:       "go",
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.changes) != 1 || len(result.changes[0].ValuePointers) != 0 || len(warnings) != 1 {
		t.Fatalf("changes = %#v, warnings = %#v", result.changes, warnings)
	}
	classification := releasedomain.ClassifyFiles(result.changes, &releasedomain.OwnershipSpec{
		ValueRules: []releasedomain.ValueRule{{
			ID: "values-api", Glob: valuesFilePattern, Pointer: "/api", Domain: releasedomain.DomainControlPlane,
		}},
	})
	if len(classification.Unknown) != 1 {
		t.Fatalf("classification = %#v", classification)
	}
}

func TestUnavailableGoGraphIsEmittedFailClosed(t *testing.T) {
	changes := []releasedomain.ChangedFile{{
		Status: releasedomain.ChangeModified,
		Path:   "internal/shared/shared.go",
	}}
	runner := runnerFunc(func(context.Context, string, []string, []byte, int64, string, ...string) ([]byte, error) {
		return nil, errors.New("tree unavailable")
	})
	warnings := make([]string, 0)
	enrichGoEvidence(
		context.Background(),
		runner,
		t.TempDir(),
		"base-object",
		"target-object",
		"go",
		changes,
		&warnings,
	)
	if len(changes[0].ConsumerDomains) != 0 || len(changes[0].OutsideConsumers) != 0 || len(warnings) != 2 {
		t.Fatalf("changes = %#v, warnings = %#v", changes, warnings)
	}
	classification := releasedomain.ClassifyFiles(changes, &releasedomain.OwnershipSpec{})
	if len(classification.Unknown) != 1 {
		t.Fatalf("classification = %#v", classification)
	}
}

func TestGoTestChangesDoNotRequireRuntimeConsumerGraph(t *testing.T) {
	changes := []releasedomain.ChangedFile{{
		Status: releasedomain.ChangeModified,
		Path:   "internal/shared/shared_test.go",
	}}
	runner := runnerFunc(func(context.Context, string, []string, []byte, int64, string, ...string) ([]byte, error) {
		t.Fatal("test-only Go change unexpectedly invoked the runtime consumer graph")
		return nil, nil
	})
	warnings := make([]string, 0)
	enrichGoEvidence(
		context.Background(),
		runner,
		t.TempDir(),
		strings.Repeat("a", 40),
		strings.Repeat("b", 40),
		"go",
		changes,
		&warnings,
	)
	if len(warnings) != 0 || len(changes[0].ConsumerDomains) != 0 || len(changes[0].OutsideConsumers) != 0 {
		t.Fatalf("changes = %#v, warnings = %#v", changes, warnings)
	}
}

func TestProduceEvidenceFromGitRevisions(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go is not installed")
	}
	repository := t.TempDir()
	runGit(t, repository, "init", "-q")
	writeTestFile(t, repository, "go.mod", "module example.test/evidence\n\ngo 1.22\n")
	writeTestFile(t, repository, "cmd/fugue-api/main.go", `package main

import "example.test/evidence/internal/shared"

func main() { _ = shared.Value() }
`)
	writeTestFile(t, repository, "cmd/operator-tool/main.go", `package main

import "example.test/evidence/internal/shared"

func main() { _ = shared.Value() }
`)
	writeTestFile(t, repository, "internal/shared/shared.go", `package shared

func Value() string { return "base" }
`)
	writeTestFile(t, repository, "deploy/helm/fugue/values.yaml", `api:
  image:
    tag: base
escaped/key:
  thing~name: base
`)
	runGit(t, repository, "add", ".")
	runGit(t, repository, "-c", "user.name=Evidence Test", "-c", "user.email=evidence@example.test", "commit", "-qm", "base")
	base := strings.TrimSpace(runGit(t, repository, "rev-parse", "HEAD"))

	writeTestFile(t, repository, "internal/shared/shared.go", `package shared

func Value() string { return "target" }
`)
	writeTestFile(t, repository, "deploy/helm/fugue/values.yaml", `api:
  image:
    tag: target
escaped/key:
  thing~name: target
`)
	runGit(t, repository, "add", ".")
	runGit(t, repository, "-c", "user.name=Evidence Test", "-c", "user.email=evidence@example.test", "commit", "-qm", "target")
	target := strings.TrimSpace(runGit(t, repository, "rev-parse", "HEAD"))
	attributeFile := filepath.Join(t.TempDir(), "unversioned-attributes")
	if err := os.WriteFile(attributeFile, []byte("cmd/operator-tool/** export-ignore\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "config", "core.attributesFile", attributeFile)
	infoAttributes := filepath.Join(repository, ".git", "info", "attributes")
	if err := os.MkdirAll(filepath.Dir(infoAttributes), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(infoAttributes, []byte("cmd/operator-tool/** export-ignore\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// The producer must not borrow evidence from the checkout. Make every
	// working-tree source it could otherwise inspect unusable after both refs
	// have been committed; the ref-only result below must remain unchanged.
	writeTestFile(t, repository, "go.mod", "this is not a go module\n")
	writeTestFile(t, repository, "internal/shared/shared.go", "this is not Go source\n")
	writeTestFile(t, repository, "deploy/helm/fugue/values.yaml", "api: [unterminated\n")
	dirtyBefore := runGit(t, repository, "status", "--porcelain=v1", "-z")

	result, warnings, err := produceEvidence(context.Background(), cliOptions{
		repository:     repository,
		baseRevision:   base,
		targetRevision: target,
		outputPath:     "-",
		goBinary:       "go",
	}, execCommandRunner{})
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v", warnings)
	}
	repeated, repeatedWarnings, err := produceEvidence(context.Background(), cliOptions{
		repository:     repository,
		baseRevision:   base,
		targetRevision: target,
		outputPath:     "-",
		goBinary:       "go",
	}, execCommandRunner{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(repeated, result) || !reflect.DeepEqual(repeatedWarnings, warnings) {
		t.Fatalf("repeated evidence drifted: first=%#v/%#v second=%#v/%#v", result, warnings, repeated, repeatedWarnings)
	}
	firstDocument, err := newEvidenceDocument(result)
	if err != nil {
		t.Fatal(err)
	}
	repeatedDocument, err := newEvidenceDocument(repeated)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, err := json.Marshal(firstDocument)
	if err != nil {
		t.Fatal(err)
	}
	repeatedBytes, err := json.Marshal(repeatedDocument)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, repeatedBytes) {
		t.Fatalf("repeated documents differ:\n%s\n%s", firstBytes, repeatedBytes)
	}
	if dirtyAfter := runGit(t, repository, "status", "--porcelain=v1", "-z"); dirtyAfter != dirtyBefore {
		t.Fatalf("producer mutated the checkout: before %q, after %q", dirtyBefore, dirtyAfter)
	}
	if len(result.changes) != 2 {
		t.Fatalf("changes = %#v", result.changes)
	}

	values := findChange(t, result.changes, "deploy/helm/fugue/values.yaml")
	if !reflect.DeepEqual(values.ValuePointers, []string{
		"/api/image/tag",
		"/escaped~1key/thing~0name",
	}) {
		t.Fatalf("value pointers = %#v", values.ValuePointers)
	}
	shared := findChange(t, result.changes, "internal/shared/shared.go")
	if !reflect.DeepEqual(shared.ConsumerDomains, []releasedomain.Domain{releasedomain.DomainControlPlane}) {
		t.Fatalf("consumer domains = %#v", shared.ConsumerDomains)
	}
	if !reflect.DeepEqual(shared.OutsideConsumers, []string{"cmd/operator-tool"}) {
		t.Fatalf("outside consumers = %#v", shared.OutsideConsumers)
	}

	document, err := newEvidenceDocument(result)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(document.Changes)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := releasedomain.DecodeChangedFilesJSON(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("embedded changes are not ChangedFile-compatible: %v", err)
	}
	if !reflect.DeepEqual(decoded, result.changes) {
		t.Fatalf("decoded = %#v, want %#v", decoded, result.changes)
	}
}

func TestProduceEvidenceDisablesReplacementObjects(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repository := t.TempDir()
	runGit(t, repository, "init", "-q")
	writeTestFile(t, repository, "docs/object.md", "base\n")
	runGit(t, repository, "add", ".")
	runGit(t, repository, "-c", "user.name=Evidence Test", "-c", "user.email=evidence@example.test", "commit", "-qm", "base")
	base := strings.TrimSpace(runGit(t, repository, "rev-parse", "HEAD"))
	writeTestFile(t, repository, "docs/object.md", "target\n")
	runGit(t, repository, "add", ".")
	runGit(t, repository, "-c", "user.name=Evidence Test", "-c", "user.email=evidence@example.test", "commit", "-qm", "target")
	target := strings.TrimSpace(runGit(t, repository, "rev-parse", "HEAD"))
	runGit(t, repository, "replace", target, base)
	if got := runGit(t, repository, "diff", "--name-only", base, target); got != "" {
		t.Fatalf("replacement object setup did not mask the target: %q", got)
	}

	result, warnings, err := produceEvidence(context.Background(), cliOptions{
		repository:     repository,
		baseRevision:   base,
		targetRevision: target,
		goBinary:       "go",
	}, execCommandRunner{})
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v", warnings)
	}
	if len(result.changes) != 1 || result.changes[0].Status != releasedomain.ChangeModified || result.changes[0].Path != "docs/object.md" {
		t.Fatalf("changes = %#v", result.changes)
	}
}

func TestProduceEvidenceExpandsRenamesAndRejectsTypeChanges(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repository := t.TempDir()
	runGit(t, repository, "init", "-q")
	oldPath := "docs/旧\tname\n.md"
	newPath := "docs/新\tname\n.md"
	writeTestFile(t, repository, oldPath, "contents\n")
	runGit(t, repository, "add", ".")
	runGit(t, repository, "-c", "user.name=Evidence Test", "-c", "user.email=evidence@example.test", "commit", "-qm", "base")
	base := strings.TrimSpace(runGit(t, repository, "rev-parse", "HEAD"))
	runGit(t, repository, "mv", oldPath, newPath)
	runGit(t, repository, "-c", "user.name=Evidence Test", "-c", "user.email=evidence@example.test", "commit", "-qm", "rename")
	renameCommit := strings.TrimSpace(runGit(t, repository, "rev-parse", "HEAD"))

	result, warnings, err := produceEvidence(context.Background(), cliOptions{
		repository:     repository,
		baseRevision:   base,
		targetRevision: renameCommit,
		goBinary:       "go",
	}, execCommandRunner{})
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 || len(result.changes) != 2 {
		t.Fatalf("changes = %#v, warnings = %#v", result.changes, warnings)
	}
	statuses := map[string]releasedomain.ChangeStatus{}
	for _, change := range result.changes {
		statuses[change.Path] = change.Status
	}
	if statuses[oldPath] != releasedomain.ChangeDeleted || statuses[newPath] != releasedomain.ChangeAdded {
		t.Fatalf("rename evidence = %#v", result.changes)
	}

	if err := os.Remove(filepath.Join(repository, filepath.FromSlash(newPath))); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("object-target", filepath.Join(repository, filepath.FromSlash(newPath))); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}
	runGit(t, repository, "add", ".")
	runGit(t, repository, "-c", "user.name=Evidence Test", "-c", "user.email=evidence@example.test", "commit", "-qm", "type change")
	typeCommit := strings.TrimSpace(runGit(t, repository, "rev-parse", "HEAD"))
	if _, _, err := produceEvidence(context.Background(), cliOptions{
		repository:     repository,
		baseRevision:   renameCommit,
		targetRevision: typeCommit,
		goBinary:       "go",
	}, execCommandRunner{}); err == nil || !strings.Contains(err.Error(), "unsupported name-status") {
		t.Fatalf("type change error = %v", err)
	}
}

func TestParseFlagsRequiresRevisions(t *testing.T) {
	var stderr bytes.Buffer
	if _, err := parseFlags(nil, &stderr); err == nil {
		t.Fatal("expected missing revisions to fail")
	}
	options, err := parseFlags([]string{"--base", "base", "--target", "target"}, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if options.repository != "." || options.outputPath != "-" || options.goBinary != "go" || options.timeout != defaultEvidenceTimeout {
		t.Fatalf("options = %#v", options)
	}
	if _, err := parseFlags([]string{"--base", "base", "--target", "target", "--timeout", "0s"}, &stderr); err == nil {
		t.Fatal("expected a non-positive timeout to fail")
	}
}

func TestEvidenceDocumentBindsCommitsAndIsDeterministic(t *testing.T) {
	result := evidenceResult{
		baseCommit:   strings.Repeat("a", 40),
		targetCommit: strings.Repeat("b", 40),
		changes: []releasedomain.ChangedFile{{
			Status: releasedomain.ChangeModified,
			Path:   "docs/evidence.md",
		}},
	}
	first, err := newEvidenceDocument(result)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newEvidenceDocument(result)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("evidence is not deterministic:\n%s\n%s", firstJSON, secondJSON)
	}
	payloadJSON, err := json.Marshal(evidencePayload{
		APIVersion:   first.APIVersion,
		Kind:         first.Kind,
		Policy:       first.Policy,
		BaseCommit:   first.BaseCommit,
		TargetCommit: first.TargetCommit,
		Changes:      first.Changes,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantDigest := fmt.Sprintf("sha256:%x", sha256.Sum256(payloadJSON))
	if first.Digest != wantDigest {
		t.Fatalf("digest = %q, want %q", first.Digest, wantDigest)
	}
	changed := result
	changed.targetCommit = strings.Repeat("c", 40)
	changedDocument, err := newEvidenceDocument(changed)
	if err != nil {
		t.Fatal(err)
	}
	if changedDocument.Digest == first.Digest {
		t.Fatal("changing the target commit did not change the evidence digest")
	}
}

func TestRunRefusesIncompleteEvidenceWithoutReplacingOutput(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repository := t.TempDir()
	runGit(t, repository, "init", "-q")
	writeTestFile(t, repository, "deploy/helm/fugue/values.yaml", "api:\n  image: base\n")
	runGit(t, repository, "add", ".")
	runGit(t, repository, "-c", "user.name=Evidence Test", "-c", "user.email=evidence@example.test", "commit", "-qm", "base")
	base := strings.TrimSpace(runGit(t, repository, "rev-parse", "HEAD"))
	writeTestFile(t, repository, "deploy/helm/fugue/values.yaml", "api: [unterminated\n")
	runGit(t, repository, "add", ".")
	runGit(t, repository, "-c", "user.name=Evidence Test", "-c", "user.email=evidence@example.test", "commit", "-qm", "target")
	target := strings.TrimSpace(runGit(t, repository, "rev-parse", "HEAD"))

	output := filepath.Join(t.TempDir(), "evidence.json")
	if err := os.WriteFile(output, []byte("preserve-me"), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := run(context.Background(), []string{
		"--repo", repository,
		"--base", base,
		"--target", target,
		"--output", output,
	}, &stdout, &stderr)
	if exitCode == 0 {
		t.Fatalf("incomplete evidence unexpectedly succeeded: %s", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("incomplete evidence leaked stdout: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "evidence is incomplete") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	contents, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "preserve-me" {
		t.Fatalf("failed production replaced output with %q", contents)
	}
}

func TestWritePrivateAtomicFileReplacesModeAndRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "evidence.json")
	if err := os.WriteFile(output, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writePrivateAtomicFile(output, []byte("new")); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "new" {
		t.Fatalf("contents = %q", contents)
	}
	info, err := os.Stat(output)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 600", info.Mode().Perm())
	}

	victim := filepath.Join(directory, "victim")
	if err := os.WriteFile(victim, []byte("victim"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "link.json")
	if err := os.Symlink(victim, link); err != nil {
		t.Skipf("symbolic links are unavailable: %v", err)
	}
	if err := writePrivateAtomicFile(link, []byte("clobber")); err == nil {
		t.Fatal("symbolic-link output unexpectedly succeeded")
	}
	victimContents, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if string(victimContents) != "victim" {
		t.Fatalf("symbolic-link victim was modified: %q", victimContents)
	}
}

func TestExecCommandRunnerBoundsOutputAndHonorsContext(t *testing.T) {
	runner := execCommandRunner{}
	if _, err := runner.Run(context.Background(), t.TempDir(), nil, make([]byte, maxCommandInput+1), 3, "sh", "-c", "exit 0"); err == nil {
		t.Fatal("oversized command input unexpectedly succeeded")
	}
	if _, err := runner.Run(context.Background(), t.TempDir(), nil, nil, 3, "sh", "-c", "printf 1234"); !errors.Is(err, errCommandOutputLimit) {
		t.Fatalf("oversized command output error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := runner.Run(ctx, t.TempDir(), nil, nil, 16, "sh", "-c", "sleep 5"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("deadline took %s", elapsed)
	}
}

func TestBuildOfflineGoEnvironmentIsolatesCachesAndNetwork(t *testing.T) {
	moduleCache := filepath.Join(t.TempDir(), "module-cache")
	if err := os.MkdirAll(filepath.Join(moduleCache, "cache", "download"), 0o755); err != nil {
		t.Fatal(err)
	}
	runner := runnerFunc(func(_ context.Context, _ string, environment []string, _ []byte, limit int64, name string, args ...string) ([]byte, error) {
		if name != "go" || !reflect.DeepEqual(args, []string{"env", "GOMODCACHE"}) {
			return nil, fmt.Errorf("unexpected command: %s %#v", name, args)
		}
		if limit != maxRevisionOutput || environmentValue(environment, "GOPROXY") != "off" || environmentValue(environment, "GOENV") != "off" {
			return nil, fmt.Errorf("unsafe discovery environment")
		}
		return []byte(moduleCache + "\n"), nil
	})
	environment, cleanup, err := buildOfflineGoEnvironment(context.Background(), runner, t.TempDir(), "go")
	if err != nil {
		t.Fatal(err)
	}
	privateRoot := filepath.Dir(environmentValue(environment, "GOCACHE"))
	defer cleanup()
	for _, name := range []string{"GOCACHE", "GOMODCACHE", "GOPATH", "GOTMPDIR", "HOME", "TMPDIR"} {
		value := environmentValue(environment, name)
		if value == "" || !strings.HasPrefix(value, privateRoot+string(filepath.Separator)) {
			t.Fatalf("%s is not private: %q", name, value)
		}
	}
	proxy := environmentValue(environment, "GOPROXY")
	if !strings.HasPrefix(proxy, "file://") || !strings.HasSuffix(proxy, ",off") {
		t.Fatalf("GOPROXY = %q", proxy)
	}
	if environmentValue(environment, "GOSUMDB") != "off" || environmentValue(environment, "GOVCS") != "*:off" || environmentValue(environment, "GOTOOLCHAIN") != "local" {
		t.Fatalf("offline environment = %#v", environment)
	}
	cleanup()
	if _, err := os.Stat(privateRoot); !os.IsNotExist(err) {
		t.Fatalf("private cache survived cleanup: %v", err)
	}
}

func TestRejectLocalModuleReplacements(t *testing.T) {
	for _, test := range []struct {
		name    string
		output  string
		wantErr bool
	}{
		{
			name:    "relative local path",
			output:  `{"Replace":[{"Old":{"Path":"example.test/plugin"},"New":{"Path":"../plugin"}}]}`,
			wantErr: true,
		},
		{
			name:    "absolute local path",
			output:  `{"Replace":[{"Old":{"Path":"example.test/plugin"},"New":{"Path":"/tmp/plugin"}}]}`,
			wantErr: true,
		},
		{
			name:   "versioned module",
			output: `{"Replace":[{"Old":{"Path":"example.test/plugin"},"New":{"Path":"example.test/plugin-v2","Version":"v2.0.0"}}]}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			runner := runnerFunc(func(_ context.Context, _ string, _ []string, input []byte, limit int64, name string, args ...string) ([]byte, error) {
				if len(input) != 0 || limit != maxGoModJSONOutput || name != "go" || !reflect.DeepEqual(args, []string{"mod", "edit", "-json"}) {
					return nil, fmt.Errorf("unexpected command: input=%q limit=%d %s %#v", input, limit, name, args)
				}
				return []byte(test.output), nil
			})
			err := rejectLocalModuleReplacements(context.Background(), runner, t.TempDir(), []string{"PATH=" + os.Getenv("PATH")}, "go")
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestChangedValuesAndDocumentBudgetsFailClosed(t *testing.T) {
	collector := newPointerCollector()
	collector.bytes = maxValuePointerBytes
	collector.add("/overflow")
	if collector.err == nil {
		t.Fatal("changed values pointer byte overflow unexpectedly succeeded")
	}
	collector = newPointerCollector()
	for index := 0; index < maxValuePointers; index++ {
		collector.values[fmt.Sprintf("/%d", index)] = struct{}{}
	}
	collector.add("/overflow")
	if collector.err == nil {
		t.Fatal("changed values pointer count overflow unexpectedly succeeded")
	}
	if _, err := newEvidenceDocument(evidenceResult{
		baseCommit:   strings.Repeat("a", 40),
		targetCommit: strings.Repeat("b", 40),
		changes: []releasedomain.ChangedFile{{
			Status: releasedomain.ChangeModified,
			Path:   strings.Repeat("x", maxEvidenceStringBytes+1),
		}},
	}); err == nil {
		t.Fatal("oversized evidence document unexpectedly succeeded")
	}
}

func TestParseRepositoryTreeRejectsUnsafeEntries(t *testing.T) {
	object := strings.Repeat("a", 40)
	for _, listing := range [][]byte{
		[]byte("100644 blob " + object + "\t../escape\x00"),
		[]byte("120000 blob " + object + "\tlink\x00"),
		[]byte("160000 commit " + object + "\tsubmodule\x00"),
		[]byte("100644 blob " + object + "\tinvalid\\path\x00"),
		[]byte("100644 blob " + object + "\tunterminated"),
	} {
		if _, err := parseRepositoryTree(listing); err == nil {
			t.Fatalf("unsafe tree listing unexpectedly succeeded: %q", listing)
		}
	}
}

func TestMaterializeRepositoryTreeRejectsOversizedAndMismatchedBlobs(t *testing.T) {
	object := strings.Repeat("a", 40)
	entry := repositoryTreeEntry{mode: 0o644, object: object, path: "safe/file"}
	for _, batch := range [][]byte{
		[]byte(fmt.Sprintf("%s blob %d\n", object, maxSnapshotFileSize+1)),
		[]byte(strings.Repeat("b", 40) + " blob 1\nx\n"),
		[]byte(object + " tree 1\nx\n"),
		[]byte(object + " blob 1\nx"),
	} {
		if err := materializeRepositoryTree(context.Background(), t.TempDir(), []repositoryTreeEntry{entry}, batch); err == nil {
			t.Fatalf("unsafe blob batch unexpectedly succeeded: %q", batch)
		}
	}
}

func environmentValue(environment []string, target string) string {
	prefix := target + "="
	for _, entry := range environment {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

func findChange(t *testing.T, changes []releasedomain.ChangedFile, repositoryPath string) releasedomain.ChangedFile {
	t.Helper()
	for _, change := range changes {
		if change.Path == repositoryPath {
			return change
		}
	}
	t.Fatalf("change %s not found in %#v", repositoryPath, changes)
	return releasedomain.ChangedFile{}
}

func writeTestFile(t *testing.T, root, relativePath, contents string) {
	t.Helper()
	filename := filepath.Join(root, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
}

func runGit(t *testing.T, repository string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = repository
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

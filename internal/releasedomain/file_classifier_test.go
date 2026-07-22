package releasedomain

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func readChangedFixture(t *testing.T, parts ...string) []ChangedFile {
	t.Helper()
	pathParts := append([]string{"testdata"}, parts...)
	file, err := os.Open(filepath.Join(pathParts...))
	if err != nil {
		t.Fatalf("open changed-files fixture: %v", err)
	}
	defer file.Close()
	changes, err := DecodeChangedFilesJSON(file)
	if err != nil {
		t.Fatalf("decode changed-files fixture: %v", err)
	}
	return changes
}

func TestClassifyFilesFixtures(t *testing.T) {
	spec := testOwnership(t)
	tests := []struct {
		name           string
		changes        []ChangedFile
		wantDomains    []Domain
		wantUnknown    bool
		wantNonRuntime bool
	}{
		{
			name: "zero", changes: readChangedFixture(t, "zero", "changed-files.json"),
			wantNonRuntime: true,
		},
		{
			name: "single-node-local", changes: readChangedFixture(t, "single-node-local", "changed-files.json"),
			wantDomains: []Domain{DomainNodeLocal},
		},
		{
			name: "multiple", changes: readChangedFixture(t, "multiple", "changed-files.json"),
			wantDomains: []Domain{DomainNodeLocal, DomainAuthoritativeDNS},
		},
		{
			name: "shared-helper", changes: readChangedFixture(t, "unknown", "shared-helper-changed-files.json"),
			wantUnknown: true,
		},
		{
			name: "crd", changes: readChangedFixture(t, "crd", "changed-files.json"),
			wantUnknown: true,
		},
		{
			name: "cnpg-backup", changes: readChangedFixture(t, "cnpg-override", "changed-files-backup.json"),
			wantDomains: []Domain{DomainBackup},
		},
		{
			name: "cnpg-multiple", changes: readChangedFixture(t, "cnpg-override", "changed-files-multiple.json"),
			wantDomains: []Domain{DomainControlPlane, DomainBackup},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ClassifyFiles(test.changes, spec)
			if !equalDomains(got.Domains, test.wantDomains) {
				t.Fatalf("domains = %v, want %v", got.Domains, test.wantDomains)
			}
			if (len(got.Unknown) > 0) != test.wantUnknown {
				t.Fatalf("unknown = %#v", got.Unknown)
			}
			if got.AllNonRuntime != test.wantNonRuntime {
				t.Fatalf("AllNonRuntime = %v", got.AllNonRuntime)
			}
		})
	}
}

func TestValuesUnknownLeafFailsClosed(t *testing.T) {
	classification := ClassifyFiles([]ChangedFile{{
		Status:        ChangeModified,
		Path:          "deploy/helm/fugue/values.yaml",
		ValuePointers: []string{"/podLabels/release"},
	}}, testOwnership(t))
	if len(classification.Unknown) != 1 || classification.AllNonRuntime {
		t.Fatalf("classification = %#v", classification)
	}
}

func TestGoConsumerAndSemanticDomainsAreCombined(t *testing.T) {
	classification := ClassifyFiles([]ChangedFile{{
		Status:          ChangeModified,
		Path:            "internal/api/backup.go",
		ConsumerDomains: []Domain{DomainControlPlane},
		SemanticDomains: []Domain{DomainBackup},
	}}, testOwnership(t))
	if len(classification.Unknown) != 0 {
		t.Fatalf("unknown = %#v", classification.Unknown)
	}
	if !equalDomains(classification.Domains, []Domain{DomainControlPlane, DomainBackup}) {
		t.Fatalf("domains = %v", classification.Domains)
	}
}

func TestGoOutsideConsumerIsUnknown(t *testing.T) {
	classification := ClassifyFiles([]ChangedFile{{
		Status:           ChangeModified,
		Path:             "cmd/fugue-dns/main.go",
		ConsumerDomains:  []Domain{DomainAuthoritativeDNS},
		OutsideConsumers: []string{"edge-proxy", "caddy"},
	}}, testOwnership(t))
	if len(classification.Unknown) != 1 {
		t.Fatalf("unknown = %#v", classification.Unknown)
	}
}

func TestRuntimeGoWithoutConsumerEvidenceIsUnknown(t *testing.T) {
	classification := ClassifyFiles([]ChangedFile{{
		Status: ChangeModified,
		Path:   "internal/dnsserver/service.go",
	}}, testOwnership(t))
	if len(classification.Unknown) != 1 {
		t.Fatalf("unknown = %#v", classification.Unknown)
	}
}

func TestAuthoritativeOpenAPISourceIsControlPlaneOwned(t *testing.T) {
	classification := ClassifyFiles([]ChangedFile{{
		Status: ChangeModified,
		Path:   "openapi/openapi.yaml",
	}}, testOwnership(t))
	if len(classification.Unknown) != 0 {
		t.Fatalf("unknown = %#v", classification.Unknown)
	}
	if !equalDomains(classification.Domains, []Domain{DomainControlPlane}) {
		t.Fatalf("domains = %v, want [%s]", classification.Domains, DomainControlPlane)
	}
	if classification.AllNonRuntime {
		t.Fatal("authoritative OpenAPI source was classified non-runtime")
	}
}

func TestParseNameStatusZ(t *testing.T) {
	changes, err := ParseNameStatusZ(strings.NewReader("M\x00docs/runbook.md\x00A\x00new.txt\x00"))
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || changes[0].Status != ChangeModified || changes[0].Path != "docs/runbook.md" || changes[1].Status != ChangeAdded {
		t.Fatalf("changes = %#v", changes)
	}
}

func TestParseNameStatusZRejectsRenameAndMissingTerminator(t *testing.T) {
	if _, err := ParseNameStatusZ(strings.NewReader("R100\x00old\x00new\x00")); err == nil {
		t.Fatal("expected rename error")
	}
	if _, err := ParseNameStatusZ(strings.NewReader("M\x00path")); err == nil {
		t.Fatal("expected terminator error")
	}
}

func TestDuplicateAndTraversalPathsAreUnknown(t *testing.T) {
	classification := ClassifyFiles([]ChangedFile{
		{Status: ChangeModified, Path: "docs/a.md"},
		{Status: ChangeModified, Path: "docs/a.md"},
		{Status: ChangeModified, Path: "../outside"},
	}, testOwnership(t))
	if len(classification.Unknown) != 2 {
		t.Fatalf("unknown = %#v", classification.Unknown)
	}
}

func TestDecodeChangedFilesJSONStrictValidDocument(t *testing.T) {
	changes, err := DecodeChangedFilesJSON(strings.NewReader(`[
  {
    "status": "M",
    "path": "internal/example/\uD83D\uDE00.go",
    "valuePointers": ["/one"],
    "consumerDomains": ["control-plane"],
    "semanticDomains": ["backup"],
    "outsideConsumers": ["external-binary"]
  }
]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Status != ChangeModified || changes[0].Path != "internal/example/😀.go" {
		t.Fatalf("changes = %#v", changes)
	}
	if !equalDomains(changes[0].ConsumerDomains, []Domain{DomainControlPlane}) ||
		!equalDomains(changes[0].SemanticDomains, []Domain{DomainBackup}) {
		t.Fatalf("domain evidence = %#v", changes[0])
	}
}

func TestDecodeChangedFilesJSONRejectsNonCanonicalEvidence(t *testing.T) {
	invalidUTF8 := append([]byte(`[{"status":"M","path":"`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"}]`)...)
	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty input", data: nil},
		{name: "null root", data: []byte(`null`)},
		{name: "object root", data: []byte(`{"status":"M","path":"a"}`)},
		{name: "scalar root", data: []byte(`1`)},
		{name: "null entry", data: []byte(`[null]`)},
		{name: "array entry", data: []byte(`[[]]`)},
		{name: "duplicate status", data: []byte(`[{"status":"M","status":"A","path":"a"}]`)},
		{name: "escaped duplicate status", data: []byte(`[{"status":"M","\u0073tatus":"A","path":"a"}]`)},
		{name: "duplicate path", data: []byte(`[{"status":"M","path":"a","path":"b"}]`)},
		{name: "unknown field", data: []byte(`[{"status":"M","path":"a","extra":[]}]`)},
		{name: "case alias", data: []byte(`[{"Status":"M","path":"a"}]`)},
		{name: "missing status", data: []byte(`[{"path":"a"}]`)},
		{name: "missing path", data: []byte(`[{"status":"M"}]`)},
		{name: "null status", data: []byte(`[{"status":null,"path":"a"}]`)},
		{name: "numeric status", data: []byte(`[{"status":1,"path":"a"}]`)},
		{name: "null path", data: []byte(`[{"status":"M","path":null}]`)},
		{name: "array path", data: []byte(`[{"status":"M","path":[]}]`)},
		{name: "rename status", data: []byte(`[{"status":"R100","path":"a"}]`)},
		{name: "copy status", data: []byte(`[{"status":"C100","path":"a"}]`)},
		{name: "type status", data: []byte(`[{"status":"T","path":"a"}]`)},
		{name: "lowercase status", data: []byte(`[{"status":"m","path":"a"}]`)},
		{name: "null value pointers", data: []byte(`[{"status":"M","path":"a","valuePointers":null}]`)},
		{name: "object value pointers", data: []byte(`[{"status":"M","path":"a","valuePointers":{}}]`)},
		{name: "mixed value pointers", data: []byte(`[{"status":"M","path":"a","valuePointers":["/a",1]}]`)},
		{name: "null consumer domains", data: []byte(`[{"status":"M","path":"a","consumerDomains":null}]`)},
		{name: "object consumer domains", data: []byte(`[{"status":"M","path":"a","consumerDomains":{}}]`)},
		{name: "mixed semantic domains", data: []byte(`[{"status":"M","path":"a","semanticDomains":["backup",false]}]`)},
		{name: "scalar outside consumers", data: []byte(`[{"status":"M","path":"a","outsideConsumers":"binary"}]`)},
		{name: "nested outside consumer", data: []byte(`[{"status":"M","path":"a","outsideConsumers":[{}]}]`)},
		{name: "invalid UTF-8", data: invalidUTF8},
		{name: "isolated high surrogate", data: []byte(`[{"status":"M","path":"\uD800"}]`)},
		{name: "high surrogate then scalar", data: []byte(`[{"status":"M","path":"\uD800x"}]`)},
		{name: "high surrogate then non-low", data: []byte(`[{"status":"M","path":"\uD800\u0041"}]`)},
		{name: "isolated low surrogate", data: []byte(`[{"status":"M","path":"\uDC00"}]`)},
		{name: "surrogate in field name", data: []byte(`[{"status":"M","pa\uD800th":"a"}]`)},
		{name: "trailing value", data: []byte(`[] []`)},
		{name: "trailing garbage", data: []byte(`[] x`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if changes, err := DecodeChangedFilesJSON(bytes.NewReader(test.data)); err == nil {
				t.Fatalf("changes = %#v, want strict decode error", changes)
			}
		})
	}
}

func TestDecodeChangedFilesJSONRejectsUnsupportedStatusBeforeClassification(t *testing.T) {
	for _, status := range []string{"R100", "C100", "T", "X"} {
		t.Run(status, func(t *testing.T) {
			input := `[{"status":"` + status + `","path":"owned/path"}]`
			if _, err := DecodeChangedFilesJSON(strings.NewReader(input)); err == nil {
				t.Fatal("expected enriched status decode error")
			}
		})
	}
}

func TestInvalidEnrichedStatusMakesCLIExitOne(t *testing.T) {
	const privateStatusSentinel = "PRIVATE-STATUS-SENTINEL-3d7c91"
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	changedFiles := filepath.Join(t.TempDir(), "invalid-changed-files.json")
	if err := os.WriteFile(changedFiles, []byte(`[{"status":"`+privateStatusSentinel+`","path":"old"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "run", "./cmd/fugue-release-domain-plan",
		"--ownership", "deploy/release-domains/ownership-v1.yaml",
		"--changed-files", changedFiles,
		"--base-manifest", "not-read-base.yaml",
		"--target-manifest", "not-read-target.yaml",
		"--repeated-target-manifest", "not-read-repeat.yaml",
		"--base-digest", "base",
		"--target-digest", "target",
		"--live-digest", "live",
	)
	command.Dir = repositoryRoot
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("CLI succeeded, output = %s", output)
	}
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) || exitError.ExitCode() != 1 {
		t.Fatalf("CLI error = %v, output = %s", err, output)
	}
	if !strings.Contains(string(output), "fugue-release-domain-plan: plan construction failed\n") {
		t.Fatalf("CLI output = %s", output)
	}
	if strings.Contains(string(output), privateStatusSentinel) {
		t.Fatalf("private status sentinel leaked through CLI output: %s", output)
	}
}

func TestReservedBootstrapPathsCannotBeAuthorizedByPolicy(t *testing.T) {
	paths := []string{
		"deploy/release-domains/ownership-v1.yaml",
		"deploy/release-domains/ownership-v2.yaml",
		"cmd/fugue-release-domain-plan/main.go",
		"cmd/fugue-release-domain-evidence/main.go",
		"internal/releasedomain/planner.go",
		"scripts/release-domains/activate.sh",
		"scripts/lib/control_plane_release_domains.sh",
		"scripts/test_release_domain_safety.sh",
		"scripts/test_single_domain_release.sh",
		"scripts/upgrade_fugue_control_plane.sh",
		".github/workflows/deploy-control-plane.yml",
		".github/workflows/release-public-data-plane.yml",
		"Makefile",
	}
	for _, repositoryPath := range paths {
		for _, status := range []ChangeStatus{ChangeAdded, ChangeModified, ChangeDeleted} {
			t.Run(string(status)+"/"+strings.ReplaceAll(repositoryPath, "/", "_"), func(t *testing.T) {
				spec := &OwnershipSpec{
					FileRules:  []FileRule{{ID: "malicious-non-runtime", Exact: repositoryPath, NonRuntime: true}},
					ValueRules: []ValueRule{{ID: "malicious-domain", Exact: repositoryPath, Pointer: "", Domain: DomainNodeLocal}},
				}
				classification := ClassifyFiles([]ChangedFile{{
					Status: status, Path: repositoryPath, ValuePointers: []string{"/owned"},
				}}, spec)
				if classification.AllNonRuntime || len(classification.Domains) != 0 || len(classification.Evidence) != 0 || len(classification.Unknown) != 1 {
					t.Fatalf("classification = %#v", classification)
				}
				if classification.Unknown[0].Status != string(status) || !strings.Contains(classification.Unknown[0].Reason, "cannot authorize itself") && !strings.Contains(classification.Unknown[0].Reason, "reserved bootstrap") {
					t.Fatalf("unknown = %#v", classification.Unknown)
				}
			})
		}
	}
}

func TestPlannerImplementationCannotSelfAuthorize(t *testing.T) {
	const repositoryPath = "internal/releasedomain/additive_foundation.go"
	for _, status := range []ChangeStatus{ChangeAdded, ChangeModified, ChangeDeleted} {
		classification := ClassifyFiles([]ChangedFile{{Status: status, Path: repositoryPath}}, &OwnershipSpec{
			FileRules: []FileRule{{ID: "malicious-foundation", Exact: repositoryPath, NonRuntime: true}},
		})
		if classification.AllNonRuntime || len(classification.Domains) != 0 || len(classification.Unknown) != 1 {
			t.Fatalf("status %s classification = %#v", status, classification)
		}
		if !strings.Contains(classification.Unknown[0].Reason, "cannot authorize itself") {
			t.Fatalf("status %s unknown = %#v", status, classification.Unknown)
		}
	}
}

func TestParseNameStatusZRejectsRawRenameCopyAndTypeChanges(t *testing.T) {
	for _, input := range []string{
		"R100\x00old\x00new\x00",
		"C100\x00old\x00new\x00",
		"T\x00path\x00",
	} {
		if _, err := ParseNameStatusZ(strings.NewReader(input)); err == nil {
			t.Fatalf("input %q unexpectedly parsed", input)
		}
	}
}

func TestRealGitNoRenamesProducesDeleteAddEvidence(t *testing.T) {
	tests := []struct {
		name        string
		oldPath     string
		newPath     string
		spec        *OwnershipSpec
		wantDomains []Domain
		wantUnknown int
	}{
		{
			name:    "same domain is single",
			oldPath: "owned/node-local/old.yaml",
			newPath: "owned/node-local/new.yaml",
			spec: &OwnershipSpec{FileRules: []FileRule{{
				ID: "node-local", Prefix: "owned/node-local/", Domains: []Domain{DomainNodeLocal},
			}}},
			wantDomains: []Domain{DomainNodeLocal},
		},
		{
			name:    "cross domain is multiple",
			oldPath: "owned/node-local/object.yaml",
			newPath: "owned/dns/object.yaml",
			spec: &OwnershipSpec{FileRules: []FileRule{
				{ID: "node-local", Prefix: "owned/node-local/", Domains: []Domain{DomainNodeLocal}},
				{ID: "dns", Prefix: "owned/dns/", Domains: []Domain{DomainAuthoritativeDNS}},
			}},
			wantDomains: []Domain{DomainNodeLocal, DomainAuthoritativeDNS},
		},
		{
			name:    "reserved and unowned are unknown",
			oldPath: "deploy/release-domains/ownership-v1.yaml",
			newPath: "unowned/ownership-v1.yaml",
			spec: &OwnershipSpec{FileRules: []FileRule{{
				ID: "attempted-policy-owner", Exact: "deploy/release-domains/ownership-v1.yaml", Domains: []Domain{DomainNodeLocal},
			}}},
			wantUnknown: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changes := realGitNoRename(t, test.oldPath, test.newPath)
			assertDeleteAdd(t, changes)
			classification := ClassifyFiles(changes, test.spec)
			if !equalDomains(classification.Domains, test.wantDomains) || len(classification.Unknown) != test.wantUnknown {
				t.Fatalf("classification = %#v", classification)
			}
			if test.wantUnknown == 0 && len(classification.Evidence) != 2 {
				t.Fatalf("evidence = %#v, want D+A evidence", classification.Evidence)
			}
		})
	}
}

func realGitNoRename(t *testing.T, oldPath, newPath string) []ChangedFile {
	t.Helper()
	repository := t.TempDir()
	runGit(t, repository, "init", "--quiet")
	runGit(t, repository, "config", "user.name", "Release Domain Test")
	runGit(t, repository, "config", "user.email", "release-domain@example.invalid")
	oldAbsolute := filepath.Join(repository, filepath.FromSlash(oldPath))
	if err := os.MkdirAll(filepath.Dir(oldAbsolute), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oldAbsolute, []byte("same content\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "--", oldPath)
	runGit(t, repository, "commit", "--quiet", "-m", "base")
	newAbsolute := filepath.Join(repository, filepath.FromSlash(newPath))
	if err := os.MkdirAll(filepath.Dir(newAbsolute), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(oldAbsolute, newAbsolute); err != nil {
		t.Fatal(err)
	}
	// git diff intentionally excludes untracked paths. Mark only the destination
	// as intent-to-add so the real --no-renames wire format contains D+A without
	// staging content or permitting Git's rename detector to collapse the pair.
	runGit(t, repository, "add", "--intent-to-add", "--", newPath)
	output := runGit(t, repository, "diff", "--no-renames", "--name-status", "-z", "HEAD", "--")
	changes, err := ParseNameStatusZ(bytes.NewReader(output))
	if err != nil {
		t.Fatalf("parse real git --no-renames output: %v", err)
	}
	return changes
}

func runGit(t *testing.T, directory string, arguments ...string) []byte {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
	return output
}

func assertDeleteAdd(t *testing.T, changes []ChangedFile) {
	t.Helper()
	counts := map[ChangeStatus]int{}
	for _, change := range changes {
		counts[change.Status]++
	}
	if len(changes) != 2 || counts[ChangeDeleted] != 1 || counts[ChangeAdded] != 1 {
		t.Fatalf("changes = %#v, want one delete and one add", changes)
	}
}

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"fugue/internal/backuprelease"
	"fugue/internal/componentmanifest"
)

const (
	cliTestBaseCommit   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cliTestTargetCommit = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	cliTestImageDigest  = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func TestRunBuildsExactCandidateFromBoundedInputs(t *testing.T) {
	chart := chartDirectory(t)
	chartDigest, err := digestChartDirectory(chart)
	if err != nil {
		t.Fatalf("digest chart: %v", err)
	}
	request := validCLIRequest(t, chartDigest)
	manifest := renderCLIManifest(t, request)
	requestPath, manifestPath := writeCLIInputs(t, request, manifest)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run([]string{"--request", requestPath, "--manifest", manifestPath, "--chart", chart}, &stdout, &stderr); err != nil {
		t.Fatalf("run candidate planner: %v stderr=%q", err, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("successful planner wrote stderr: %q", stderr.String())
	}
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	decoder.DisallowUnknownFields()
	var candidate backuprelease.Candidate
	if err := decoder.Decode(&candidate); err != nil {
		t.Fatalf("decode candidate output: %v\n%s", err, stdout.String())
	}
	if err := backuprelease.VerifyCandidate(candidate); err != nil {
		t.Fatalf("verify candidate output: %v", err)
	}
	if candidate.ChartDigest != chartDigest || candidate.SourceCommit != cliTestTargetCommit ||
		candidate.WorkloadName != "fugue-backup-observer-app-database-0123456789abcdef" {
		t.Fatalf("candidate output drifted: %+v", candidate)
	}
}

func TestRunDigestChartIsDeterministicAndInert(t *testing.T) {
	chart := chartDirectory(t)
	first, err := digestChartDirectory(chart)
	if err != nil {
		t.Fatalf("first digest: %v", err)
	}
	second, err := digestChartDirectory(chart)
	if err != nil || second != first || !strings.HasPrefix(first, "sha256:") || len(first) != len("sha256:")+64 {
		t.Fatalf("chart digest is not deterministic: first=%q second=%q err=%v", first, second, err)
	}
	var stdout bytes.Buffer
	if err := run([]string{"--digest-chart", "--chart", chart}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("digest-only run: %v", err)
	}
	if strings.TrimSpace(stdout.String()) != first {
		t.Fatalf("digest-only output = %q, want %q", stdout.String(), first)
	}

	copyRoot := filepath.Join(t.TempDir(), "chart")
	copyDirectory(t, chart, copyRoot)
	copyDigest, err := digestChartDirectory(copyRoot)
	if err != nil || copyDigest != first {
		t.Fatalf("copied chart digest = %q, want %q err=%v", copyDigest, first, err)
	}
	valuesPath := filepath.Join(copyRoot, "values.yaml")
	file, err := os.OpenFile(valuesPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open copied values: %v", err)
	}
	if _, err := file.WriteString("\n# digest mutation\n"); err != nil {
		file.Close()
		t.Fatalf("mutate copied values: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close copied values: %v", err)
	}
	mutated, err := digestChartDirectory(copyRoot)
	if err != nil || mutated == first {
		t.Fatalf("chart content mutation was not detected: digest=%q err=%v", mutated, err)
	}
}

func TestRunRejectsAmbiguousRequestAndFilesystemInputs(t *testing.T) {
	chart := chartDirectory(t)
	chartDigest, err := digestChartDirectory(chart)
	if err != nil {
		t.Fatalf("digest chart: %v", err)
	}
	request := validCLIRequest(t, chartDigest)
	manifest := renderCLIManifest(t, request)
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	unknown := append(append([]byte(nil), encoded[:len(encoded)-1]...), []byte(`,"unexpected":true}`)...)
	duplicate := append([]byte(`{"apiVersion":"duplicate",`), encoded[1:]...)
	trailing := append(append([]byte(nil), encoded...), []byte(`{}`)...)
	for name, document := range map[string][]byte{
		"unknown field": unknown,
		"duplicate key": duplicate,
		"trailing data": trailing,
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			requestPath := filepath.Join(root, "request.json")
			manifestPath := filepath.Join(root, "manifest.yaml")
			writeFile(t, requestPath, document)
			writeFile(t, manifestPath, manifest)
			if err := run([]string{"--request", requestPath, "--manifest", manifestPath, "--chart", chart}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
				t.Fatal("ambiguous request was accepted")
			}
		})
	}

	t.Run("digest mismatch", func(t *testing.T) {
		mutated := request
		mutated.ChartDigest = cliTestImageDigest
		requestPath, manifestPath := writeCLIInputs(t, mutated, manifest)
		if err := run([]string{"--request", requestPath, "--manifest", manifestPath, "--chart", chart}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatal("mismatched chart digest was accepted")
		}
	})

	t.Run("hardlink alias", func(t *testing.T) {
		root := t.TempDir()
		requestPath := filepath.Join(root, "request.json")
		manifestPath := filepath.Join(root, "manifest.yaml")
		writeFile(t, requestPath, encoded)
		if err := os.Link(requestPath, manifestPath); err != nil {
			t.Skipf("hard links unavailable: %v", err)
		}
		if err := run([]string{"--request", requestPath, "--manifest", manifestPath, "--chart", chart}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatal("hardlinked request/manifest alias was accepted")
		}
	})

	t.Run("symlink request", func(t *testing.T) {
		root := t.TempDir()
		realRequest := filepath.Join(root, "real-request.json")
		requestPath := filepath.Join(root, "request.json")
		manifestPath := filepath.Join(root, "manifest.yaml")
		writeFile(t, realRequest, encoded)
		writeFile(t, manifestPath, manifest)
		if err := os.Symlink(realRequest, requestPath); err != nil {
			t.Fatalf("create request symlink: %v", err)
		}
		if err := run([]string{"--request", requestPath, "--manifest", manifestPath, "--chart", chart}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatal("symlink request was accepted")
		}
	})

	t.Run("request inside chart", func(t *testing.T) {
		root := t.TempDir()
		manifestPath := filepath.Join(root, "manifest.yaml")
		writeFile(t, manifestPath, manifest)
		if err := run([]string{"--request", filepath.Join(chart, "Chart.yaml"), "--manifest", manifestPath, "--chart", chart}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatal("request inside chart closure was accepted")
		}
	})

	t.Run("noncanonical path", func(t *testing.T) {
		requestPath, manifestPath := writeCLIInputs(t, request, manifest)
		separator := string(filepath.Separator)
		dirtyPath := filepath.Dir(requestPath) + separator + "sub" + separator + ".." + separator + filepath.Base(requestPath)
		if err := run([]string{"--request", dirtyPath, "--manifest", manifestPath, "--chart", chart}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatal("noncanonical input path was accepted")
		}
	})

	if err := run(nil, nil, &bytes.Buffer{}); err == nil {
		t.Fatal("nil output writer was accepted")
	}
}

func TestChartDigestRejectsSymlinksAndUnboundedTopology(t *testing.T) {
	root := filepath.Join(t.TempDir(), "chart")
	if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatalf("mkdir chart: %v", err)
	}
	writeFile(t, filepath.Join(root, "Chart.yaml"), []byte("apiVersion: v2\nname: test\nversion: 0.1.0\n"))
	if err := os.Symlink("Chart.yaml", filepath.Join(root, "alias.yaml")); err != nil {
		t.Fatalf("create chart symlink: %v", err)
	}
	if _, err := digestChartDirectory(root); err == nil {
		t.Fatal("chart symlink was accepted")
	}
	if err := os.Remove(filepath.Join(root, "alias.yaml")); err != nil {
		t.Fatalf("remove chart symlink: %v", err)
	}
	for index := 0; index < maxCandidateChartDirs; index++ {
		if err := os.Mkdir(filepath.Join(root, fmt.Sprintf("dir-%03d", index)), 0o750); err != nil {
			t.Fatalf("mkdir bounded topology %d: %v", index, err)
		}
	}
	if _, err := digestChartDirectory(root); err == nil {
		t.Fatal("chart with too many directories was accepted")
	}
	deep := []byte(strings.Repeat("[", maxCandidateJSONDepth+2) + "0" + strings.Repeat("]", maxCandidateJSONDepth+2))
	if err := rejectDuplicateJSONKeys(deep); err == nil {
		t.Fatal("overly deep request JSON was accepted")
	}
}

func TestBackupReleasePlannerDependencyBoundary(t *testing.T) {
	command := exec.Command("go", "list", "-deps", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list planner dependencies: %v", err)
	}
	var local []string
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{
			"fugue/internal/api", "fugue/internal/auth", "fugue/internal/controller", "fugue/internal/model",
			"fugue/internal/releasecontrol", "fugue/internal/store", "database/sql", "k8s.io/client-go", "helm.sh/", "os/exec",
		} {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden+"/") {
				t.Fatalf("planner crossed mutation boundary through %q", dependency)
			}
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/cmd/fugue-backup-release-plan",
		"fugue/internal/backuprelease",
		"fugue/internal/componentmanifest",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("planner local dependency closure = %v, want %v", local, want)
	}
}

func validCLIRequest(t *testing.T, chartDigest string) backuprelease.CandidateRequest {
	t.Helper()
	manifestFile, err := os.Open("../../docs/architecture/component-ownership-v1.yaml")
	if err != nil {
		t.Fatalf("open ownership manifest: %v", err)
	}
	defer manifestFile.Close()
	manifest, err := componentmanifest.Load(manifestFile)
	if err != nil {
		t.Fatalf("load ownership manifest: %v", err)
	}
	plan, err := componentmanifest.PlanChanges(manifest, []string{"deploy/helm/fugue-backup-observer/templates/deployment.yaml"})
	if err != nil {
		t.Fatalf("plan backup chart: %v", err)
	}
	coordination, err := componentmanifest.BuildShadowCoordinationPlan(plan)
	if err != nil {
		t.Fatalf("build coordination: %v", err)
	}
	envelope, err := componentmanifest.BuildShadowArtifactEnvelope(manifest, plan, coordination, cliTestBaseCommit, cliTestTargetCommit)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	identity, err := envelope.ArtifactIdentity()
	if err != nil {
		t.Fatalf("artifact identity: %v", err)
	}
	content, err := envelope.Content()
	if err != nil {
		t.Fatalf("artifact content: %v", err)
	}
	contentBytes, err := json.Marshal(content)
	if err != nil {
		t.Fatalf("marshal artifact content: %v", err)
	}
	contentHash := sha256.Sum256(contentBytes)
	status := backuprelease.ComponentPlanStatusV1{
		APIVersion:                backuprelease.ComponentPlanStatusAPIVersionV1,
		Kind:                      backuprelease.ComponentPlanStatusKindV1,
		Policy:                    backuprelease.ComponentPlanStatusPolicyV1,
		State:                     "observed",
		ArtifactID:                "component-release-plan-artifact",
		ContentHash:               "sha256:" + hex.EncodeToString(contentHash[:]),
		ScopeKey:                  identity.ScopeKey,
		Generation:                identity.Generation,
		PlanDigest:                plan.PlanDigest,
		CoordinationDigest:        coordination.CoordinationDigest,
		ReleaseID:                 "release-observation-1",
		LaneKey:                   "component_release_plan|" + identity.ScopeKey + "|shadow",
		FencingToken:              11,
		LaneVersion:               4,
		IdempotencyKey:            coordination.IdempotencyKey,
		ObservationOnly:           true,
		ProductionMutationAllowed: false,
	}
	status.Digest = backuprelease.DigestComponentPlanStatusV1(status)
	return backuprelease.CandidateRequest{
		APIVersion:            backuprelease.CandidateRequestAPIVersion,
		Kind:                  backuprelease.CandidateRequestKind,
		SourceCommit:          cliTestTargetCommit,
		CellKey:               "backup/app-database/0123456789abcdef",
		ReleaseName:           "backup-app-database-0123456789abcdef",
		ReleaseNamespace:      "fugue-system",
		ImageRepository:       "registry.example.test/fugue/fugue-backup-observer",
		ImageDigest:           cliTestImageDigest,
		ChartDigest:           chartDigest,
		APIBaseURL:            "https://api.fugue-system.svc.cluster.local:8443",
		SpecConfigMapName:     "backup-app-database-spec",
		SpecConfigMapKey:      "desired.json",
		TokenSecretName:       "backup-app-database-token",
		TokenSecretKey:        "observer-token",
		LKGClaimName:          "fugue-backup-observer-app-database-0123456789abcdef-lkg",
		ReconcileInterval:     "30s",
		AttemptTimeout:        "20s",
		RequestTimeout:        "10s",
		ShutdownTimeout:       "10s",
		MaxResponseBytes:      65536,
		ComponentPlanEnvelope: envelope,
		ComponentPlanStatus:   status,
	}
}

func renderCLIManifest(t *testing.T, request backuprelease.CandidateRequest) []byte {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skipf("helm is required: %v", err)
	}
	command := exec.Command(
		"helm", "template", request.ReleaseName, chartDirectory(t),
		"--namespace", request.ReleaseNamespace,
		"--set", "enabled=true",
		"--set-string", "image.repository="+request.ImageRepository,
		"--set-string", "image.digest="+request.ImageDigest,
		"--set-string", "api.baseURL="+request.APIBaseURL,
		"--set-string", "cell.key="+request.CellKey,
		"--set-string", "spec.existingConfigMap.name="+request.SpecConfigMapName,
		"--set-string", "spec.existingConfigMap.key="+request.SpecConfigMapKey,
		"--set-string", "token.existingSecret.name="+request.TokenSecretName,
		"--set-string", "token.existingSecret.key="+request.TokenSecretKey,
		"--set-string", "lkg.existingClaim.name="+request.LKGClaimName,
	)
	manifest, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("render chart: %v\n%s", err, manifest)
	}
	return manifest
}

func writeCLIInputs(t *testing.T, request backuprelease.CandidateRequest, manifest []byte) (string, string) {
	t.Helper()
	root := t.TempDir()
	requestPath := filepath.Join(root, "request.json")
	manifestPath := filepath.Join(root, "manifest.yaml")
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal candidate request: %v", err)
	}
	writeFile(t, requestPath, encoded)
	writeFile(t, manifestPath, manifest)
	return requestPath, manifestPath
}

func chartDirectory(t *testing.T) string {
	t.Helper()
	chart, err := filepath.Abs("../../deploy/helm/fugue-backup-observer")
	if err != nil {
		t.Fatalf("absolute chart path: %v", err)
	}
	return filepath.Clean(chart)
}

func copyDirectory(t *testing.T, source, destination string) {
	t.Helper()
	err := filepath.WalkDir(source, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, current)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		data, err := os.ReadFile(current)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o640)
	})
	if err != nil {
		t.Fatalf("copy chart directory: %v", err)
	}
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

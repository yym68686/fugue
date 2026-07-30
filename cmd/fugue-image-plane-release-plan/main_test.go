package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const imagePlaneCandidateWorkflowPath = "../../.github/workflows/validate-image-plane-release-candidate.yml"

type candidateWorkflowStep struct {
	Name string `yaml:"name"`
	Uses string `yaml:"uses"`
	Run  string `yaml:"run"`
}

func TestRunRejectsAmbiguousOrUnknownInput(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run([]string{"--request", "request.json", "--manifest", "manifest.yaml", "--chart", "chart"}, &stdout, &stderr); err == nil {
		t.Fatal("relative input paths were accepted")
	}
	directory := t.TempDir()
	requestPath := filepath.Join(directory, "request.json")
	manifestPath := filepath.Join(directory, "manifest.yaml")
	chartPath := filepath.Join(directory, "chart")
	if err := os.WriteFile(requestPath, []byte("{\"unexpected\":true}\n"), 0o600); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if err := os.WriteFile(manifestPath, []byte("not-a-manifest\n"), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	if err := os.Mkdir(chartPath, 0o700); err != nil {
		t.Fatalf("create chart: %v", err)
	}
	if err := os.WriteFile(filepath.Join(chartPath, "Chart.yaml"), []byte("name: test\n"), 0o600); err != nil {
		t.Fatalf("write chart: %v", err)
	}
	stdout.Reset()
	stderr.Reset()
	if err := run([]string{"--request", requestPath, "--manifest", manifestPath, "--chart", chartPath}, &stdout, &stderr); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown request field error=%v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("invalid input produced candidate output: %s", stdout.String())
	}
}

func TestDigestChartDirectoryIsDeterministicAndTamperEvident(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "templates"), 0o700); err != nil {
		t.Fatalf("create templates: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "values.yaml"), []byte("enabled: false\n"), 0o600); err != nil {
		t.Fatalf("write values: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "templates", "daemonset.yaml"), []byte("{{- if .Values.enabled }}\n{{- end }}\n"), 0o600); err != nil {
		t.Fatalf("write template: %v", err)
	}
	first, err := digestChartDirectory(root)
	if err != nil {
		t.Fatalf("digest chart: %v", err)
	}
	second, err := digestChartDirectory(root)
	if err != nil || first != second {
		t.Fatalf("chart digest is not deterministic: first=%s second=%s err=%v", first, second, err)
	}
	if err := os.WriteFile(filepath.Join(root, "values.yaml"), []byte("enabled: true\n"), 0o600); err != nil {
		t.Fatalf("mutate values: %v", err)
	}
	mutated, err := digestChartDirectory(root)
	if err != nil {
		t.Fatalf("digest mutated chart: %v", err)
	}
	if mutated == first {
		t.Fatal("chart content mutation retained the same digest")
	}
	var stdout bytes.Buffer
	if err := run([]string{"--digest-chart", "--chart", root}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("print chart digest: %v", err)
	}
	if strings.TrimSpace(stdout.String()) != mutated {
		t.Fatalf("printed chart digest=%q, want %q", stdout.String(), mutated)
	}
	if err := os.Symlink(filepath.Join(root, "values.yaml"), filepath.Join(root, "templates", "values-link")); err != nil {
		t.Fatalf("create chart symlink: %v", err)
	}
	if _, err := digestChartDirectory(root); err == nil {
		t.Fatal("chart symlink was accepted")
	}
}

func TestReadBoundedRegularFileRejectsSymlinkAndEmptyFile(t *testing.T) {
	directory := t.TempDir()
	empty := filepath.Join(directory, "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatalf("write empty file: %v", err)
	}
	if _, err := readBoundedRegularFile(empty, 32); err == nil {
		t.Fatal("empty input was accepted")
	}
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write target: %v", err)
	}
	link := filepath.Join(directory, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	if _, err := readBoundedRegularFile(link, 32); err == nil {
		t.Fatal("symlink input was accepted")
	}
}

func TestImagePlaneReleasePlannerDependencyAndCapabilityBoundary(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", "./cmd/fugue-image-plane-release-plan")
	command.Dir = "../.."
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("list planner dependencies: %v\n%s", err, output)
	}
	dependencies := strings.Fields(string(output))
	for _, dependency := range dependencies {
		for _, forbidden := range []string{
			"database/sql",
			"fugue/internal/api",
			"fugue/internal/releasecontrol",
			"fugue/internal/store",
			"github.com/google/go-containerregistry",
			"github.com/jackc/pgx",
			"helm.sh",
			"k8s.io/client-go",
		} {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden+"/") {
				t.Fatalf("planner dependency closure contains forbidden capability %q", dependency)
			}
		}
	}
	var local []string
	for _, dependency := range dependencies {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
	}
	sort.Strings(local)
	wantLocal := []string{
		"fugue/cmd/fugue-image-plane-release-plan",
		"fugue/internal/componentmanifest",
		"fugue/internal/imageplanerelease",
	}
	if !reflect.DeepEqual(local, wantLocal) {
		t.Fatalf("planner local dependencies=%v, want %v", local, wantLocal)
	}

	source, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read planner source: %v", err)
	}
	for _, forbidden := range []string{
		"exec.Command",
		"workflow_dispatch",
		"kubectl",
		"helm upgrade",
		"helm install",
		"docker push",
		"packages: write",
	} {
		if strings.Contains(string(source), forbidden) {
			t.Fatalf("planner source contains forbidden execution capability %q", forbidden)
		}
	}
}

func TestImagePlaneCandidateWorkflowIsIndependentValidationOnly(t *testing.T) {
	data, err := os.ReadFile(imagePlaneCandidateWorkflowPath)
	if err != nil {
		t.Fatalf("read candidate workflow: %v", err)
	}
	var workflow struct {
		Name        string            `yaml:"name"`
		Permissions map[string]string `yaml:"permissions"`
		On          struct {
			Push struct {
				Branches []string `yaml:"branches"`
				Paths    []string `yaml:"paths"`
			} `yaml:"push"`
			PullRequest struct {
				Paths []string `yaml:"paths"`
			} `yaml:"pull_request"`
		} `yaml:"on"`
		Concurrency struct {
			Group            string `yaml:"group"`
			CancelInProgress bool   `yaml:"cancel-in-progress"`
		} `yaml:"concurrency"`
		Jobs map[string]struct {
			RunsOn      string                  `yaml:"runs-on"`
			Timeout     int                     `yaml:"timeout-minutes"`
			Permissions map[string]string       `yaml:"permissions"`
			Steps       []candidateWorkflowStep `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatalf("parse candidate workflow: %v", err)
	}
	if workflow.Name != "validate image-plane release candidate" ||
		!reflect.DeepEqual(workflow.Permissions, map[string]string{"contents": "read"}) ||
		!reflect.DeepEqual(workflow.On.Push.Branches, []string{"main"}) || len(workflow.On.Push.Paths) == 0 ||
		!reflect.DeepEqual(workflow.On.Push.Paths, workflow.On.PullRequest.Paths) ||
		workflow.Concurrency.Group != "image-plane-candidate-${{ github.ref }}" || !workflow.Concurrency.CancelInProgress ||
		len(workflow.Jobs) != 1 {
		t.Fatalf("candidate workflow boundary drifted: %+v", workflow)
	}
	job, ok := workflow.Jobs["validate-only"]
	if !ok || job.RunsOn != "ubuntu-latest" || job.Timeout != 10 || len(job.Permissions) != 0 || len(job.Steps) == 0 {
		t.Fatalf("candidate validation-only job drifted: %+v", workflow.Jobs)
	}
	raw := string(data)
	for _, required := range []string{
		"cmd/fugue-image-plane-release-plan/**",
		"internal/imageplanerelease/**",
		"internal/componentmanifest/**",
		"deploy/helm/fugue-image-plane/**",
		"docs/architecture/component-ownership-v1.yaml",
		"go test ./cmd/fugue-image-plane-release-plan ./internal/imageplanerelease ./internal/componentmanifest ./deploy/helm/fugue-image-plane",
		"go build -trimpath -buildvcs=false -o /tmp/fugue-image-plane-release-plan",
		"--digest-chart",
		"helm lint deploy/helm/fugue-image-plane",
		"helm template image-plane-shadow deploy/helm/fugue-image-plane",
		"actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
		"actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16",
		"azure/setup-helm@9bc31f4ebc9c6b171d7bfbaa5d006ae7abdb4310",
	} {
		if !strings.Contains(raw, required) {
			t.Fatalf("candidate workflow is missing boundary %q", required)
		}
	}
	for _, forbidden := range []string{
		"packages: write",
		"actions/upload-artifact@",
		"docker/login-action@",
		"--push",
		"push: true",
		"ghcr.io/",
		"environment: production",
		"workflow_dispatch",
		"self-hosted",
		"kubectl ",
		"helm install",
		"helm upgrade",
		"helm uninstall",
		"helm push",
		"helm package",
		"docker build",
	} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("candidate validation workflow contains forbidden capability %q", forbidden)
		}
	}
	for _, step := range job.Steps {
		if step.Uses != "" && !regexp.MustCompile(`^[^@]+@[0-9a-f]{40}$`).MatchString(step.Uses) {
			t.Fatalf("candidate workflow action %q is not pinned to a full commit SHA", step.Uses)
		}
		if step.Run == "" {
			continue
		}
		command := exec.Command("bash", "-n")
		command.Stdin = strings.NewReader(step.Run)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("candidate workflow step %q has invalid bash: %v output=%q", step.Name, err, output)
		}
	}

	agentWorkflow, err := os.ReadFile("../../.github/workflows/build-image-plane-agent-image.yml")
	if err != nil {
		t.Fatalf("read agent workflow: %v", err)
	}
	for _, candidatePath := range []string{"cmd/fugue-image-plane-release-plan", "internal/imageplanerelease"} {
		if strings.Contains(string(agentWorkflow), candidatePath) {
			t.Fatalf("agent image workflow still owns candidate path %q", candidatePath)
		}
	}
}

package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/releaseterminal"
	"gopkg.in/yaml.v3"
)

const (
	testRepository = "fugue-test/repository"
	testToken      = "reader-token"
	testObjectOID  = "1111111111111111111111111111111111111111"
	testTreeOID    = "2222222222222222222222222222222222222222"
	testParentOID  = "3333333333333333333333333333333333333333"
)

type carrierFixture struct {
	payload        []byte
	refSequence    []string
	parents        []string
	treeTruncated  bool
	invalidBase64  bool
	blobSizeOffset int64
	refStatus      int
	oversizedRef   bool
	nullRefList    bool
	omitParents    bool
	nullParents    bool
	refReads       atomic.Int32
}

func TestGitHubReaderReturnsStableAbsentEvidence(t *testing.T) {
	fixture := &carrierFixture{refSequence: []string{"", ""}}
	server := httptest.NewServer(fixture.handler(t))
	defer server.Close()
	reader := testReader(t, server.URL)

	result, err := reader.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	want := readResult{SchemaVersion: 1, Ref: terminalRef, State: "absent"}
	if !reflect.DeepEqual(result, want) {
		t.Fatalf("Read() = %#v, want %#v", result, want)
	}
	encoded, err := encodeResult(result)
	if err != nil {
		t.Fatalf("encodeResult() error = %v", err)
	}
	if string(encoded) != "{\"schema_version\":1,\"ref\":\"refs/heads/fugue-control-plane-release-terminal-state\",\"state\":\"absent\"}\n" {
		t.Fatalf("encodeResult() = %q", encoded)
	}
	if fixture.refReads.Load() != 2 {
		t.Fatalf("terminal ref reads = %d, want 2", fixture.refReads.Load())
	}
}

func TestGitHubReaderResolvesStableImmutableCarrier(t *testing.T) {
	document := testReservation()
	payload, err := releaseterminal.Encode(document)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &carrierFixture{payload: payload, refSequence: []string{testObjectOID, testObjectOID}}
	server := httptest.NewServer(fixture.handler(t))
	defer server.Close()
	reader := testReader(t, server.URL)

	result, err := reader.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if result.State != "present" || result.ObjectOID != testObjectOID || result.Document == nil || *result.Document != document {
		t.Fatalf("Read() = %#v, want present canonical document", result)
	}
	first, err := encodeResult(result)
	if err != nil {
		t.Fatal(err)
	}
	second, err := encodeResult(result)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) || !bytes.HasSuffix(first, []byte{'\n'}) || bytes.HasSuffix(first, []byte("\n\n")) {
		t.Fatal("present result encoding is not deterministic one-LF JSON")
	}
	if fixture.refReads.Load() != 2 {
		t.Fatalf("terminal ref reads = %d, want 2", fixture.refReads.Load())
	}
}

func TestGitHubReaderRejectsRefRacesAndMalformedRemoteState(t *testing.T) {
	payload, err := releaseterminal.Encode(testReservation())
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]*carrierFixture{
		"ref appears": {
			refSequence: []string{"", testObjectOID},
		},
		"ref changes": {
			payload: payload, refSequence: []string{testObjectOID, strings.Repeat("4", 40)},
		},
		"physical parent mismatch": {
			payload: payload, refSequence: []string{testObjectOID, testObjectOID}, parents: []string{testParentOID},
		},
		"truncated tree": {
			payload: payload, refSequence: []string{testObjectOID}, treeTruncated: true,
		},
		"invalid base64": {
			payload: payload, refSequence: []string{testObjectOID}, invalidBase64: true,
		},
		"blob size mismatch": {
			payload: payload, refSequence: []string{testObjectOID}, blobSizeOffset: 1,
		},
		"API status": {
			refStatus: http.StatusForbidden,
		},
		"oversized response": {
			oversizedRef: true,
		},
		"null ref list": {
			nullRefList: true,
		},
		"missing parents": {
			payload: payload, refSequence: []string{testObjectOID}, omitParents: true,
		},
		"null parents": {
			payload: payload, refSequence: []string{testObjectOID}, nullParents: true,
		},
	}
	for name, fixture := range tests {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(fixture.handler(t))
			defer server.Close()
			if _, err := testReader(t, server.URL).Read(context.Background()); err == nil {
				t.Fatal("Read() unexpectedly accepted unsafe remote state")
			}
		})
	}
}

func TestGitHubReaderRejectsRedirects(t *testing.T) {
	var followed atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		followed.Store(true)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, target.URL, http.StatusFound)
	}))
	defer source.Close()

	if _, err := testReader(t, source.URL).Read(context.Background()); err == nil {
		t.Fatal("Read() unexpectedly followed a redirect")
	}
	if followed.Load() {
		t.Fatal("redirect target was contacted")
	}
}

func TestParseOptionsAndResultValidation(t *testing.T) {
	if got, err := parseOptions([]string{"--repository", testRepository}); err != nil || got.repository != testRepository {
		t.Fatalf("parseOptions() = %#v, %v", got, err)
	}
	for _, arguments := range [][]string{
		nil,
		{"--repository", "owner"},
		{"--repository", "../owner/repo"},
		{"--repository", ".owner/repo"},
		{"--repository", "owner/repo."},
		{"--repository", testRepository, "extra"},
	} {
		if _, err := parseOptions(arguments); err == nil {
			t.Fatalf("parseOptions(%q) unexpectedly succeeded", arguments)
		}
	}

	invalidResults := []readResult{
		{},
		{SchemaVersion: 1, Ref: terminalRef, State: "absent", ObjectOID: testObjectOID},
		{SchemaVersion: 1, Ref: terminalRef, State: "present"},
		{SchemaVersion: 1, Ref: terminalRef, State: "unknown"},
	}
	for _, result := range invalidResults {
		if _, err := encodeResult(result); err == nil {
			t.Fatalf("encodeResult(%#v) unexpectedly succeeded", result)
		}
	}
}

func TestWorkflowIsManualGitHubHostedAndReadOnly(t *testing.T) {
	path := filepath.Join("..", "..", ".github", "workflows", "read-control-plane-release-terminal-rp1.yml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var workflow workflowDocument
	if err := yaml.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	if workflow.Name != "read-control-plane-release-terminal-rp1" || len(workflow.On) != 1 {
		t.Fatalf("workflow identity/triggers drifted: %#v", workflow.On)
	}
	dispatch, ok := workflow.On["workflow_dispatch"]
	if !ok || len(dispatch.Inputs) != 1 {
		t.Fatalf("workflow_dispatch inputs drifted: %#v", dispatch.Inputs)
	}
	expected := dispatch.Inputs["expected_sha"]
	if !expected.Required || expected.Type != "string" || len(workflow.Permissions) != 0 {
		t.Fatal("workflow authorization contract drifted")
	}
	if workflow.Concurrency.Group != "fugue-release-policy-rp1-terminal-reader-v1" || workflow.Concurrency.CancelInProgress {
		t.Fatal("workflow concurrency contract drifted")
	}
	if len(workflow.Jobs) != 1 {
		t.Fatalf("workflow job inventory = %d, want 1", len(workflow.Jobs))
	}
	job := workflow.Jobs["read-terminal-state"]
	if job.RunsOn != "ubuntu-latest" || job.TimeoutMinutes != 10 || job.Environment != "production" ||
		!reflect.DeepEqual(job.Permissions, map[string]string{"contents": "read"}) {
		t.Fatalf("reader job boundary drifted: %#v", job)
	}
	if len(job.Steps) != 4 {
		t.Fatalf("reader step inventory = %d, want 4", len(job.Steps))
	}
	checkout, setup, guard, read := job.Steps[0], job.Steps[1], job.Steps[2], job.Steps[3]
	if checkout.Name != "Checkout exact RP1 reader without persisted credentials" ||
		setup.Name != "Setup Go" || guard.Name != "Verify exact read-only authorization" ||
		read.Name != "Read stable terminal ref without mutation" || read.ID != "read" {
		t.Fatal("reader step identity or order drifted")
	}
	if checkout.Uses != "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" ||
		checkout.With["ref"] != "${{ github.sha }}" || checkout.With["fetch-depth"] != 1 || checkout.With["persist-credentials"] != false {
		t.Fatalf("checkout contract drifted: %#v", checkout)
	}
	if setup.Uses != "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16" ||
		setup.With["go-version-file"] != "go.mod" || setup.With["cache"] != true {
		t.Fatalf("setup-go contract drifted: %#v", setup)
	}
	if guard.Env["EXPECTED_SHA"] != "${{ inputs.expected_sha }}" || guard.Env["GH_TOKEN"] != "${{ github.token }}" ||
		read.Env["GITHUB_TOKEN"] != "${{ github.token }}" {
		t.Fatal("reader workflow token or expected-SHA binding drifted")
	}
	for _, fragment := range []string{
		"GITHUB_EVENT_NAME", "refs/heads/main", "GITHUB_RUN_ATTEMPT", "EXPECTED_SHA", "GITHUB_SHA",
		"git/ref/heads/main", "cmd/fugue-release-terminal-read/main.go", "internal/releaseterminal/resolver.go",
	} {
		if !strings.Contains(guard.Run, fragment) {
			t.Fatalf("authorization guard is missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"go run ./cmd/fugue-release-terminal-read", "--repository \"${GITHUB_REPOSITORY}\"",
		"terminal-state.json", "sha256sum", "GITHUB_STEP_SUMMARY",
	} {
		if !strings.Contains(read.Run, fragment) {
			t.Fatalf("reader step is missing %q", fragment)
		}
	}
	workflowText := string(data)
	for _, forbidden := range []string{
		"self-hosted", "--method", "graphql", "updateRefs", "kubectl", "helm ", "ssh ",
		"actions/upload-artifact", "contents: write", "actions: write", "id-token:", "secrets.",
		"pull_request:", "push:", "schedule:",
	} {
		if strings.Contains(workflowText, forbidden) {
			t.Fatalf("reader workflow contains out-of-scope capability %q", forbidden)
		}
	}
	if githubAPIBaseURL != "https://api.github.com" {
		t.Fatalf("executable GitHub API origin drifted to %q", githubAPIBaseURL)
	}
}

func (fixture *carrierFixture) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", request.Method)
			response.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if request.Header.Get("Authorization") != "Bearer "+testToken ||
			request.Header.Get("Accept") != "application/vnd.github+json" ||
			request.Header.Get("X-GitHub-Api-Version") != "2022-11-28" {
			t.Error("GitHub request headers drifted")
			response.WriteHeader(http.StatusUnauthorized)
			return
		}
		prefix := "/repos/" + testRepository + "/"
		if !strings.HasPrefix(request.URL.Path, prefix) {
			t.Errorf("path = %q, want repository prefix", request.URL.Path)
			response.WriteHeader(http.StatusNotFound)
			return
		}
		endpoint := strings.TrimPrefix(request.URL.Path, prefix)
		switch endpoint {
		case terminalMatchingRef:
			fixture.serveRef(response)
		case "git/commits/" + testObjectOID:
			if fixture.omitParents {
				writeJSON(t, response, map[string]any{
					"sha": testObjectOID, "tree": gitObject{SHA: testTreeOID},
				})
				return
			}
			if fixture.nullParents {
				writeJSON(t, response, map[string]any{
					"sha": testObjectOID, "tree": gitObject{SHA: testTreeOID}, "parents": nil,
				})
				return
			}
			parents := make([]gitObject, len(fixture.parents))
			for index, parent := range fixture.parents {
				parents[index] = gitObject{SHA: parent}
			}
			writeJSON(t, response, map[string]any{
				"sha": testObjectOID, "tree": gitObject{SHA: testTreeOID}, "parents": parents,
			})
		case "git/trees/" + testTreeOID:
			truncated := fixture.treeTruncated
			writeJSON(t, response, gitTree{
				SHA: testTreeOID, Truncated: &truncated,
				Tree: []gitTreeEntry{{Path: releaseterminal.CarrierPayloadPath, Mode: "100644", Type: "blob", SHA: gitBlobOID(fixture.payload)}},
			})
		case "git/blobs/" + gitBlobOID(fixture.payload):
			content := base64.StdEncoding.EncodeToString(fixture.payload)
			if fixture.invalidBase64 {
				content = "not base64!"
			}
			size := int64(len(fixture.payload)) + fixture.blobSizeOffset
			writeJSON(t, response, gitBlob{SHA: gitBlobOID(fixture.payload), Encoding: "base64", Content: content, Size: &size})
		default:
			t.Errorf("unexpected GitHub API endpoint %q", endpoint)
			response.WriteHeader(http.StatusNotFound)
		}
	})
}

func (fixture *carrierFixture) serveRef(response http.ResponseWriter) {
	if fixture.refStatus != 0 {
		response.WriteHeader(fixture.refStatus)
		return
	}
	if fixture.oversizedRef {
		_, _ = response.Write(bytes.Repeat([]byte{'x'}, maxAPIResponseBytes+1))
		return
	}
	if fixture.nullRefList {
		_, _ = response.Write([]byte("null\n"))
		return
	}
	read := int(fixture.refReads.Add(1)) - 1
	oid := ""
	if read < len(fixture.refSequence) {
		oid = fixture.refSequence[read]
	} else if len(fixture.refSequence) > 0 {
		oid = fixture.refSequence[len(fixture.refSequence)-1]
	}
	refs := []gitRef{{Ref: terminalRef + "-prefix", Object: gitObject{SHA: testParentOID, Type: "commit"}}}
	if oid != "" {
		refs = append(refs, gitRef{Ref: terminalRef, Object: gitObject{SHA: oid, Type: "commit"}})
	}
	_ = json.NewEncoder(response).Encode(refs)
}

func testReader(t *testing.T, baseURL string) *githubReader {
	t.Helper()
	client := &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	reader, err := newGitHubReader(baseURL, testRepository, testToken, client)
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func testReservation() releaseterminal.Document {
	return releaseterminal.Document{
		SchemaVersion:            releaseterminal.SchemaVersion,
		CertificateKind:          releaseterminal.CertificateKindReservation,
		TerminalMode:             releaseterminal.ModeReservation,
		SourceRunID:              "101",
		SourceRunAttempt:         1,
		SourceHeadSHA:            strings.Repeat("a", 40),
		SourceWorkflow:           releaseterminal.WorkflowDeployV2,
		SourceConclusion:         "in_progress",
		PreviousTerminalStateOID: releaseterminal.AbsentOID,
	}
}

func gitBlobOID(payload []byte) string {
	header := []byte(fmt.Sprintf("blob %d%c", len(payload), byte(0)))
	digest := sha1.Sum(append(header, payload...)) // #nosec G401 -- Git object fixture
	return fmt.Sprintf("%x", digest)
}

func writeJSON(t *testing.T, response http.ResponseWriter, value any) {
	t.Helper()
	response.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(response).Encode(value); err != nil {
		t.Errorf("encode response: %v", err)
	}
}

type workflowDocument struct {
	Name        string                      `yaml:"name"`
	On          map[string]workflowDispatch `yaml:"on"`
	Permissions map[string]string           `yaml:"permissions"`
	Concurrency struct {
		Group            string `yaml:"group"`
		CancelInProgress bool   `yaml:"cancel-in-progress"`
	} `yaml:"concurrency"`
	Jobs map[string]workflowJob `yaml:"jobs"`
}

type workflowDispatch struct {
	Inputs map[string]workflowInput `yaml:"inputs"`
}

type workflowInput struct {
	Required bool   `yaml:"required"`
	Type     string `yaml:"type"`
}

type workflowJob struct {
	RunsOn         string            `yaml:"runs-on"`
	TimeoutMinutes int               `yaml:"timeout-minutes"`
	Environment    string            `yaml:"environment"`
	Permissions    map[string]string `yaml:"permissions"`
	Steps          []workflowStep    `yaml:"steps"`
}

type workflowStep struct {
	Name string         `yaml:"name"`
	ID   string         `yaml:"id"`
	Uses string         `yaml:"uses"`
	With map[string]any `yaml:"with"`
	Env  map[string]any `yaml:"env"`
	Run  string         `yaml:"run"`
}

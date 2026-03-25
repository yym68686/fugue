package api

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

type fakeWorkspacePodLister struct {
	pods []kubePodInfo
	err  error
}

func (f fakeWorkspacePodLister) listPodsBySelector(_ context.Context, _, _ string) ([]kubePodInfo, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make([]kubePodInfo, len(f.pods))
	copy(out, f.pods)
	return out, nil
}

type fakeWorkspaceExecCall struct {
	namespace string
	podName   string
	container string
	stdin     []byte
	command   []string
}

type fakeWorkspaceExecRunner struct {
	outputs [][]byte
	errs    []error
	calls   []fakeWorkspaceExecCall
}

func (f *fakeWorkspaceExecRunner) Run(_ context.Context, namespace, podName, containerName string, stdin []byte, command ...string) ([]byte, error) {
	call := fakeWorkspaceExecCall{
		namespace: namespace,
		podName:   podName,
		container: containerName,
		stdin:     append([]byte(nil), stdin...),
		command:   append([]string(nil), command...),
	}
	f.calls = append(f.calls, call)

	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return nil, err
	}
	if len(f.outputs) == 0 {
		return nil, errors.New("unexpected workspace exec call")
	}
	out := f.outputs[0]
	f.outputs = f.outputs[1:]
	return append([]byte(nil), out...), nil
}

func TestAppFilesystemTreeAndReadUseWorkspaceSidecar(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppFilesystemTestServer(t)

	pod := kubePodInfo{}
	pod.Metadata.Name = "demo-pod"
	pod.Metadata.CreationTimestamp = time.Now().UTC()
	pod.Status.Phase = "Running"

	server.newWorkspacePodLister = func(namespace string) (workspacePodLister, error) {
		if namespace == "" {
			t.Fatal("expected namespace")
		}
		return fakeWorkspacePodLister{pods: []kubePodInfo{pod}}, nil
	}

	treeOutput := strings.Join([]string{
		encodeFilesystemTreeLine("dir", "/workspace/dir", "dir", 0, 0o755, time.Unix(1700000000, 0).UTC(), true),
		encodeFilesystemTreeLine("file.txt", "/workspace/file.txt", "file", 5, 0o644, time.Unix(1700000001, 0).UTC(), false),
	}, "\n")
	runner := &fakeWorkspaceExecRunner{
		outputs: [][]byte{
			[]byte(treeOutput),
			[]byte("5\t644\t1700000001\nhello"),
		},
	}
	server.workspaceExecRunner = runner

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/filesystem/tree?path=/workspace", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var treeResponse struct {
		Pod     string               `json:"pod"`
		Path    string               `json:"path"`
		Entries []appFilesystemEntry `json:"entries"`
	}
	mustDecodeJSON(t, recorder, &treeResponse)
	if treeResponse.Pod != "demo-pod" {
		t.Fatalf("expected pod demo-pod, got %q", treeResponse.Pod)
	}
	if treeResponse.Path != "/workspace" {
		t.Fatalf("expected workspace root path, got %q", treeResponse.Path)
	}
	if len(treeResponse.Entries) != 2 {
		t.Fatalf("expected 2 filesystem entries, got %d", len(treeResponse.Entries))
	}
	if treeResponse.Entries[0].Kind != "dir" || treeResponse.Entries[0].Path != "/workspace/dir" {
		t.Fatalf("unexpected first tree entry: %+v", treeResponse.Entries[0])
	}
	if treeResponse.Entries[1].Kind != "file" || treeResponse.Entries[1].Path != "/workspace/file.txt" {
		t.Fatalf("unexpected second tree entry: %+v", treeResponse.Entries[1])
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/filesystem/file?path=/workspace/file.txt", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var fileResponse struct {
		Pod      string `json:"pod"`
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
		Size     int64  `json:"size"`
		Mode     int32  `json:"mode"`
	}
	mustDecodeJSON(t, recorder, &fileResponse)
	if fileResponse.Content != "hello" {
		t.Fatalf("expected file content hello, got %q", fileResponse.Content)
	}
	if fileResponse.Encoding != "utf-8" {
		t.Fatalf("expected utf-8 encoding, got %q", fileResponse.Encoding)
	}
	if fileResponse.Size != 5 {
		t.Fatalf("expected file size 5, got %d", fileResponse.Size)
	}
	if fileResponse.Mode != 0o644 {
		t.Fatalf("expected file mode 0644, got %o", fileResponse.Mode)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("expected 2 workspace exec calls, got %d", len(runner.calls))
	}
	if runner.calls[0].container != runtime.AppWorkspaceContainerName {
		t.Fatalf("expected workspace sidecar container %q, got %q", runtime.AppWorkspaceContainerName, runner.calls[0].container)
	}
}

func TestAppFilesystemWriteDirectoryAndDelete(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppFilesystemTestServer(t)

	pod := kubePodInfo{}
	pod.Metadata.Name = "demo-pod"
	pod.Metadata.CreationTimestamp = time.Now().UTC()
	pod.Status.Phase = "Running"

	server.newWorkspacePodLister = func(string) (workspacePodLister, error) {
		return fakeWorkspacePodLister{pods: []kubePodInfo{pod}}, nil
	}
	runner := &fakeWorkspaceExecRunner{
		outputs: [][]byte{
			[]byte("2\t640\t1700000002"),
			[]byte("0\t755\t1700000003"),
			[]byte(""),
		},
	}
	server.workspaceExecRunner = runner

	recorder := performJSONRequest(t, server, http.MethodPut, "/v1/apps/"+app.ID+"/filesystem/file", apiKey, map[string]any{
		"path":          "/workspace/notes/hello.txt",
		"content":       base64.StdEncoding.EncodeToString([]byte("hi")),
		"encoding":      "base64",
		"mode":          0o640,
		"mkdir_parents": true,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var writeResponse struct {
		Path string `json:"path"`
		Mode int32  `json:"mode"`
	}
	mustDecodeJSON(t, recorder, &writeResponse)
	if writeResponse.Path != "/workspace/notes/hello.txt" {
		t.Fatalf("unexpected written path %q", writeResponse.Path)
	}
	if writeResponse.Mode != 0o640 {
		t.Fatalf("expected written file mode 0640, got %o", writeResponse.Mode)
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/filesystem/directory", apiKey, map[string]any{
		"path":    "/workspace/assets",
		"parents": true,
		"mode":    0o755,
	})
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodDelete, "/v1/apps/"+app.ID+"/filesystem?path=/workspace/assets&recursive=true", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	if len(runner.calls) != 3 {
		t.Fatalf("expected 3 workspace exec calls, got %d", len(runner.calls))
	}
	if string(runner.calls[0].stdin) != "hi" {
		t.Fatalf("expected write stdin hi, got %q", string(runner.calls[0].stdin))
	}
	if got := runner.calls[0].command[len(runner.calls[0].command)-3:]; strings.Join(got, ",") != "/workspace/notes/hello.txt,true,640" {
		t.Fatalf("unexpected write command args: %v", got)
	}
	if got := runner.calls[1].command[len(runner.calls[1].command)-3:]; strings.Join(got, ",") != "/workspace/assets,true,755" {
		t.Fatalf("unexpected mkdir command args: %v", got)
	}
	if got := runner.calls[2].command[len(runner.calls[2].command)-2:]; strings.Join(got, ",") != "/workspace/assets,true" {
		t.Fatalf("unexpected delete command args: %v", got)
	}
}

func TestAppFilesystemRejectsPathsOutsideWorkspace(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppFilesystemTestServer(t)

	pod := kubePodInfo{}
	pod.Metadata.Name = "demo-pod"
	pod.Metadata.CreationTimestamp = time.Now().UTC()
	pod.Status.Phase = "Running"

	server.newWorkspacePodLister = func(string) (workspacePodLister, error) {
		return fakeWorkspacePodLister{pods: []kubePodInfo{pod}}, nil
	}
	runner := &fakeWorkspaceExecRunner{}
	server.workspaceExecRunner = runner

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/filesystem/file?path=/etc/passwd", apiKey, nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("expected no workspace exec calls, got %d", len(runner.calls))
	}
}

func TestAppFilesystemExecUnavailableReturnsServiceUnavailable(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppFilesystemTestServer(t)

	pod := kubePodInfo{}
	pod.Metadata.Name = "demo-pod"
	pod.Metadata.CreationTimestamp = time.Now().UTC()
	pod.Status.Phase = "Running"

	server.newWorkspacePodLister = func(string) (workspacePodLister, error) {
		return fakeWorkspacePodLister{pods: []kubePodInfo{pod}}, nil
	}
	server.workspaceExecRunner = &fakeWorkspaceExecRunner{
		errs: []error{errKubeWorkspaceExecUnavailable},
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/filesystem/file?path=/workspace/file.txt", apiKey, nil)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Error string `json:"error"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Error != "workspace access is not available in the api runtime" {
		t.Fatalf("unexpected error message %q", response.Error)
	}
}

func setupAppFilesystemTestServer(t *testing.T) (*store.Store, *Server, string, model.App) {
	t.Helper()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Filesystem Test Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "worker-1", model.RuntimeTypeManagedOwned, "https://runtime.example.com", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
		Workspace: &model.AppWorkspaceSpec{},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	return s, server, apiKey, app
}

func encodeFilesystemTreeLine(name, entryPath, kind string, size int64, mode int32, modifiedAt time.Time, hasChildren bool) string {
	return strings.Join([]string{
		base64.StdEncoding.EncodeToString([]byte(name)),
		base64.StdEncoding.EncodeToString([]byte(entryPath)),
		kind,
		strconv.FormatInt(size, 10),
		strconv.FormatInt(int64(mode), 8),
		strconv.FormatInt(modifiedAt.Unix(), 10),
		strconv.FormatBool(hasChildren),
	}, "\t")
}

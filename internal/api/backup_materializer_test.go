package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/backupmaterializer/composition"
	"fugue/internal/backupmaterializer/httpapi"
	"fugue/internal/store"
)

var _ BackupMaterializerEndpoint = (*composition.Handler)(nil)

type backupMaterializerEndpointStub struct {
	enabled       bool
	enabledCalls  atomic.Int64
	serveCalls    atomic.Int64
	requestRun    string
	requestPath   string
	authorization string
}

func (stub *backupMaterializerEndpointStub) BackupMaterializerEndpointV1() {}

func (stub *backupMaterializerEndpointStub) Enabled() bool {
	stub.enabledCalls.Add(1)
	return stub != nil && stub.enabled
}

func (stub *backupMaterializerEndpointStub) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	stub.serveCalls.Add(1)
	stub.requestRun = request.PathValue("run")
	stub.requestPath = request.URL.Path
	stub.authorization = request.Header.Get("Authorization")
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(writer, `{"apiVersion":"backup-materializer.fugue.dev/v1"}`)
}

type changingBackupMaterializerEndpoint struct {
	enabledCalls atomic.Int64
	serveCalls   atomic.Int64
}

func (endpoint *changingBackupMaterializerEndpoint) BackupMaterializerEndpointV1() {}

func (endpoint *changingBackupMaterializerEndpoint) Enabled() bool {
	return endpoint.enabledCalls.Add(1) == 1
}

func (endpoint *changingBackupMaterializerEndpoint) ServeHTTP(http.ResponseWriter, *http.Request) {
	endpoint.serveCalls.Add(1)
}

func TestBackupMaterializerEndpointDefaultsPrivateAndOff(t *testing.T) {
	stateStore := newBackupMaterializerRouteStore(t)
	disabled := &backupMaterializerEndpointStub{}
	var typedNil *backupMaterializerEndpointStub
	tests := map[string]BackupMaterializerEndpoint{
		"absent":    nil,
		"disabled":  disabled,
		"typed nil": typedNil,
	}
	for name, endpoint := range tests {
		t.Run(name, func(t *testing.T) {
			server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{
				BackupMaterializerEndpoint: endpoint,
			})
			request := httptest.NewRequest(http.MethodGet, backupMaterializerRoute("run-route-default-1"), nil)
			request.Header.Set("Authorization", "Bearer presented.materializer.token")
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			if response.Code != http.StatusNotFound {
				t.Fatalf("default-off endpoint status=%d body=%s", response.Code, response.Body.String())
			}
			assertBackupMaterializerPrivateHeaders(t, response.Header())
			if strings.Contains(response.Body.String(), "materializer") || strings.Contains(response.Body.String(), "presented") {
				t.Fatalf("default-off response exposed endpoint state: %s", response.Body.String())
			}
		})
	}
	if disabled.serveCalls.Load() != 0 {
		t.Fatalf("disabled endpoint served %d request(s)", disabled.serveCalls.Load())
	}

	direct := httptest.NewRecorder()
	(*Server)(nil).handleGetBackupObserverInputBundle(direct, httptest.NewRequest(http.MethodGet, "/", nil))
	if direct.Code != http.StatusNotFound {
		t.Fatalf("nil server direct status=%d body=%s", direct.Code, direct.Body.String())
	}
	assertBackupMaterializerPrivateHeaders(t, direct.Header())
}

func TestBackupMaterializerEndpointDelegatesExactGeneratedRoute(t *testing.T) {
	stateStore := newBackupMaterializerRouteStore(t)
	endpoint := &backupMaterializerEndpointStub{enabled: true}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{
		BackupMaterializerEndpoint: endpoint,
	})
	runID := "run-route-enabled-1"
	target := backupMaterializerRoute(runID)
	request := httptest.NewRequest(http.MethodGet, target, nil)
	request.Header.Set("Authorization", "Bearer header.payload.signature")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || endpoint.serveCalls.Load() != 1 || endpoint.enabledCalls.Load() != 2 {
		t.Fatalf("enabled endpoint status=%d enabled=%d served=%d body=%s", response.Code, endpoint.enabledCalls.Load(), endpoint.serveCalls.Load(), response.Body.String())
	}
	if endpoint.requestRun != runID || endpoint.requestPath != target || endpoint.authorization != "Bearer header.payload.signature" {
		t.Fatalf("generated route drifted: run=%q path=%q auth=%q", endpoint.requestRun, endpoint.requestPath, endpoint.authorization)
	}
	assertBackupMaterializerPrivateHeaders(t, response.Header())
}

func TestBackupMaterializerEndpointRechecksEnablementBeforeDispatch(t *testing.T) {
	stateStore := newBackupMaterializerRouteStore(t)
	endpoint := &changingBackupMaterializerEndpoint{}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{
		BackupMaterializerEndpoint: endpoint,
	})
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, backupMaterializerRoute("run-route-change-1"), nil),
	)
	if response.Code != http.StatusNotFound || endpoint.enabledCalls.Load() != 2 || endpoint.serveCalls.Load() != 0 {
		t.Fatalf("enablement change was not fail-closed: status=%d enabled=%d served=%d", response.Code, endpoint.enabledCalls.Load(), endpoint.serveCalls.Load())
	}
	assertBackupMaterializerPrivateHeaders(t, response.Header())
}

func TestBackupMaterializerRouteAdapterOwnsNoCompositionCapability(t *testing.T) {
	source, err := os.ReadFile("backup_materializer.go")
	if err != nil {
		t.Fatalf("read backup materializer route adapter: %v", err)
	}
	for _, forbidden := range []string{
		"backupmaterializer/composition", "backupmaterializerreview", "internal/store", "os.Getenv", "TokenReview", "ObserverKeyring",
	} {
		if strings.Contains(string(source), forbidden) {
			t.Fatalf("route adapter gained composition capability through %q", forbidden)
		}
	}
}

func backupMaterializerRoute(runID string) string {
	return strings.Replace(httpapi.RoutePath, "{run}", runID, 1)
}

func newBackupMaterializerRouteStore(t *testing.T) *store.Store {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("initialize backup materializer route store: %v", err)
	}
	return stateStore
}

func assertBackupMaterializerPrivateHeaders(t *testing.T, header http.Header) {
	t.Helper()
	if header.Get("Cache-Control") != "private, no-store, max-age=0" || header.Get("Pragma") != "no-cache" ||
		header.Get("X-Content-Type-Options") != "nosniff" || header.Get("Vary") != "Authorization" {
		t.Fatalf("backup materializer response lost private headers: %v", header)
	}
}

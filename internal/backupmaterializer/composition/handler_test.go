package composition

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/backupadapter"
	"fugue/internal/backupidentity"
	"fugue/internal/backupmaterializer"
	"fugue/internal/backupmaterializer/httpapi"
	"fugue/internal/backupmaterializeridentity"
	"fugue/internal/backupmaterializerreview/projected"
	"fugue/internal/model"
	"fugue/internal/store"

	authenticationv1 "k8s.io/api/authentication/v1"
)

const (
	testWorkloadToken  = "materializer-header.materializer-payload.materializer-signature"
	testAPICallerToken = "api-header.api-payload.api-signature"
)

type compositionStoreStub struct {
	runCalls     atomic.Int64
	backendCalls atomic.Int64
}

func (stub *compositionStoreStub) GetBackupRun(string, string, bool) (model.BackupRun, error) {
	stub.runCalls.Add(1)
	return model.BackupRun{}, store.ErrNotFound
}

func (stub *compositionStoreStub) GetBackupBackendObservation(string, string, bool) (store.BackupBackendObservation, error) {
	stub.backendCalls.Add(1)
	return store.BackupBackendObservation{}, store.ErrNotFound
}

func TestDisabledCompositionTouchesNoCapabilityAndFailsClosed(t *testing.T) {
	stub := &compositionStoreStub{}
	config := Config{
		Enabled: false,
		Store:   stub,
		ObserverKeyring: backupidentity.Keyring{
			ActiveKeyID: "sensitive-key-id",
			Keys:        map[string]string{"sensitive-key-id": "sensitive-observer-signing-material"},
		},
		TokenReview: projected.Config{
			APIServerURL:   "https://sensitive-api.example.test",
			ProjectionRoot: "/sensitive/projected/credential",
		},
	}
	handler, err := New(config)
	if err != nil || handler == nil || handler.Enabled() {
		t.Fatalf("disabled composition: handler=%#v err=%v", handler, err)
	}
	request := httptest.NewRequest(http.MethodGet, "/unregistered", nil)
	request.Header.Set("Authorization", "Bearer "+testWorkloadToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || stub.runCalls.Load() != 0 || stub.backendCalls.Load() != 0 {
		t.Fatalf("disabled composition crossed a capability: status=%d run=%d backend=%d", response.Code, stub.runCalls.Load(), stub.backendCalls.Load())
	}
	if response.Header().Get("Cache-Control") != "private, no-store, max-age=0" ||
		response.Header().Get("Pragma") != "no-cache" || response.Header().Get("Vary") != "Authorization" {
		t.Fatalf("disabled response lost private headers: %v", response.Header())
	}
	rendered := fmt.Sprintf("%+v %#v %+v %#v", config, config, handler, handler)
	for _, secret := range []string{"sensitive-key-id", "sensitive-observer", "sensitive-api", "sensitive/projected", testWorkloadToken} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("composition formatting exposed %q: %s", secret, rendered)
		}
	}
}

func TestCompositionServesRealPrivateBundleThroughEveryBoundary(t *testing.T) {
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "composition-store-encryption-key-material")
	now := time.Date(2026, 7, 30, 8, 0, 0, 0, time.UTC)
	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("initialize composition store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name: "composition backend", Provider: model.DataBackendProviderS3,
		Bucket: "private-composition-bucket", Region: "region-1", Endpoint: "https://s3.private.example.test", Prefix: "backups",
		Credentials: model.DataBackendCredentials{
			AccessKeyID: "composition-access-key", SecretAccessKey: "composition-secret-key", Token: "composition-session-token",
		},
	})
	if err != nil {
		t.Fatalf("create composition backend: %v", err)
	}
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		ID: "run-composition-real-1", AppID: "app-1", BackendID: backend.ID, Trigger: model.BackupRunTriggerManual,
		Target: model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app-1"},
	})
	if err != nil {
		t.Fatalf("create composition run: %v", err)
	}
	observation, err := stateStore.GetBackupBackendObservation(backend.ID, "", true)
	if err != nil {
		t.Fatalf("read composition backend generation: %v", err)
	}
	expectedSpec, err := backupadapter.BuildShadowSpec(run, observation.Generation)
	if err != nil {
		t.Fatalf("build expected composition spec: %v", err)
	}
	before, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read store before composed request: %v", err)
	}

	certificates := issueCompositionCertificate(t)
	serviceAccountName := backupmaterializeridentity.ServiceAccountNameForCell(expectedSpec.CellKey)
	var reviewCalls atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		reviewCalls.Add(1)
		if request.Method != http.MethodPost || request.URL.Path != "/apis/authentication.k8s.io/v1/tokenreviews" ||
			request.Header.Get("Authorization") != "Bearer "+testAPICallerToken {
			http.Error(writer, "bad review boundary", http.StatusBadRequest)
			return
		}
		var review authenticationv1.TokenReview
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&review); err != nil || review.Spec.Token != testWorkloadToken ||
			!reflect.DeepEqual(review.Spec.Audiences, []string{backupmaterializeridentity.Audience}) {
			http.Error(writer, "bad review request", http.StatusBadRequest)
			return
		}
		response := authenticationv1.TokenReview{
			TypeMeta: review.TypeMeta,
			Spec:     review.Spec,
			Status: authenticationv1.TokenReviewStatus{
				Authenticated: true,
				Audiences:     []string{backupmaterializeridentity.Audience},
				User: authenticationv1.UserInfo{
					Username: "system:serviceaccount:" + backupmaterializeridentity.ServiceAccountNamespace + ":" + serviceAccountName,
					UID:      "11111111-1111-4111-8111-111111111111",
					Groups: []string{
						"system:serviceaccounts",
						"system:serviceaccounts:" + backupmaterializeridentity.ServiceAccountNamespace,
						"system:authenticated",
					},
					Extra: map[string]authenticationv1.ExtraValue{
						"authentication.kubernetes.io/credential-id": {"JTI=33333333-3333-4333-8333-333333333333"},
						"authentication.kubernetes.io/pod-name":      {serviceAccountName + "-6d4f7c8b9f-abcde"},
						"authentication.kubernetes.io/pod-uid":       {"22222222-2222-4222-8222-222222222222"},
					},
				},
			},
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(writer).Encode(response)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificates.server}}
	server.StartTLS()
	defer server.Close()

	projectionRoot := t.TempDir()
	writeCompositionFile(t, filepath.Join(projectionRoot, projected.TokenFileName), []byte(testAPICallerToken), 0o440)
	writeCompositionFile(t, filepath.Join(projectionRoot, projected.CAFileName), certificates.caPEM, 0o444)
	keyring := backupidentity.DeriveKeyring(
		"composition-observer-active-key-material-at-least-32-bytes",
		"composition-active-1",
		"",
		"",
		nil,
	)
	handler, err := New(Config{
		Enabled:         true,
		Store:           stateStore,
		ObserverKeyring: keyring,
		TokenReview: projected.Config{
			APIServerURL:   server.URL,
			ProjectionRoot: projectionRoot,
		},
		Now: func() time.Time { return now },
	})
	if err != nil || !handler.Enabled() {
		t.Fatalf("construct enabled composition: handler=%#v err=%v", handler, err)
	}
	path := strings.Replace(httpapi.RoutePath, "{run}", run.ID, 1)
	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.SetPathValue("run", run.ID)
	request.Header.Set("Authorization", "Bearer "+testWorkloadToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || reviewCalls.Load() != 1 {
		t.Fatalf("composed request failed: status=%d reviews=%d body=%s", response.Code, reviewCalls.Load(), response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "private, no-store, max-age=0" ||
		response.Header().Get("Pragma") != "no-cache" || response.Header().Get("Vary") != "Authorization" {
		t.Fatalf("composed response lost private headers: %v", response.Header())
	}
	bundle, err := backupmaterializer.DecodeObserverInputBundle(response.Body.Bytes(), keyring, now)
	if err != nil || bundle.DesiredSpec != expectedSpec || bundle.RunID != run.ID || !bundle.ObservationOnly || bundle.ProductionMutationAllowed {
		t.Fatalf("composed bundle drifted: bundle=%+v err=%v", bundle, err)
	}
	for _, forbidden := range []string{
		backend.Bucket, backend.Endpoint, "composition-access-key", "composition-secret-key", "composition-session-token",
		testAPICallerToken, testWorkloadToken,
	} {
		if strings.Contains(response.Body.String(), forbidden) {
			t.Fatalf("composed response exposed %q", forbidden)
		}
	}
	after, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read store after composed request: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("composed read path mutated the JSON store")
	}
}

func TestEnabledCompositionConfigurationErrorsAreUniform(t *testing.T) {
	certificates := issueCompositionCertificate(t)
	projectionRoot := t.TempDir()
	writeCompositionFile(t, filepath.Join(projectionRoot, projected.TokenFileName), []byte(testAPICallerToken), 0o440)
	writeCompositionFile(t, filepath.Join(projectionRoot, projected.CAFileName), certificates.caPEM, 0o444)
	validStore := &compositionStoreStub{}
	validKeyring := backupidentity.DeriveKeyring(
		"composition-config-active-key-material-at-least-32-bytes", "composition-active-1", "", "", nil,
	)
	base := Config{
		Enabled:         true,
		Store:           validStore,
		ObserverKeyring: validKeyring,
		TokenReview: projected.Config{
			APIServerURL:   "https://127.0.0.1:6443",
			ProjectionRoot: projectionRoot,
		},
		Now: time.Now,
	}
	var typedNil *compositionStoreStub
	tests := map[string]func(*Config){
		"nil clock":          func(config *Config) { config.Now = nil },
		"nil store":          func(config *Config) { config.Store = nil },
		"typed nil store":    func(config *Config) { config.Store = typedNil },
		"invalid keyring":    func(config *Config) { config.ObserverKeyring = backupidentity.Keyring{} },
		"invalid projection": func(config *Config) { config.TokenReview.ProjectionRoot = "/missing/private/projection" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			config := base
			mutate(&config)
			handler, err := New(config)
			if handler != nil || !errors.Is(err, ErrConfig) || err.Error() != ErrConfig.Error() {
				t.Fatalf("configuration result: handler=%#v err=%v", handler, err)
			}
			for _, detail := range []string{projectionRoot, "composition-config-active", "127.0.0.1"} {
				if strings.Contains(err.Error(), detail) {
					t.Fatalf("configuration error exposed %q: %v", detail, err)
				}
			}
		})
	}
	if validStore.runCalls.Load() != 0 || validStore.backendCalls.Load() != 0 {
		t.Fatalf("construction read the store: run=%d backend=%d", validStore.runCalls.Load(), validStore.backendCalls.Load())
	}
}

func TestCompositionDependencyAndRouteBoundary(t *testing.T) {
	command := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list composition dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		if dependency == "database/sql" || dependency == "os" || dependency == "os/exec" ||
			strings.HasPrefix(dependency, "k8s.io/") || dependency == "fugue/internal/api" {
			t.Fatalf("composition gained forbidden direct dependency %q", dependency)
		}
	}
	sort.Strings(local)
	want := []string{
		"fugue/internal/backupidentity",
		"fugue/internal/backupmaterializer/httpapi",
		"fugue/internal/backupmaterializer/legacysource",
		"fugue/internal/backupmaterializer/localissuer",
		"fugue/internal/backupmaterializer/storesource",
		"fugue/internal/backupmaterializeridentity/httpauth",
		"fugue/internal/backupmaterializerreview/projected",
	}
	if !reflect.DeepEqual(local, want) {
		t.Fatalf("composition dependency boundary widened: got=%v want=%v", local, want)
	}
	source, err := os.ReadFile("handler.go")
	if err != nil {
		t.Fatalf("read composition source: %v", err)
	}
	for _, forbidden := range []string{"NewServeMux", ".Handle(", ".HandleFunc(", "RoutePath", "ListenAndServe", "http.Server{"} {
		if strings.Contains(string(source), forbidden) {
			t.Fatalf("composition gained route/server ownership through %q", forbidden)
		}
	}
}

type compositionCertificate struct {
	caPEM  []byte
	server tls.Certificate
}

func issueCompositionCertificate(t *testing.T) compositionCertificate {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate composition CA: %v", err)
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(101), Subject: pkix.Name{CommonName: "fugue composition test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create composition CA: %v", err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate composition server key: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(102), Subject: pkix.Name{CommonName: "fugue composition test server"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(12 * time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create composition server certificate: %v", err)
	}
	return compositionCertificate{
		caPEM:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		server: tls.Certificate{Certificate: [][]byte{leafDER, caDER}, PrivateKey: leafKey},
	}
}

func writeCompositionFile(t *testing.T, path string, document []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, document, mode); err != nil {
		t.Fatalf("write composition projection: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("set composition projection mode: %v", err)
	}
}

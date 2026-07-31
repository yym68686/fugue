package projected

import (
	"context"
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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/backupidentity"
	"fugue/internal/backupmaterializer"
	materializerclient "fugue/internal/backupmaterializer/client"
)

const (
	testProjectedTokenOne = "header-one.materializer-one.signature-one"
	testProjectedTokenTwo = "header-two.materializer-two.signature-two"
)

type certificateFixture struct {
	caPEM      []byte
	leafPEM    []byte
	serverCert *tls.Certificate
}

func TestClientReloadsAtomicCredentialCAAndConnections(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 31, 3, 0, 0, 0, time.UTC)
	spec := testProjectedSpec(t)
	document := testProjectedBundleDocument(t, spec, now)
	firstCertificates := issueProjectedCertificateFixture(t, 1)
	secondCertificates := issueProjectedCertificateFixture(t, 2)
	var currentCertificate atomic.Pointer[tls.Certificate]
	currentCertificate.Store(firstCertificates.serverCert)
	var connections atomic.Int64
	var requests atomic.Int64
	var authorizationsMu sync.Mutex
	authorizations := []string{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		authorizationsMu.Lock()
		authorizations = append(authorizations, request.Header.Get("Authorization"))
		authorizationsMu.Unlock()
		if request.Method != http.MethodGet || request.URL.RawQuery != "" || request.ContentLength != 0 {
			t.Errorf("projected client request drifted: method=%s uri=%s length=%d", request.Method, request.URL.RequestURI(), request.ContentLength)
		}
		writeProjectedPrivateResponse(writer, document)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			certificate := currentCertificate.Load()
			if certificate == nil {
				return nil, errors.New("test certificate unavailable")
			}
			return &tls.Config{
				MinVersion:   tls.VersionTLS12,
				Certificates: []tls.Certificate{*certificate},
			}, nil
		},
	}
	server.StartTLS()
	defer server.Close()

	root := t.TempDir()
	firstGeneration := "..2026_07_31_03_00_00.000000001"
	secondGeneration := "..2026_07_31_03_05_00.000000002"
	invalidCAGeneration := "..2026_07_31_03_10_00.000000003"
	invalidTokenGeneration := "..2026_07_31_03_15_00.000000004"
	writeProjectedGeneration(t, root, firstGeneration, testProjectedTokenOne, firstCertificates.caPEM, 0o600, 0o444)
	writeProjectedGeneration(t, root, secondGeneration, testProjectedTokenTwo, secondCertificates.caPEM, 0o600, 0o444)
	writeProjectedGeneration(t, root, invalidCAGeneration, testProjectedTokenTwo, []byte("not a CA bundle"), 0o600, 0o444)
	writeProjectedGeneration(t, root, invalidTokenGeneration, "opaque", secondCertificates.caPEM, 0o600, 0o444)
	linkProjectedFiles(t, root)
	activateProjectedGenerationForTest(t, root, firstGeneration, 1)

	client, err := New(Config{
		Enabled:         true,
		BaseURL:         server.URL,
		ProjectionRoot:  root,
		ExpectedCellKey: spec.CellKey,
		ExpectedRunID:   spec.RunID,
		Now:             func() time.Time { return now.Add(time.Minute) },
	})
	if err != nil {
		t.Fatalf("create projected materializer client: %v", err)
	}
	if requests.Load() != 0 || connections.Load() != 0 {
		t.Fatalf("construction performed network I/O: requests=%d connections=%d", requests.Load(), connections.Load())
	}
	fetch := func() {
		t.Helper()
		bundle, err := client.Fetch(context.Background())
		if err != nil || bundle.CellKey != spec.CellKey || bundle.RunID != spec.RunID || bundle.DesiredSpec != spec {
			t.Fatalf("fetch projected bundle: bundle=%#v err=%v", bundle, err)
		}
	}
	fetch()

	currentCertificate.Store(secondCertificates.serverCert)
	activateProjectedGenerationForTest(t, root, secondGeneration, 2)
	if err := os.RemoveAll(filepath.Join(root, firstGeneration)); err != nil {
		t.Fatalf("remove retired projection generation: %v", err)
	}
	fetch()

	activateProjectedGenerationForTest(t, root, invalidCAGeneration, 3)
	if _, err := client.Fetch(context.Background()); !errors.Is(err, materializerclient.ErrInputUnavailable) {
		t.Fatalf("invalid rotated CA error = %v, want input unavailable", err)
	} else {
		assertProjectedSecretFree(t, err)
	}
	activateProjectedGenerationForTest(t, root, invalidTokenGeneration, 4)
	if _, err := client.Fetch(context.Background()); !errors.Is(err, materializerclient.ErrCredentialUnavailable) {
		t.Fatalf("invalid rotated credential error = %v, want credential unavailable", err)
	} else {
		assertProjectedSecretFree(t, err)
	}
	activateProjectedGenerationForTest(t, root, secondGeneration, 5)
	fetch()

	authorizationsMu.Lock()
	wantAuthorizations := []string{
		"Bearer " + testProjectedTokenOne,
		"Bearer " + testProjectedTokenTwo,
		"Bearer " + testProjectedTokenTwo,
	}
	if !reflect.DeepEqual(authorizations, wantAuthorizations) {
		t.Fatalf("projected credential rotation drifted: got=%v want=%v", authorizations, wantAuthorizations)
	}
	authorizationsMu.Unlock()
	if requests.Load() != 3 || connections.Load() != 3 {
		t.Fatalf("fetches reused a connection or invalid input reached the network: requests=%d connections=%d", requests.Load(), connections.Load())
	}
}

func TestDisabledProjectedClientIsInertAndRetainsNoCapabilities(t *testing.T) {
	t.Parallel()
	config := Config{
		Enabled:         false,
		BaseURL:         "not a URL",
		ProjectionRoot:  filepath.Join(t.TempDir(), "missing"),
		ExpectedCellKey: "not a cell",
		ExpectedRunID:   "not a run",
		Now:             func() time.Time { panic("disabled projected client read the clock") },
	}
	client, err := New(config)
	if err != nil {
		t.Fatalf("create disabled projected client: %v", err)
	}
	if client.Enabled() {
		t.Fatal("disabled projected client reported enabled")
	}
	if _, err := client.Fetch(nil); !errors.Is(err, materializerclient.ErrDisabled) {
		t.Fatalf("disabled fetch error = %v, want disabled", err)
	}
	for _, rendered := range []string{fmt.Sprint(config), fmt.Sprintf("%#v", config), fmt.Sprint(client), fmt.Sprintf("%#v", client)} {
		if strings.Contains(rendered, config.BaseURL) || strings.Contains(rendered, config.ProjectionRoot) ||
			!strings.Contains(rendered, "[REDACTED]") {
			t.Fatalf("disabled projected configuration leaked or was retained: %q", rendered)
		}
	}
}

func TestProjectedClientAcceptsStableKubernetesFileModes(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 31, 4, 0, 0, 0, time.UTC)
	spec := testProjectedSpec(t)
	certificates := issueProjectedCertificateFixture(t, 11)
	server := newProjectedTLSServer(t, certificates.serverCert, testProjectedBundleDocument(t, spec, now))
	defer server.Close()
	for _, tokenMode := range []os.FileMode{0o400, 0o440, 0o600, 0o640} {
		t.Run(fmt.Sprintf("token-%#o", tokenMode), func(t *testing.T) {
			root := newStableProjectedFiles(t, testProjectedTokenOne+"\n", certificates.caPEM, tokenMode, 0o644)
			client, err := New(Config{
				Enabled: true, BaseURL: server.URL, ProjectionRoot: root,
				ExpectedCellKey: spec.CellKey, ExpectedRunID: spec.RunID,
				Now: func() time.Time { return now.Add(time.Minute) },
			})
			if err != nil {
				t.Fatalf("create stable projected client: %v", err)
			}
			if _, err := client.Fetch(context.Background()); err != nil {
				t.Fatalf("fetch through stable projected files: %v", err)
			}
		})
	}
}

func TestProjectedClientRejectsConfigurationDrift(t *testing.T) {
	t.Parallel()
	certificates := issueProjectedCertificateFixture(t, 21)
	validRoot := newStableProjectedFiles(t, testProjectedTokenOne, certificates.caPEM, 0o600, 0o444)
	unsafeRoot := t.TempDir()
	if err := os.Chmod(unsafeRoot, 0o777); err != nil {
		t.Fatalf("chmod unsafe projection root: %v", err)
	}
	realRoot := t.TempDir()
	symlinkRoot := filepath.Join(t.TempDir(), "projection")
	if err := os.Symlink(realRoot, symlinkRoot); err != nil {
		t.Fatalf("link projection root: %v", err)
	}
	valid := Config{
		Enabled:          true,
		BaseURL:          "https://api.example.test",
		ProjectionRoot:   validRoot,
		ExpectedCellKey:  "backup/app-database/0123456789abcdef",
		ExpectedRunID:    "run-1",
		RequestTimeout:   5 * time.Second,
		HandshakeTimeout: 3 * time.Second,
	}
	tests := map[string]func(*Config){
		"empty URL":          func(value *Config) { value.BaseURL = "" },
		"plaintext URL":      func(value *Config) { value.BaseURL = "http://api.example.test" },
		"URL root path":      func(value *Config) { value.BaseURL = "https://api.example.test/" },
		"URL credentials":    func(value *Config) { value.BaseURL = "https://user@api.example.test" },
		"URL query":          func(value *Config) { value.BaseURL = "https://api.example.test?x=1" },
		"URL fragment":       func(value *Config) { value.BaseURL = "https://api.example.test#x" },
		"invalid port":       func(value *Config) { value.BaseURL = "https://api.example.test:70000" },
		"empty port":         func(value *Config) { value.BaseURL = "https://api.example.test:" },
		"relative root":      func(value *Config) { value.ProjectionRoot = "projection" },
		"filesystem root":    func(value *Config) { value.ProjectionRoot = string(filepath.Separator) },
		"symlink root":       func(value *Config) { value.ProjectionRoot = symlinkRoot },
		"writable root":      func(value *Config) { value.ProjectionRoot = unsafeRoot },
		"invalid cell":       func(value *Config) { value.ExpectedCellKey = "backup/app-database/ABC" },
		"invalid run":        func(value *Config) { value.ExpectedRunID = "run/1" },
		"short request":      func(value *Config) { value.RequestTimeout = time.Second - time.Millisecond },
		"long request":       func(value *Config) { value.RequestTimeout = materializerclient.MaximumRequestTimeout + time.Second },
		"fractional request": func(value *Config) { value.RequestTimeout = time.Second + time.Nanosecond },
		"short handshake":    func(value *Config) { value.HandshakeTimeout = time.Second - time.Millisecond },
		"long handshake":     func(value *Config) { value.HandshakeTimeout = maximumHandshakeTimeout + time.Second },
		"handshake over request": func(value *Config) {
			value.RequestTimeout = 2 * time.Second
			value.HandshakeTimeout = 3 * time.Second
		},
		"fractional handshake": func(value *Config) { value.HandshakeTimeout = time.Second + time.Nanosecond },
		"small response":       func(value *Config) { value.MaxResponseBytes = 100 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if _, err := New(candidate); !errors.Is(err, ErrConfig) {
				t.Fatalf("configuration error = %v, want invalid config", err)
			}
		})
	}
}

func TestProjectedClientRejectsUnsafeCredentialAndTrustFiles(t *testing.T) {
	t.Parallel()
	certificates := issueProjectedCertificateFixture(t, 31)
	credentialTests := map[string]func(*testing.T) string{
		"missing token": func(t *testing.T) string {
			root := t.TempDir()
			writeProjectedFile(t, filepath.Join(root, CAFileName), certificates.caPEM, 0o444)
			return root
		},
		"opaque token": func(t *testing.T) string {
			return newStableProjectedFiles(t, "opaque", certificates.caPEM, 0o600, 0o444)
		},
		"extra newline": func(t *testing.T) string {
			return newStableProjectedFiles(t, testProjectedTokenOne+"\n\n", certificates.caPEM, 0o600, 0o444)
		},
		"world-readable token": func(t *testing.T) string {
			return newStableProjectedFiles(t, testProjectedTokenOne, certificates.caPEM, 0o444, 0o444)
		},
		"oversized token": func(t *testing.T) string {
			return newStableProjectedFiles(t, strings.Repeat("a", int(maxTokenBytes)+1), certificates.caPEM, 0o600, 0o444)
		},
		"arbitrary token symlink": func(t *testing.T) string {
			root := t.TempDir()
			outside := filepath.Join(t.TempDir(), TokenFileName)
			writeProjectedFile(t, outside, []byte(testProjectedTokenOne), 0o600)
			if err := os.Symlink(outside, filepath.Join(root, TokenFileName)); err != nil {
				t.Fatalf("link arbitrary token: %v", err)
			}
			writeProjectedFile(t, filepath.Join(root, CAFileName), certificates.caPEM, 0o444)
			return root
		},
		"escaping data link": func(t *testing.T) string {
			root := t.TempDir()
			if err := os.Symlink(filepath.Join("..data", TokenFileName), filepath.Join(root, TokenFileName)); err != nil {
				t.Fatalf("link projected token: %v", err)
			}
			if err := os.Symlink("../outside", filepath.Join(root, "..data")); err != nil {
				t.Fatalf("link escaping data generation: %v", err)
			}
			writeProjectedFile(t, filepath.Join(root, CAFileName), certificates.caPEM, 0o444)
			return root
		},
		"symlink generation": func(t *testing.T) string {
			root := t.TempDir()
			generation := "..2026_07_31_05_00_00.000000001"
			outside := t.TempDir()
			writeProjectedFile(t, filepath.Join(outside, TokenFileName), []byte(testProjectedTokenOne), 0o600)
			if err := os.Symlink(outside, filepath.Join(root, generation)); err != nil {
				t.Fatalf("link generation directory: %v", err)
			}
			if err := os.Symlink(generation, filepath.Join(root, "..data")); err != nil {
				t.Fatalf("link data generation: %v", err)
			}
			if err := os.Symlink(filepath.Join("..data", TokenFileName), filepath.Join(root, TokenFileName)); err != nil {
				t.Fatalf("link projected token: %v", err)
			}
			writeProjectedFile(t, filepath.Join(root, CAFileName), certificates.caPEM, 0o444)
			return root
		},
	}
	for name, setup := range credentialTests {
		t.Run(name, func(t *testing.T) {
			root := setup(t)
			if _, err := New(testProjectedConfig(root)); !errors.Is(err, ErrCredential) {
				t.Fatalf("credential error = %v, want credential unavailable", err)
			} else {
				assertProjectedSecretFree(t, err)
			}
		})
	}

	otherCertificates := issueProjectedCertificateFixture(t, 32)
	trustTests := map[string]struct {
		ca   []byte
		mode os.FileMode
	}{
		"empty CA":           {ca: nil, mode: 0o444},
		"invalid PEM":        {ca: []byte("not a CA bundle"), mode: 0o444},
		"non-CA certificate": {ca: certificates.leafPEM, mode: 0o444},
		"trailing data":      {ca: append(append([]byte(nil), certificates.caPEM...), []byte("trailing")...), mode: 0o444},
		"writable CA":        {ca: certificates.caPEM, mode: 0o666},
		"too many CAs":       {ca: repeatProjectedBytes(certificates.caPEM, maxCACertificates+1), mode: 0o444},
		"oversized CA":       {ca: repeatProjectedBytes([]byte(" "), int(maxCABundleBytes)+1), mode: 0o444},
	}
	for name, test := range trustTests {
		t.Run(name, func(t *testing.T) {
			root := newStableProjectedFiles(t, testProjectedTokenOne, test.ca, 0o600, test.mode)
			if _, err := New(testProjectedConfig(root)); !errors.Is(err, ErrTrust) {
				t.Fatalf("trust error = %v, want trust unavailable", err)
			} else {
				assertProjectedSecretFree(t, err)
			}
		})
	}
	validBundle := append(append([]byte(nil), certificates.caPEM...), otherCertificates.caPEM...)
	root := newStableProjectedFiles(t, testProjectedTokenOne, validBundle, 0o600, 0o444)
	projection, err := openProjection(root)
	if err != nil {
		t.Fatalf("open multi-CA projection: %v", err)
	}
	if _, err := projection.loadCAPool(context.Background()); err != nil {
		t.Fatalf("strict multi-CA bundle rejected: %v", err)
	}
}

func TestRotatingClientTransportRejectsEndpointDriftBeforeNetwork(t *testing.T) {
	t.Parallel()
	certificates := issueProjectedCertificateFixture(t, 41)
	root := newStableProjectedFiles(t, testProjectedTokenOne, certificates.caPEM, 0o600, 0o444)
	projection, err := openProjection(root)
	if err != nil {
		t.Fatalf("open transport projection: %v", err)
	}
	transport := &rotatingCATransport{
		projection:        projection,
		expectedAuthority: "api.example.test",
		requestTimeout:    5 * time.Second,
		handshakeTimeout:  3 * time.Second,
	}
	for name, rawURL := range map[string]string{
		"plaintext":       "http://api.example.test/v1/backup-control/runs/run-1/observer-input-bundle",
		"wrong authority": "https://127.0.0.1:1/v1/backup-control/runs/run-1/observer-input-bundle",
		"URL credentials": "https://user@api.example.test/v1/backup-control/runs/run-1/observer-input-bundle",
		"query":           "https://api.example.test/v1/backup-control/runs/run-1/observer-input-bundle?x=1",
	} {
		t.Run(name, func(t *testing.T) {
			request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, rawURL, nil)
			if err != nil {
				t.Fatalf("create drifted request: %v", err)
			}
			if _, err := transport.RoundTrip(request); !errors.Is(err, ErrTrust) {
				t.Fatalf("endpoint drift error = %v, want trust unavailable", err)
			}
		})
	}
}

func TestProjectedCredentialReadsOnlyCompleteAtomicGenerations(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	certificates := issueProjectedCertificateFixture(t, 51)
	firstGeneration := "..2026_07_31_06_00_00.000000001"
	secondGeneration := "..2026_07_31_06_05_00.000000002"
	writeProjectedGeneration(t, root, firstGeneration, testProjectedTokenOne, certificates.caPEM, 0o640, 0o444)
	writeProjectedGeneration(t, root, secondGeneration, testProjectedTokenTwo, certificates.caPEM, 0o640, 0o444)
	linkProjectedFiles(t, root)
	activateProjectedGenerationForTest(t, root, firstGeneration, 1)
	projection, err := openProjection(root)
	if err != nil {
		t.Fatalf("open rotating projection: %v", err)
	}
	source := &credentialSource{projection: projection}
	errCh := make(chan error, 1)
	go func() {
		for index := 0; index < 200; index++ {
			generation := firstGeneration
			if index%2 == 1 {
				generation = secondGeneration
			}
			if err := activateProjectedGeneration(root, generation, index+2); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()
	for range 500 {
		token, err := source.Credential(context.Background())
		if err != nil {
			if !errors.Is(err, ErrCredential) {
				t.Fatalf("rotation read error = %v, want bounded credential failure", err)
			}
			continue
		}
		if token != testProjectedTokenOne && token != testProjectedTokenTwo {
			t.Fatalf("read partial or foreign projected credential %q", token)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatalf("rotate atomic projection: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.Credential(canceled); !errors.Is(err, ErrCredential) {
		t.Fatalf("canceled credential read error = %v, want credential unavailable", err)
	}
}

func TestProjectedClientProductionDependencyBoundary(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-deps", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list projected client dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{"database/sql", "os/exec", "fugue/internal/backupidentity", "fugue/internal/backupmaterializer"} {
			if dependency == forbidden {
				t.Fatalf("projected client gained forbidden dependency %q", dependency)
			}
		}
		for _, prefix := range []string{
			"k8s.io/", "fugue/internal/api", "fugue/internal/store", "fugue/internal/model",
			"fugue/internal/backupmaterializerreview", "fugue/internal/backupmaterializeridentity",
		} {
			if strings.HasPrefix(dependency, prefix) {
				t.Fatalf("projected client crossed component boundary through %q", dependency)
			}
		}
	}
	sort.Strings(local)
	wantLocal := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/client",
		"fugue/internal/backupmaterializer/client/projected",
		"fugue/internal/backupmaterializer/contract",
	}
	if !reflect.DeepEqual(local, wantLocal) {
		t.Fatalf("projected client local closure drifted: got=%v want=%v", local, wantLocal)
	}
	direct := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	directOutput, err := direct.Output()
	if err != nil {
		t.Fatalf("list direct projected client imports: %v", err)
	}
	gotDirect := strings.Fields(string(directOutput))
	sort.Strings(gotDirect)
	wantDirect := []string{
		"bytes",
		"context",
		"crypto/tls",
		"crypto/x509",
		"encoding/pem",
		"errors",
		"fugue/internal/backupmaterializer/client",
		"io",
		"net",
		"net/http",
		"net/url",
		"os",
		"path/filepath",
		"strconv",
		"strings",
		"time",
	}
	if !reflect.DeepEqual(gotDirect, wantDirect) {
		t.Fatalf("projected client direct imports drifted: got=%v want=%v", gotDirect, wantDirect)
	}
}

func testProjectedConfig(root string) Config {
	return Config{
		Enabled:         true,
		BaseURL:         "https://api.example.test",
		ProjectionRoot:  root,
		ExpectedCellKey: "backup/app-database/0123456789abcdef",
		ExpectedRunID:   "run-1",
	}
}

func testProjectedSpec(t *testing.T) backupcontrol.BackupRunSpec {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		"run-1",
		"run-1",
		backupcontrol.BackupTarget{Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database"},
		"backend-1",
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		4,
		120,
		1800,
	)
	if err != nil {
		t.Fatalf("build backup spec: %v", err)
	}
	return spec
}

func testProjectedBundleDocument(t *testing.T, spec backupcontrol.BackupRunSpec, now time.Time) []byte {
	t.Helper()
	keyring := backupidentity.DeriveKeyring(strings.Repeat("k", 32), "backup-key-1", "", "", nil)
	bundle, err := backupmaterializer.IssueObserverInputBundle(keyring, spec, "tenant-1", now)
	if err != nil {
		t.Fatalf("issue observer input bundle: %v", err)
	}
	document, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("encode observer input bundle: %v", err)
	}
	return document
}

func newProjectedTLSServer(t *testing.T, certificate *tls.Certificate, document []byte) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeProjectedPrivateResponse(writer, document)
	}))
	server.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{*certificate},
	}
	server.StartTLS()
	return server
}

func writeProjectedPrivateResponse(writer http.ResponseWriter, document []byte) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Content-Length", fmt.Sprint(len(document)))
	writer.Header().Set("Cache-Control", "private, no-store, max-age=0")
	writer.Header().Set("Pragma", "no-cache")
	writer.Header().Set("Vary", "Authorization")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(document)
}

func issueProjectedCertificateFixture(t *testing.T, serial int64) certificateFixture {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate test CA key: %v", err)
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(serial*10 + 1),
		Subject:               pkix.Name{CommonName: "fugue projected client test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create test CA: %v", err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate test server key: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial*10 + 2),
		Subject:      pkix.Name{CommonName: "fugue projected client test server"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(12 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create test server certificate: %v", err)
	}
	return certificateFixture{
		caPEM:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		leafPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		serverCert: &tls.Certificate{
			Certificate: [][]byte{leafDER, caDER},
			PrivateKey:  leafKey,
		},
	}
}

func newStableProjectedFiles(
	t *testing.T,
	token string,
	caPEM []byte,
	tokenMode os.FileMode,
	caMode os.FileMode,
) string {
	t.Helper()
	root := t.TempDir()
	writeProjectedFile(t, filepath.Join(root, TokenFileName), []byte(token), tokenMode)
	writeProjectedFile(t, filepath.Join(root, CAFileName), caPEM, caMode)
	return root
}

func writeProjectedGeneration(
	t *testing.T,
	root string,
	generation string,
	token string,
	caPEM []byte,
	tokenMode os.FileMode,
	caMode os.FileMode,
) {
	t.Helper()
	directory := filepath.Join(root, generation)
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatalf("create projection generation: %v", err)
	}
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatalf("set projection generation mode: %v", err)
	}
	writeProjectedFile(t, filepath.Join(directory, TokenFileName), []byte(token), tokenMode)
	writeProjectedFile(t, filepath.Join(directory, CAFileName), caPEM, caMode)
}

func linkProjectedFiles(t *testing.T, root string) {
	t.Helper()
	for _, name := range []string{TokenFileName, CAFileName} {
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(root, name)); err != nil {
			t.Fatalf("link projected %s: %v", name, err)
		}
	}
}

func activateProjectedGenerationForTest(t *testing.T, root string, generation string, sequence int) {
	t.Helper()
	if err := activateProjectedGeneration(root, generation, sequence); err != nil {
		t.Fatalf("activate projection generation: %v", err)
	}
}

func activateProjectedGeneration(root string, generation string, sequence int) error {
	temporary := filepath.Join(root, "..data_tmp_"+big.NewInt(int64(sequence)).String())
	if err := os.Symlink(generation, temporary); err != nil {
		return err
	}
	if err := os.Rename(temporary, filepath.Join(root, "..data")); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}

func writeProjectedFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("write projected test file: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("set projected test file mode: %v", err)
	}
}

func repeatProjectedBytes(value []byte, count int) []byte {
	result := make([]byte, 0, len(value)*count)
	for range count {
		result = append(result, value...)
	}
	return result
}

func assertProjectedSecretFree(t *testing.T, err error) {
	t.Helper()
	rendered := fmt.Sprintf("%+v", err)
	for _, secret := range []string{testProjectedTokenOne, testProjectedTokenTwo, "opaque", "not a CA bundle"} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("projected client error leaked private input %q: %v", secret, err)
		}
	}
}

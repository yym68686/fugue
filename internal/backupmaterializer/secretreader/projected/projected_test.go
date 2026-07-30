package projected

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
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
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
	"fugue/internal/backupmaterializer/secretreader"
)

const (
	testProjectedTokenOne = "header-one.kubernetes-one.signature-one"
	testProjectedTokenTwo = "header-two.kubernetes-two.signature-two"
)

type certificateFixture struct {
	caPEM      []byte
	leafPEM    []byte
	serverCert *tls.Certificate
}

func TestReaderReloadsAtomicCredentialCAAndConnections(t *testing.T) {
	t.Parallel()
	cellKey := testCellKey(t)
	identity, err := materialization.SecretIdentityForCell(cellKey)
	if err != nil {
		t.Fatalf("derive Secret identity: %v", err)
	}
	firstCertificates := issueCertificateFixture(t, 1)
	secondCertificates := issueCertificateFixture(t, 2)
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
		wantPath := "/api/v1/namespaces/" + identity.Namespace + "/secrets/" + identity.SecretName
		if request.Method != http.MethodGet || request.URL.Path != wantPath || request.URL.RawQuery != "" ||
			request.ContentLength != 0 || request.Header.Get("Accept") != "application/json" ||
			request.Header.Get("Accept-Encoding") != "identity" ||
			request.Header.Get("User-Agent") != secretreader.RequestUserAgent {
			t.Errorf("projected reader request drifted: method=%s uri=%s length=%d headers=%v", request.Method, request.URL.RequestURI(), request.ContentLength, request.Header)
		}
		writeNotFoundResponse(writer, identity.SecretName)
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
			return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{*certificate}}, nil
		},
	}
	server.StartTLS()
	defer server.Close()

	root := t.TempDir()
	firstGeneration := "..2026_08_02_07_00_00.000000001"
	secondGeneration := "..2026_08_02_07_05_00.000000002"
	invalidCAGeneration := "..2026_08_02_07_10_00.000000003"
	invalidTokenGeneration := "..2026_08_02_07_15_00.000000004"
	writeGeneration(t, root, firstGeneration, testProjectedTokenOne, firstCertificates.caPEM, 0o600, 0o444)
	writeGeneration(t, root, secondGeneration, testProjectedTokenTwo, secondCertificates.caPEM, 0o600, 0o444)
	writeGeneration(t, root, invalidCAGeneration, testProjectedTokenTwo, []byte("not a CA bundle"), 0o600, 0o444)
	writeGeneration(t, root, invalidTokenGeneration, "opaque", secondCertificates.caPEM, 0o600, 0o444)
	linkProjectedFiles(t, root)
	activateGenerationForTest(t, root, firstGeneration, 1)

	reader, err := New(Config{
		Enabled: true, APIServerURL: server.URL, ProjectionRoot: root, ExpectedCellKey: cellKey,
	})
	if err != nil {
		t.Fatalf("create projected Secret reader: %v", err)
	}
	if requests.Load() != 0 || connections.Load() != 0 {
		t.Fatalf("construction performed network I/O: requests=%d connections=%d", requests.Load(), connections.Load())
	}
	observe := func() {
		t.Helper()
		observation, err := reader.Observe(context.Background())
		if err != nil || observation.State != reconcile.StateAbsent || observation.CellKey != cellKey {
			t.Fatalf("observe projected Secret: observation=%#v err=%v", observation, err)
		}
	}
	observe()

	currentCertificate.Store(secondCertificates.serverCert)
	activateGenerationForTest(t, root, secondGeneration, 2)
	if err := os.RemoveAll(filepath.Join(root, firstGeneration)); err != nil {
		t.Fatalf("remove retired projection generation: %v", err)
	}
	observe()

	activateGenerationForTest(t, root, invalidCAGeneration, 3)
	if _, err := reader.Observe(context.Background()); !errors.Is(err, secretreader.ErrSecretUnavailable) {
		t.Fatalf("invalid rotated CA error = %v, want Secret unavailable", err)
	} else {
		assertSecretFree(t, err)
	}
	activateGenerationForTest(t, root, invalidTokenGeneration, 4)
	if _, err := reader.Observe(context.Background()); !errors.Is(err, secretreader.ErrCredentialUnavailable) {
		t.Fatalf("invalid rotated credential error = %v, want credential unavailable", err)
	} else {
		assertSecretFree(t, err)
	}
	activateGenerationForTest(t, root, secondGeneration, 5)
	observe()

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
		t.Fatalf("observations reused a connection or invalid input reached the network: requests=%d connections=%d", requests.Load(), connections.Load())
	}
}

func TestDisabledProjectedReaderIsInertAndRetainsNoCapabilities(t *testing.T) {
	t.Parallel()
	config := Config{
		Enabled: false, APIServerURL: "not a URL", ProjectionRoot: filepath.Join(t.TempDir(), "missing"),
		ExpectedCellKey: "not a cell",
	}
	reader, err := New(config)
	if err != nil {
		t.Fatalf("create disabled projected reader: %v", err)
	}
	if reader.Enabled() {
		t.Fatal("disabled projected reader reported enabled")
	}
	if _, err := reader.Observe(nil); !errors.Is(err, secretreader.ErrDisabled) {
		t.Fatalf("disabled observation error = %v, want disabled", err)
	}
	for _, rendered := range []string{fmt.Sprint(config), fmt.Sprintf("%#v", config), fmt.Sprint(reader), fmt.Sprintf("%#v", reader)} {
		if strings.Contains(rendered, config.APIServerURL) || strings.Contains(rendered, config.ProjectionRoot) ||
			!strings.Contains(rendered, "[REDACTED]") {
			t.Fatalf("disabled projected configuration leaked or was retained: %q", rendered)
		}
	}
}

func TestProjectedReaderAcceptsStableKubernetesFileModes(t *testing.T) {
	t.Parallel()
	cellKey := testCellKey(t)
	identity, err := materialization.SecretIdentityForCell(cellKey)
	if err != nil {
		t.Fatalf("derive Secret identity: %v", err)
	}
	certificates := issueCertificateFixture(t, 11)
	server := newTLSServer(t, certificates.serverCert, identity.SecretName)
	defer server.Close()
	for _, tokenMode := range []os.FileMode{0o400, 0o440, 0o600, 0o640} {
		t.Run(fmt.Sprintf("token-%#o", tokenMode), func(t *testing.T) {
			root := newStableFiles(t, testProjectedTokenOne+"\n", certificates.caPEM, tokenMode, 0o644)
			reader, err := New(Config{
				Enabled: true, APIServerURL: server.URL, ProjectionRoot: root, ExpectedCellKey: cellKey,
			})
			if err != nil {
				t.Fatalf("create stable projected reader: %v", err)
			}
			if observation, err := reader.Observe(context.Background()); err != nil || observation.State != reconcile.StateAbsent {
				t.Fatalf("observe through stable projected files: observation=%#v err=%v", observation, err)
			}
		})
	}
}

func TestProjectedReaderRejectsConfigurationDrift(t *testing.T) {
	t.Parallel()
	certificates := issueCertificateFixture(t, 21)
	validRoot := newStableFiles(t, testProjectedTokenOne, certificates.caPEM, 0o600, 0o444)
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
		Enabled: true, APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: validRoot,
		ExpectedCellKey: testCellKey(t), RequestTimeout: 5 * time.Second, HandshakeTimeout: 3 * time.Second,
	}
	tests := map[string]func(*Config){
		"empty URL":          func(value *Config) { value.APIServerURL = "" },
		"plaintext URL":      func(value *Config) { value.APIServerURL = "http://kubernetes.default.svc" },
		"URL root path":      func(value *Config) { value.APIServerURL = "https://kubernetes.default.svc/" },
		"URL credentials":    func(value *Config) { value.APIServerURL = "https://user@kubernetes.default.svc" },
		"URL query":          func(value *Config) { value.APIServerURL = "https://kubernetes.default.svc?x=1" },
		"URL fragment":       func(value *Config) { value.APIServerURL = "https://kubernetes.default.svc#x" },
		"invalid port":       func(value *Config) { value.APIServerURL = "https://kubernetes.default.svc:70000" },
		"empty port":         func(value *Config) { value.APIServerURL = "https://kubernetes.default.svc:" },
		"relative root":      func(value *Config) { value.ProjectionRoot = "projection" },
		"filesystem root":    func(value *Config) { value.ProjectionRoot = string(filepath.Separator) },
		"symlink root":       func(value *Config) { value.ProjectionRoot = symlinkRoot },
		"writable root":      func(value *Config) { value.ProjectionRoot = unsafeRoot },
		"invalid cell":       func(value *Config) { value.ExpectedCellKey = "backup/app-database/ABC" },
		"short request":      func(value *Config) { value.RequestTimeout = time.Second - time.Millisecond },
		"long request":       func(value *Config) { value.RequestTimeout = secretreader.MaximumRequestTimeout + time.Second },
		"fractional request": func(value *Config) { value.RequestTimeout = time.Second + time.Nanosecond },
		"short handshake":    func(value *Config) { value.HandshakeTimeout = time.Second - time.Millisecond },
		"long handshake":     func(value *Config) { value.HandshakeTimeout = maximumHandshakeTimeout + time.Second },
		"handshake over request": func(value *Config) {
			value.RequestTimeout = 2 * time.Second
			value.HandshakeTimeout = 3 * time.Second
		},
		"fractional handshake": func(value *Config) { value.HandshakeTimeout = time.Second + time.Nanosecond },
		"small response":       func(value *Config) { value.MaxResponseBytes = 100 },
		"large response":       func(value *Config) { value.MaxResponseBytes = secretreader.MaximumResponse + 1 },
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

func TestProjectedReaderRejectsUnsafeCredentialAndTrustFiles(t *testing.T) {
	t.Parallel()
	certificates := issueCertificateFixture(t, 31)
	credentialTests := map[string]func(*testing.T) string{
		"missing token": func(t *testing.T) string {
			root := t.TempDir()
			writeFile(t, filepath.Join(root, CAFileName), certificates.caPEM, 0o444)
			return root
		},
		"opaque token": func(t *testing.T) string {
			return newStableFiles(t, "opaque", certificates.caPEM, 0o600, 0o444)
		},
		"extra newline": func(t *testing.T) string {
			return newStableFiles(t, testProjectedTokenOne+"\n\n", certificates.caPEM, 0o600, 0o444)
		},
		"world-readable token": func(t *testing.T) string {
			return newStableFiles(t, testProjectedTokenOne, certificates.caPEM, 0o444, 0o444)
		},
		"oversized token": func(t *testing.T) string {
			return newStableFiles(t, strings.Repeat("a", int(maxTokenBytes)+1), certificates.caPEM, 0o600, 0o444)
		},
		"token directory": func(t *testing.T) string {
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, TokenFileName), 0o700); err != nil {
				t.Fatalf("create token directory: %v", err)
			}
			writeFile(t, filepath.Join(root, CAFileName), certificates.caPEM, 0o444)
			return root
		},
		"arbitrary token symlink": func(t *testing.T) string {
			root := t.TempDir()
			outside := filepath.Join(t.TempDir(), TokenFileName)
			writeFile(t, outside, []byte(testProjectedTokenOne), 0o600)
			if err := os.Symlink(outside, filepath.Join(root, TokenFileName)); err != nil {
				t.Fatalf("link arbitrary token: %v", err)
			}
			writeFile(t, filepath.Join(root, CAFileName), certificates.caPEM, 0o444)
			return root
		},
		"escaping data link": func(t *testing.T) string {
			root := t.TempDir()
			if err := os.Symlink(filepath.Join("..data", TokenFileName), filepath.Join(root, TokenFileName)); err != nil {
				t.Fatalf("link projected token: %v", err)
			}
			if err := os.Symlink("../outside", filepath.Join(root, "..data")); err != nil {
				t.Fatalf("link escaping generation: %v", err)
			}
			writeFile(t, filepath.Join(root, CAFileName), certificates.caPEM, 0o444)
			return root
		},
		"symlink generation": func(t *testing.T) string {
			root := t.TempDir()
			generation := "..2026_08_02_08_00_00.000000001"
			outside := t.TempDir()
			writeFile(t, filepath.Join(outside, TokenFileName), []byte(testProjectedTokenOne), 0o600)
			if err := os.Symlink(outside, filepath.Join(root, generation)); err != nil {
				t.Fatalf("link generation directory: %v", err)
			}
			if err := os.Symlink(generation, filepath.Join(root, "..data")); err != nil {
				t.Fatalf("link data generation: %v", err)
			}
			if err := os.Symlink(filepath.Join("..data", TokenFileName), filepath.Join(root, TokenFileName)); err != nil {
				t.Fatalf("link projected token: %v", err)
			}
			writeFile(t, filepath.Join(root, CAFileName), certificates.caPEM, 0o444)
			return root
		},
	}
	for name, setup := range credentialTests {
		t.Run(name, func(t *testing.T) {
			root := setup(t)
			if _, err := New(testConfig(root)); !errors.Is(err, ErrCredential) {
				t.Fatalf("credential error = %v, want credential unavailable", err)
			} else {
				assertSecretFree(t, err)
			}
		})
	}

	otherCertificates := issueCertificateFixture(t, 32)
	trustTests := map[string]struct {
		ca   []byte
		mode os.FileMode
	}{
		"empty CA":           {ca: nil, mode: 0o444},
		"invalid PEM":        {ca: []byte("not a CA bundle"), mode: 0o444},
		"non-CA certificate": {ca: certificates.leafPEM, mode: 0o444},
		"trailing data":      {ca: append(append([]byte(nil), certificates.caPEM...), []byte("trailing")...), mode: 0o444},
		"writable CA":        {ca: certificates.caPEM, mode: 0o666},
		"too many CAs":       {ca: repeatBytes(certificates.caPEM, maxCACertificates+1), mode: 0o444},
		"oversized CA":       {ca: repeatBytes([]byte(" "), int(maxCABundleBytes)+1), mode: 0o444},
	}
	for name, fixture := range trustTests {
		t.Run(name, func(t *testing.T) {
			root := newStableFiles(t, testProjectedTokenOne, fixture.ca, 0o600, fixture.mode)
			if _, err := New(testConfig(root)); !errors.Is(err, ErrTrust) {
				t.Fatalf("trust error = %v, want trust unavailable", err)
			} else {
				assertSecretFree(t, err)
			}
		})
	}
	validBundle := append(append([]byte(nil), certificates.caPEM...), otherCertificates.caPEM...)
	root := newStableFiles(t, testProjectedTokenOne, validBundle, 0o600, 0o444)
	projection, err := openProjection(root)
	if err != nil {
		t.Fatalf("open multi-CA projection: %v", err)
	}
	if _, err := projection.loadCAPool(context.Background()); err != nil {
		t.Fatalf("strict multi-CA bundle rejected: %v", err)
	}
}

func TestRotatingReaderTransportRejectsCapabilityDriftBeforeNetwork(t *testing.T) {
	t.Parallel()
	certificates := issueCertificateFixture(t, 41)
	root := newStableFiles(t, testProjectedTokenOne, certificates.caPEM, 0o600, 0o444)
	projection, err := openProjection(root)
	if err != nil {
		t.Fatalf("open transport projection: %v", err)
	}
	expectedPath := "/api/v1/namespaces/fugue-system/secrets/fugue-backup-observer-app-database-0123456789abcdef-input"
	transport := &rotatingCATransport{
		projection: projection, expectedAuthority: "kubernetes.default.svc", expectedPath: expectedPath,
		requestTimeout: 5 * time.Second, handshakeTimeout: 3 * time.Second,
	}
	tests := map[string]func(*testing.T) *http.Request{
		"plaintext": func(t *testing.T) *http.Request {
			return newTransportRequest(t, http.MethodGet, "http://kubernetes.default.svc"+expectedPath, nil)
		},
		"wrong authority": func(t *testing.T) *http.Request {
			return newTransportRequest(t, http.MethodGet, "https://127.0.0.1:1"+expectedPath, nil)
		},
		"URL credentials": func(t *testing.T) *http.Request {
			return newTransportRequest(t, http.MethodGet, "https://user@kubernetes.default.svc"+expectedPath, nil)
		},
		"wrong path": func(t *testing.T) *http.Request {
			return newTransportRequest(t, http.MethodGet, "https://kubernetes.default.svc/api/v1/namespaces/default/secrets/other", nil)
		},
		"query": func(t *testing.T) *http.Request {
			return newTransportRequest(t, http.MethodGet, "https://kubernetes.default.svc"+expectedPath+"?x=1", nil)
		},
		"method": func(t *testing.T) *http.Request {
			return newTransportRequest(t, http.MethodPost, "https://kubernetes.default.svc"+expectedPath, nil)
		},
		"body": func(t *testing.T) *http.Request {
			return newTransportRequest(t, http.MethodGet, "https://kubernetes.default.svc"+expectedPath, strings.NewReader("body"))
		},
		"host override": func(t *testing.T) *http.Request {
			request := newTransportRequest(t, http.MethodGet, "https://kubernetes.default.svc"+expectedPath, nil)
			request.Host = "other.example.test"
			return request
		},
		"fragment": func(t *testing.T) *http.Request {
			return newTransportRequest(t, http.MethodGet, "https://kubernetes.default.svc"+expectedPath+"#fragment", nil)
		},
		"keep alive": func(t *testing.T) *http.Request {
			request := newTransportRequest(t, http.MethodGet, "https://kubernetes.default.svc"+expectedPath, nil)
			request.Close = false
			return request
		},
		"transfer encoding": func(t *testing.T) *http.Request {
			request := newTransportRequest(t, http.MethodGet, "https://kubernetes.default.svc"+expectedPath, nil)
			request.TransferEncoding = []string{"chunked"}
			return request
		},
		"extra header": func(t *testing.T) *http.Request {
			request := newTransportRequest(t, http.MethodGet, "https://kubernetes.default.svc"+expectedPath, nil)
			request.Header.Set("X-Extra", "value")
			return request
		},
		"invalid authorization": func(t *testing.T) *http.Request {
			request := newTransportRequest(t, http.MethodGet, "https://kubernetes.default.svc"+expectedPath, nil)
			request.Header.Set("Authorization", "Bearer opaque")
			return request
		},
	}
	for name, build := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := transport.RoundTrip(build(t)); !errors.Is(err, ErrTrust) {
				t.Fatalf("capability drift error = %v, want trust unavailable", err)
			}
		})
	}
	if _, err := (*rotatingCATransport)(nil).RoundTrip(nil); !errors.Is(err, ErrTrust) {
		t.Fatalf("nil transport error = %v", err)
	}
}

func TestProjectedCredentialReadsOnlyCompleteAtomicGenerations(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	certificates := issueCertificateFixture(t, 51)
	firstGeneration := "..2026_08_02_09_00_00.000000001"
	secondGeneration := "..2026_08_02_09_05_00.000000002"
	writeGeneration(t, root, firstGeneration, testProjectedTokenOne, certificates.caPEM, 0o640, 0o444)
	writeGeneration(t, root, secondGeneration, testProjectedTokenTwo, certificates.caPEM, 0o640, 0o444)
	linkProjectedFiles(t, root)
	activateGenerationForTest(t, root, firstGeneration, 1)
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
			if err := activateGeneration(root, generation, index+2); err != nil {
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
	if _, err := (*credentialSource)(nil).Credential(context.Background()); !errors.Is(err, ErrCredential) {
		t.Fatalf("nil credential source error = %v", err)
	}
}

func TestProjectedReaderProductionDependencyBoundary(t *testing.T) {
	t.Parallel()
	command := exec.Command("go", "list", "-deps", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list projected reader dependencies: %v", err)
	}
	local := []string{}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{
			"database/sql", "os/exec", "fugue/internal/backupidentity", "fugue/internal/backupmaterializer",
		} {
			if dependency == forbidden {
				t.Fatalf("projected reader gained forbidden dependency %q", dependency)
			}
		}
		for _, prefix := range []string{
			"k8s.io/", "fugue/internal/api", "fugue/internal/store", "fugue/internal/model",
			"fugue/internal/backupmaterializer/client", "fugue/internal/backupmaterializerreview",
			"fugue/internal/backupmaterializeridentity",
		} {
			if strings.HasPrefix(dependency, prefix) {
				t.Fatalf("projected reader crossed component boundary through %q", dependency)
			}
		}
	}
	sort.Strings(local)
	wantLocal := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconcile",
		"fugue/internal/backupmaterializer/secretreader",
		"fugue/internal/backupmaterializer/secretreader/projected",
	}
	if !reflect.DeepEqual(local, wantLocal) {
		t.Fatalf("projected reader local closure drifted: got=%v want=%v", local, wantLocal)
	}
	direct := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	directOutput, err := direct.Output()
	if err != nil {
		t.Fatalf("list direct projected reader imports: %v", err)
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
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/secretreader",
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
		t.Fatalf("projected reader direct imports drifted: got=%v want=%v", gotDirect, wantDirect)
	}
}

func testConfig(root string) Config {
	return Config{
		Enabled: true, APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: root,
		ExpectedCellKey: "backup/app-database/0123456789abcdef",
	}
}

func testCellKey(t *testing.T) string {
	t.Helper()
	cellKey := backupcontrol.BackupCellKey(backupcontrol.BackupTarget{
		Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database",
	})
	if cellKey == "" {
		t.Fatal("derive test cell key")
	}
	return cellKey
}

func newTransportRequest(t *testing.T, method, rawURL string, body io.Reader) *http.Request {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), method, rawURL, body)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	request.Close = true
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("Authorization", "Bearer "+testProjectedTokenOne)
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("User-Agent", secretreader.RequestUserAgent)
	return request
}

func newTLSServer(t *testing.T, certificate *tls.Certificate, secretName string) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeNotFoundResponse(writer, secretName)
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{*certificate}}
	server.StartTLS()
	return server
}

func writeNotFoundResponse(writer http.ResponseWriter, secretName string) {
	document := []byte(fmt.Sprintf(
		`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"NotFound","details":{"name":%q,"kind":"secrets","group":""},"code":404}`,
		secretName,
	))
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Content-Length", fmt.Sprint(len(document)))
	writer.WriteHeader(http.StatusNotFound)
	_, _ = writer.Write(document)
}

func issueCertificateFixture(t *testing.T, serial int64) certificateFixture {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate test CA key: %v", err)
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial*10 + 1), Subject: pkix.Name{CommonName: "fugue projected Secret reader test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
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
		SerialNumber: big.NewInt(serial*10 + 2), Subject: pkix.Name{CommonName: "fugue projected Secret reader test server"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(12 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames: []string{"localhost"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create test server certificate: %v", err)
	}
	return certificateFixture{
		caPEM:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		leafPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		serverCert: &tls.Certificate{
			Certificate: [][]byte{leafDER, caDER}, PrivateKey: leafKey,
		},
	}
}

func newStableFiles(t *testing.T, token string, caPEM []byte, tokenMode, caMode os.FileMode) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, filepath.Join(root, TokenFileName), []byte(token), tokenMode)
	writeFile(t, filepath.Join(root, CAFileName), caPEM, caMode)
	return root
}

func writeGeneration(
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
	writeFile(t, filepath.Join(directory, TokenFileName), []byte(token), tokenMode)
	writeFile(t, filepath.Join(directory, CAFileName), caPEM, caMode)
}

func linkProjectedFiles(t *testing.T, root string) {
	t.Helper()
	for _, name := range []string{TokenFileName, CAFileName} {
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(root, name)); err != nil {
			t.Fatalf("link projected %s: %v", name, err)
		}
	}
}

func activateGenerationForTest(t *testing.T, root, generation string, sequence int) {
	t.Helper()
	if err := activateGeneration(root, generation, sequence); err != nil {
		t.Fatalf("activate projection generation: %v", err)
	}
}

func activateGeneration(root, generation string, sequence int) error {
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

func writeFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("write projected test file: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("set projected test file mode: %v", err)
	}
}

func repeatBytes(value []byte, count int) []byte {
	result := make([]byte, 0, len(value)*count)
	for range count {
		result = append(result, value...)
	}
	return result
}

func assertSecretFree(t *testing.T, err error) {
	t.Helper()
	rendered := fmt.Sprintf("%+v", err)
	for _, secret := range []string{testProjectedTokenOne, testProjectedTokenTwo, "opaque", "not a CA bundle"} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("projected reader error leaked private input %q: %v", secret, err)
		}
	}
}

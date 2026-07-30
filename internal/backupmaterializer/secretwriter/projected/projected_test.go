package projected

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
	"fugue/internal/backupmaterializer/secretwriter"
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

func TestWriterReloadsAtomicCredentialCAAndConnections(t *testing.T) {
	t.Parallel()
	now := testNow()
	previous := testPlan(t, "run-projected-previous", now)
	desired := testPlan(t, "run-projected-desired", now)
	create := testCreateDecision(t, previous, now)
	replace := testReplaceDecision(t, previous, desired, now)
	firstCertificates := issueCertificateFixture(t, 1)
	secondCertificates := issueCertificateFixture(t, 2)
	var currentCertificate atomic.Pointer[tls.Certificate]
	currentCertificate.Store(firstCertificates.serverCert)
	var connections atomic.Int64
	var requests atomic.Int64
	var authorizationsMu sync.Mutex
	authorizations := []string{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		authorizationsMu.Lock()
		authorizations = append(authorizations, request.Header.Get("Authorization"))
		authorizationsMu.Unlock()
		document, err := io.ReadAll(io.LimitReader(request.Body, secretwriter.MaximumRequestBytes+1))
		if err != nil || int64(len(document)) != request.ContentLength ||
			secretwriter.ValidateTransportRequest(request.Method, request.URL.Path, request.URL.RawQuery, previous.CellKey, document) != nil ||
			request.Header.Get("Accept") != "application/json" || request.Header.Get("Accept-Encoding") != "identity" ||
			request.Header.Get("Content-Type") != "application/json" || request.Header.Get("User-Agent") != secretwriter.RequestUserAgent {
			t.Errorf("projected dry-run request drifted: method=%s uri=%s length=%d headers=%v err=%v", request.Method, request.URL.RequestURI(), request.ContentLength, request.Header, err)
		}
		status := http.StatusCreated
		if request.Method == http.MethodPut {
			status = http.StatusOK
		}
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Content-Length", fmt.Sprint(len(document)))
		response.WriteHeader(status)
		_, _ = response.Write(document)
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
	firstGeneration := "..2026_08_02_10_00_00.000000001"
	secondGeneration := "..2026_08_02_10_05_00.000000002"
	invalidCAGeneration := "..2026_08_02_10_10_00.000000003"
	invalidTokenGeneration := "..2026_08_02_10_15_00.000000004"
	writeGeneration(t, root, firstGeneration, testProjectedTokenOne, firstCertificates.caPEM, 0o600, 0o444)
	writeGeneration(t, root, secondGeneration, testProjectedTokenTwo, secondCertificates.caPEM, 0o600, 0o444)
	writeGeneration(t, root, invalidCAGeneration, testProjectedTokenTwo, []byte("not a CA"), 0o600, 0o444)
	writeGeneration(t, root, invalidTokenGeneration, "opaque", secondCertificates.caPEM, 0o600, 0o444)
	linkProjectedFiles(t, root)
	activateGenerationForTest(t, root, firstGeneration, 1)

	writer, err := New(Config{
		Enabled: true, APIServerURL: server.URL, ProjectionRoot: root, ExpectedCellKey: previous.CellKey,
		RequestTimeout: 5 * time.Second, HandshakeTimeout: 3 * time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("construct projected writer: %v", err)
	}
	first, err := writer.DryRun(context.Background(), previous, create)
	if err != nil || secretwriter.ValidateResult(first) != nil || first.Action != reconcile.ActionCreateIfAbsent ||
		connections.Load() != 1 || requests.Load() != 1 {
		t.Fatalf("first projected dry-run: result=%#v err=%v connections=%d requests=%d", first, err, connections.Load(), requests.Load())
	}

	activateGenerationForTest(t, root, secondGeneration, 2)
	currentCertificate.Store(secondCertificates.serverCert)
	second, err := writer.DryRun(context.Background(), desired, replace)
	if err != nil || secretwriter.ValidateResult(second) != nil || second.Action != reconcile.ActionReplaceResourceVersionCAS ||
		connections.Load() != 2 || requests.Load() != 2 {
		t.Fatalf("rotated projected dry-run: result=%#v err=%v connections=%d requests=%d", second, err, connections.Load(), requests.Load())
	}

	activateGenerationForTest(t, root, invalidCAGeneration, 3)
	if _, err := writer.DryRun(context.Background(), desired, replace); !errors.Is(err, secretwriter.ErrUnavailable) {
		t.Fatalf("invalid rotated CA error = %v, want unavailable", err)
	}
	activateGenerationForTest(t, root, secondGeneration, 4)
	if _, err := writer.DryRun(context.Background(), desired, replace); err != nil {
		t.Fatalf("writer did not recover after CA rotation: %v", err)
	}

	requestsBeforeInvalidToken := requests.Load()
	activateGenerationForTest(t, root, invalidTokenGeneration, 5)
	if _, err := writer.DryRun(context.Background(), desired, replace); !errors.Is(err, secretwriter.ErrCredentialUnavailable) {
		t.Fatalf("invalid rotated token error = %v, want credential unavailable", err)
	}
	if requests.Load() != requestsBeforeInvalidToken {
		t.Fatalf("invalid credential reached network: before=%d after=%d", requestsBeforeInvalidToken, requests.Load())
	}
	activateGenerationForTest(t, root, secondGeneration, 6)
	if _, err := writer.DryRun(context.Background(), desired, replace); err != nil {
		t.Fatalf("writer did not recover after token rotation: %v", err)
	}

	authorizationsMu.Lock()
	gotAuthorizations := append([]string(nil), authorizations...)
	authorizationsMu.Unlock()
	if len(gotAuthorizations) < 4 || gotAuthorizations[0] != "Bearer "+testProjectedTokenOne ||
		gotAuthorizations[1] != "Bearer "+testProjectedTokenTwo || gotAuthorizations[len(gotAuthorizations)-1] != "Bearer "+testProjectedTokenTwo {
		t.Fatalf("projected credentials did not rotate/recover: %v", gotAuthorizations)
	}
}

func TestDisabledProjectedWriterPerformsNoFilesystemAccess(t *testing.T) {
	t.Parallel()
	writer, err := New(Config{
		Enabled: false, APIServerURL: "private invalid URL", ProjectionRoot: "/private/missing",
		ExpectedCellKey: "private invalid cell", RequestTimeout: -time.Second, HandshakeTimeout: -time.Second,
		MaxResponseBytes: -1, Now: func() time.Time { panic("clock used") },
	})
	if err != nil || writer.Enabled() {
		t.Fatalf("disabled projected writer = %#v err=%v", writer, err)
	}
	config := Config{APIServerURL: "private endpoint", ProjectionRoot: "/private/projection"}
	if strings.Contains(fmt.Sprintf("%#v", config), "private") || !strings.Contains(fmt.Sprintf("%#v", config), "[REDACTED]") {
		t.Fatalf("projected config leaked: %#v", config)
	}
}

func TestProjectedWriterRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()
	certificates := issueCertificateFixture(t, 11)
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
		ExpectedCellKey: testPlan(t, "run-config", testNow()).CellKey,
		RequestTimeout:  5 * time.Second, HandshakeTimeout: 3 * time.Second,
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
		"long request":       func(value *Config) { value.RequestTimeout = secretwriter.MaximumRequestTimeout + time.Second },
		"fractional request": func(value *Config) { value.RequestTimeout = time.Second + time.Nanosecond },
		"short handshake":    func(value *Config) { value.HandshakeTimeout = time.Second - time.Millisecond },
		"long handshake":     func(value *Config) { value.HandshakeTimeout = maximumHandshakeTimeout + time.Second },
		"handshake over request": func(value *Config) {
			value.RequestTimeout = 2 * time.Second
			value.HandshakeTimeout = 3 * time.Second
		},
		"fractional handshake": func(value *Config) { value.HandshakeTimeout = time.Second + time.Nanosecond },
		"small response":       func(value *Config) { value.MaxResponseBytes = 100 },
		"large response":       func(value *Config) { value.MaxResponseBytes = secretwriter.MaximumResponse + 1 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if _, err := New(candidate); !errors.Is(err, ErrConfig) {
				t.Fatalf("configuration error = %v, want ErrConfig", err)
			}
		})
	}
}

func TestProjectedWriterRejectsUnsafeCredentialAndTrustFiles(t *testing.T) {
	t.Parallel()
	certificates := issueCertificateFixture(t, 21)
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
	}
	for name, setup := range credentialTests {
		t.Run(name, func(t *testing.T) {
			if _, err := New(testConfig(setup(t))); !errors.Is(err, ErrCredential) {
				t.Fatalf("credential error = %v, want ErrCredential", err)
			} else {
				assertSecretFree(t, err)
			}
		})
	}

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
				t.Fatalf("trust error = %v, want ErrTrust", err)
			} else {
				assertSecretFree(t, err)
			}
		})
	}
}

func TestRotatingWriterTransportRejectsCapabilityDriftBeforeNetwork(t *testing.T) {
	t.Parallel()
	now := testNow()
	plan := testPlan(t, "run-transport", now)
	decision := testCreateDecision(t, plan, now)
	certificates := issueCertificateFixture(t, 31)
	root := newStableFiles(t, testProjectedTokenOne, certificates.caPEM, 0o600, 0o444)
	projection, err := openProjection(root)
	if err != nil {
		t.Fatalf("open transport projection: %v", err)
	}
	collectionPath := "/api/v1/namespaces/" + plan.Namespace + "/secrets"
	itemPath := collectionPath + "/" + plan.SecretName
	transport := &rotatingCATransport{
		projection: projection, expectedAuthority: "kubernetes.default.svc", expectedCellKey: plan.CellKey,
		collectionPath: collectionPath, itemPath: itemPath, requestTimeout: 5 * time.Second, handshakeTimeout: 3 * time.Second,
	}
	valid := func(t *testing.T) *http.Request { return newTransportRequest(t, plan, decision) }
	tests := map[string]func(*testing.T) *http.Request{
		"plaintext": func(t *testing.T) *http.Request {
			request := valid(t)
			request.URL.Scheme = "http"
			return request
		},
		"wrong authority": func(t *testing.T) *http.Request {
			request := valid(t)
			request.URL.Host = "127.0.0.1:1"
			return request
		},
		"URL credentials": func(t *testing.T) *http.Request {
			request := valid(t)
			request.URL.User = urlUser("user")
			return request
		},
		"wrong path": func(t *testing.T) *http.Request {
			request := valid(t)
			request.URL.Path = "/api/v1/namespaces/default/secrets/other"
			return request
		},
		"live query": func(t *testing.T) *http.Request {
			request := valid(t)
			request.URL.RawQuery = "fieldManager=fugue-backup-materializer"
			return request
		},
		"method": func(t *testing.T) *http.Request {
			request := valid(t)
			request.Method = http.MethodDelete
			return request
		},
		"PUT collection": func(t *testing.T) *http.Request {
			request := valid(t)
			request.Method = http.MethodPut
			return request
		},
		"nil body": func(t *testing.T) *http.Request {
			request := valid(t)
			request.Body = nil
			return request
		},
		"non-replayable input": func(t *testing.T) *http.Request {
			request := valid(t)
			request.GetBody = nil
			return request
		},
		"length mismatch": func(t *testing.T) *http.Request {
			request := valid(t)
			request.ContentLength++
			return request
		},
		"invalid body": func(t *testing.T) *http.Request {
			request := valid(t)
			document := []byte(`{}`)
			request.Body = io.NopCloser(bytes.NewReader(document))
			request.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(document)), nil }
			request.ContentLength = int64(len(document))
			return request
		},
		"host override": func(t *testing.T) *http.Request {
			request := valid(t)
			request.Host = "other.example.test"
			return request
		},
		"fragment": func(t *testing.T) *http.Request {
			request := valid(t)
			request.URL.Fragment = "fragment"
			return request
		},
		"keep alive": func(t *testing.T) *http.Request {
			request := valid(t)
			request.Close = false
			return request
		},
		"transfer encoding": func(t *testing.T) *http.Request {
			request := valid(t)
			request.TransferEncoding = []string{"chunked"}
			return request
		},
		"extra header": func(t *testing.T) *http.Request {
			request := valid(t)
			request.Header.Set("X-Extra", "value")
			return request
		},
		"invalid authorization": func(t *testing.T) *http.Request {
			request := valid(t)
			request.Header.Set("Authorization", "Bearer opaque")
			return request
		},
		"wrong content type": func(t *testing.T) *http.Request {
			request := valid(t)
			request.Header.Set("Content-Type", "application/merge-patch+json")
			return request
		},
	}
	for name, build := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := transport.RoundTrip(build(t)); !errors.Is(err, ErrTrust) {
				t.Fatalf("capability drift error = %v, want ErrTrust", err)
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
	certificates := issueCertificateFixture(t, 41)
	firstGeneration := "..2026_08_02_11_00_00.000000001"
	secondGeneration := "..2026_08_02_11_05_00.000000002"
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
				t.Fatalf("rotation read error = %v", err)
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
}

func TestProjectedWriterProductionDependencyBoundary(t *testing.T) {
	t.Parallel()
	output, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("list projected writer dependencies: %v", err)
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
				t.Fatalf("projected writer gained forbidden dependency %q", dependency)
			}
		}
		for _, prefix := range []string{
			"k8s.io/", "fugue/internal/api", "fugue/internal/store", "fugue/internal/model",
			"fugue/internal/backupmaterializer/client", "fugue/internal/backupmaterializer/secretreader",
			"fugue/internal/backupmaterializerreview", "fugue/internal/backupmaterializeridentity",
		} {
			if strings.HasPrefix(dependency, prefix) {
				t.Fatalf("projected writer crossed component boundary through %q", dependency)
			}
		}
	}
	sort.Strings(local)
	wantLocal := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconcile",
		"fugue/internal/backupmaterializer/secretwriter",
		"fugue/internal/backupmaterializer/secretwriter/projected",
	}
	if !reflect.DeepEqual(local, wantLocal) {
		t.Fatalf("projected writer local closure drifted: got=%v want=%v", local, wantLocal)
	}
	directOutput, err := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".").Output()
	if err != nil {
		t.Fatalf("list direct projected writer imports: %v", err)
	}
	gotDirect := strings.Fields(string(directOutput))
	sort.Strings(gotDirect)
	wantDirect := []string{
		"bytes", "context", "crypto/tls", "crypto/x509", "encoding/pem", "errors",
		"fugue/internal/backupmaterializer/materialization", "fugue/internal/backupmaterializer/secretwriter",
		"io", "net", "net/http", "net/url", "os", "path/filepath", "strconv", "strings", "time",
	}
	sort.Strings(wantDirect)
	if !reflect.DeepEqual(gotDirect, wantDirect) {
		t.Fatalf("projected writer direct imports drifted: got=%v want=%v", gotDirect, wantDirect)
	}
}

func newTransportRequest(t *testing.T, plan materialization.Plan, decision reconcile.Decision) *http.Request {
	t.Helper()
	now := decision.DecidedAt
	manifest, err := reconcile.BuildManifest(plan, now)
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	data, err := plan.Data(now)
	if err != nil {
		t.Fatalf("read plan data: %v", err)
	}
	immutable := false
	document, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name": manifest.SecretName, "namespace": manifest.Namespace,
			"labels": manifest.Labels, "annotations": manifest.Annotations,
		},
		"immutable": &immutable,
		"type":      manifest.SecretType,
		"data": map[string]string{
			data.SpecKey: base64.StdEncoding.EncodeToString(data.SpecDocument), data.TokenKey: base64.StdEncoding.EncodeToString(data.ObserverToken),
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	rawURL := "https://kubernetes.default.svc/api/v1/namespaces/" + plan.Namespace + "/secrets?" + secretwriter.DryRunRawQuery
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, rawURL, bytes.NewReader(document))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	request.Close = true
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "identity")
	request.Header.Set("Authorization", "Bearer "+testProjectedTokenOne)
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", secretwriter.RequestUserAgent)
	return request
}

func testConfig(root string) Config {
	return Config{
		Enabled: true, APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: root,
		ExpectedCellKey: testPlanCellKey(),
	}
}

func testPlanCellKey() string {
	return backupcontrol.BackupCellKey(backupcontrol.BackupTarget{
		Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database",
	})
}

func testCreateDecision(t *testing.T, plan materialization.Plan, now time.Time) reconcile.Decision {
	t.Helper()
	current, err := reconcile.ObserveAbsent(plan.CellKey)
	if err != nil {
		t.Fatalf("observe absent: %v", err)
	}
	decision, err := reconcile.Decide(plan.CellKey, &plan, current, now)
	if err != nil {
		t.Fatalf("decide create: %v", err)
	}
	return decision
}

func testReplaceDecision(t *testing.T, previous, desired materialization.Plan, now time.Time) reconcile.Decision {
	t.Helper()
	manifest, err := reconcile.BuildManifest(previous, now)
	if err != nil {
		t.Fatalf("build previous manifest: %v", err)
	}
	data, err := previous.Data(now)
	if err != nil {
		t.Fatalf("read previous data: %v", err)
	}
	snapshot, err := reconcile.SealCurrent(previous, reconcile.SecretEvidence{
		Namespace: previous.Namespace, SecretName: previous.SecretName, UID: "01234567-89ab-cdef-0123-456789abcdef",
		ResourceVersion: "42", SecretType: manifest.SecretType, Labels: cloneMap(manifest.Labels),
		Annotations: cloneMap(manifest.Annotations), Data: map[string][]byte{
			data.SpecKey: append([]byte(nil), data.SpecDocument...), data.TokenKey: append([]byte(nil), data.ObserverToken...),
		},
	})
	if err != nil {
		t.Fatalf("seal previous: %v", err)
	}
	current, err := reconcile.ObserveManaged(snapshot)
	if err != nil {
		t.Fatalf("observe previous: %v", err)
	}
	decision, err := reconcile.Decide(desired.CellKey, &desired, current, now)
	if err != nil {
		t.Fatalf("decide replacement: %v", err)
	}
	return decision
}

func testPlan(t *testing.T, runID string, now time.Time) materialization.Plan {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		runID, runID, backupcontrol.BackupTarget{Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database"},
		"backend-1", "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 4, 120, 1800,
	)
	if err != nil {
		t.Fatalf("build backup spec: %v", err)
	}
	issuedAt := now.Add(-30 * time.Second)
	tokenID := "AAAAAAAAAAAAAAAAAAAAAA"
	claims := struct {
		Version       string `json:"v"`
		CredentialID  string `json:"credential_id"`
		TokenID       string `json:"token_id"`
		RunID         string `json:"run_id"`
		TenantID      string `json:"tenant_id"`
		CellKey       string `json:"cell_key"`
		SpecDigest    string `json:"spec_digest"`
		Permission    string `json:"permission"`
		IssuedAtUnix  int64  `json:"issued_at"`
		ExpiresAtUnix int64  `json:"expires_at"`
	}{
		Version: "v1", CredentialID: materializercontract.CredentialIDForCell(spec.CellKey), TokenID: tokenID,
		RunID: spec.RunID, TenantID: "tenant-1", CellKey: spec.CellKey, SpecDigest: spec.Digest,
		Permission: materializercontract.ObserverIdentityPermission, IssuedAtUnix: issuedAt.Unix(),
		ExpiresAtUnix: issuedAt.Add(materializercontract.ObserverIdentityTTL).Unix(),
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	observerToken := "fugue_bo_v1." + base64.RawURLEncoding.EncodeToString([]byte("backup-key-1")) + "." +
		base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{'s'}, 32))
	bundle := materializercontract.ObserverInputBundle{
		APIVersion: materializercontract.ObserverInputBundleAPIVersion, Kind: materializercontract.ObserverInputBundleKind,
		Policy: materializercontract.ObserverInputBundlePolicy, CellKey: spec.CellKey, RunID: spec.RunID,
		SpecDigest: spec.Digest, CredentialID: claims.CredentialID, TokenID: tokenID, DesiredSpec: spec,
		ObserverToken: observerToken, IssuedAt: issuedAt,
		RenewAfter: issuedAt.Add(materializercontract.ObserverIdentityRenewAfter),
		ExpiresAt:  issuedAt.Add(materializercontract.ObserverIdentityTTL), ObservationOnly: true,
	}
	bundle.Digest = materializercontract.DigestObserverInputBundle(bundle)
	plan, err := materialization.Build(bundle, now)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}
	return plan
}

func testNow() time.Time { return time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC) }

func cloneMap(value map[string]string) map[string]string {
	result := make(map[string]string, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func issueCertificateFixture(t *testing.T, serial int64) certificateFixture {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial*10 + 1), Subject: pkix.Name{CommonName: "fugue projected Secret writer test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial*10 + 2), Subject: pkix.Name{CommonName: "fugue projected Secret writer test server"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(12 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames: []string{"localhost"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
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

func writeGeneration(t *testing.T, root, generation, token string, caPEM []byte, tokenMode, caMode os.FileMode) {
	t.Helper()
	directory := filepath.Join(root, generation)
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatalf("create generation: %v", err)
	}
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatalf("chmod generation: %v", err)
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
		t.Fatalf("activate generation %s: %v", generation, err)
	}
}

func activateGeneration(root, generation string, sequence int) error {
	temporary := filepath.Join(root, fmt.Sprintf("..data_tmp_%d", sequence))
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
		t.Fatalf("write %s: %v", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
}

func repeatBytes(value []byte, count int) []byte {
	return bytes.Repeat(value, count)
}

func assertSecretFree(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, forbidden := range []string{testProjectedTokenOne, "BEGIN CERTIFICATE", "/private/"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("error leaked private material: %v", err)
		}
	}
}

func urlUser(username string) *url.Userinfo {
	return url.User(username)
}

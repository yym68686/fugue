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
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/backupmaterializeridentity"
	"fugue/internal/backupmaterializerreview"

	authenticationv1 "k8s.io/api/authentication/v1"
)

const (
	testPresentedToken = "test-header.test-materializer-payload.test-materializer-signature"
	testCallerTokenOne = "caller-one.caller-one-payload.caller-one-signature"
	testCallerTokenTwo = "caller-two.caller-two-payload.caller-two-signature"
)

type certificateFixture struct {
	caPEM      []byte
	leafPEM    []byte
	serverCert *tls.Certificate
}

func TestReviewerReloadsAtomicCredentialAndCA(t *testing.T) {
	firstCertificates := issueCertificateFixture(t, 1)
	secondCertificates := issueCertificateFixture(t, 2)
	var currentCertificate atomic.Pointer[tls.Certificate]
	currentCertificate.Store(firstCertificates.serverCert)
	var authorizationsMu sync.Mutex
	authorizations := []string{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		authorizationsMu.Lock()
		authorizations = append(authorizations, request.Header.Get("Authorization"))
		authorizationsMu.Unlock()
		var review authenticationv1.TokenReview
		if err := json.NewDecoder(request.Body).Decode(&review); err != nil {
			http.Error(w, "invalid review", http.StatusBadRequest)
			return
		}
		response := review
		response.Status = authenticationv1.TokenReviewStatus{
			Authenticated: true,
			Audiences:     []string{backupmaterializeridentity.Audience},
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(response)
	}))
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
	firstGeneration := "..2026_07_30_10_00_00.000000001"
	secondGeneration := "..2026_07_30_10_05_00.000000002"
	invalidGeneration := "..2026_07_30_10_10_00.000000003"
	writeProjectionGeneration(t, root, firstGeneration, testCallerTokenOne, firstCertificates.caPEM, 0o440, 0o444)
	writeProjectionGeneration(t, root, secondGeneration, testCallerTokenTwo, secondCertificates.caPEM, 0o440, 0o444)
	writeProjectionGeneration(t, root, invalidGeneration, testCallerTokenTwo, []byte("not a CA bundle"), 0o440, 0o444)
	linkProjectionFiles(t, root)
	activateProjectionForTest(t, root, firstGeneration, 1)

	reviewer, err := New(Config{APIServerURL: server.URL, ProjectionRoot: root})
	if err != nil {
		t.Fatalf("create projected reviewer: %v", err)
	}
	review := func() {
		t.Helper()
		result, err := reviewer.ReviewToken(
			context.Background(),
			testPresentedToken,
			[]string{backupmaterializeridentity.Audience},
		)
		if err != nil || !result.Authenticated || !reflect.DeepEqual(result.Audiences, []string{backupmaterializeridentity.Audience}) {
			t.Fatalf("review projected credential: result=%+v err=%v", result, err)
		}
	}
	review()

	currentCertificate.Store(secondCertificates.serverCert)
	activateProjectionForTest(t, root, secondGeneration, 2)
	if err := os.RemoveAll(filepath.Join(root, firstGeneration)); err != nil {
		t.Fatalf("remove retired projection generation: %v", err)
	}
	review()

	activateProjectionForTest(t, root, invalidGeneration, 3)
	_, err = reviewer.ReviewToken(
		context.Background(),
		testPresentedToken,
		[]string{backupmaterializeridentity.Audience},
	)
	if !errors.Is(err, backupmaterializerreview.ErrReviewerUnavailable) {
		t.Fatalf("invalid rotated CA error = %v, want reviewer unavailable", err)
	}
	assertNoTestCredential(t, err)
	activateProjectionForTest(t, root, secondGeneration, 4)
	review()

	authorizationsMu.Lock()
	defer authorizationsMu.Unlock()
	want := []string{
		"Bearer " + testCallerTokenOne,
		"Bearer " + testCallerTokenTwo,
		"Bearer " + testCallerTokenTwo,
	}
	if !reflect.DeepEqual(authorizations, want) {
		t.Fatalf("projected caller credential did not rotate or recover: got=%v want=%v", authorizations, want)
	}
}

func TestReviewerAcceptsSafeStableProjectionFiles(t *testing.T) {
	certificates := issueCertificateFixture(t, 11)
	server := newFixedTLSServer(t, certificates.serverCert)
	defer server.Close()
	root := t.TempDir()
	writeFile(t, filepath.Join(root, TokenFileName), []byte(testCallerTokenOne+"\n"), 0o440)
	writeFile(t, filepath.Join(root, CAFileName), certificates.caPEM, 0o444)
	reviewer, err := New(Config{APIServerURL: server.URL, ProjectionRoot: root})
	if err != nil {
		t.Fatalf("create stable-file reviewer: %v", err)
	}
	if _, err := reviewer.ReviewToken(
		context.Background(),
		testPresentedToken,
		[]string{backupmaterializeridentity.Audience},
	); err != nil {
		t.Fatalf("review through stable projection files: %v", err)
	}
}

func TestNewRejectsConfigurationDrift(t *testing.T) {
	certificates := issueCertificateFixture(t, 21)
	validRoot := newStableProjection(t, testCallerTokenOne, certificates.caPEM, 0o440, 0o444)
	unsafeRoot := t.TempDir()
	if err := os.Chmod(unsafeRoot, 0o777); err != nil {
		t.Fatalf("chmod unsafe projection root: %v", err)
	}
	realRoot := t.TempDir()
	symlinkRoot := filepath.Join(t.TempDir(), "projection")
	if err := os.Symlink(realRoot, symlinkRoot); err != nil {
		t.Fatalf("link projection root: %v", err)
	}
	tests := map[string]Config{
		"empty API URL":        {ProjectionRoot: validRoot},
		"plaintext API URL":    {APIServerURL: "http://kubernetes.default.svc", ProjectionRoot: validRoot},
		"API URL path":         {APIServerURL: "https://kubernetes.default.svc/api", ProjectionRoot: validRoot},
		"API URL credentials":  {APIServerURL: "https://user@kubernetes.default.svc", ProjectionRoot: validRoot},
		"relative root":        {APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: "projection"},
		"filesystem root":      {APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: string(filepath.Separator)},
		"symlink root":         {APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: symlinkRoot},
		"writable root":        {APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: unsafeRoot},
		"short request":        {APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: validRoot, RequestTimeout: time.Millisecond},
		"long request":         {APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: validRoot, RequestTimeout: 16 * time.Second},
		"long handshake":       {APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: validRoot, RequestTimeout: 5 * time.Second, HandshakeTimeout: 6 * time.Second},
		"fractional handshake": {APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: validRoot, HandshakeTimeout: time.Second + time.Nanosecond},
		"small response":       {APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: validRoot, MaxResponseBytes: 100},
	}
	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := New(config); !errors.Is(err, ErrConfig) {
				t.Fatalf("configuration error = %v, want ErrConfig", err)
			}
		})
	}
}

func TestNewRejectsUnsafeCredentialProjection(t *testing.T) {
	certificates := issueCertificateFixture(t, 31)
	tests := map[string]func(*testing.T) string{
		"missing token": func(t *testing.T) string {
			root := t.TempDir()
			writeFile(t, filepath.Join(root, CAFileName), certificates.caPEM, 0o444)
			return root
		},
		"opaque token": func(t *testing.T) string {
			return newStableProjection(t, "opaque", certificates.caPEM, 0o440, 0o444)
		},
		"token with extra newline": func(t *testing.T) string {
			return newStableProjection(t, testCallerTokenOne+"\n\n", certificates.caPEM, 0o440, 0o444)
		},
		"broad token": func(t *testing.T) string {
			return newStableProjection(t, testCallerTokenOne, certificates.caPEM, 0o444, 0o444)
		},
		"oversized token": func(t *testing.T) string {
			return newStableProjection(t, strings.Repeat("a", int(maxTokenBytes)+1), certificates.caPEM, 0o440, 0o444)
		},
		"arbitrary token symlink": func(t *testing.T) string {
			root := t.TempDir()
			outside := filepath.Join(t.TempDir(), "token")
			writeFile(t, outside, []byte(testCallerTokenOne), 0o440)
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
				t.Fatalf("link escaping data generation: %v", err)
			}
			writeFile(t, filepath.Join(root, CAFileName), certificates.caPEM, 0o444)
			return root
		},
		"symlink generation directory": func(t *testing.T) string {
			root := t.TempDir()
			generation := "..2026_07_30_11_00_00.000000001"
			outside := t.TempDir()
			writeFile(t, filepath.Join(outside, TokenFileName), []byte(testCallerTokenOne), 0o440)
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
	for name, setup := range tests {
		t.Run(name, func(t *testing.T) {
			root := setup(t)
			if _, err := New(Config{APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: root}); !errors.Is(err, ErrCredential) {
				t.Fatalf("credential error = %v, want ErrCredential", err)
			}
		})
	}
}

func TestNewRejectsUnsafeTrustProjection(t *testing.T) {
	certificates := issueCertificateFixture(t, 41)
	otherCertificates := issueCertificateFixture(t, 42)
	tests := map[string]struct {
		ca   []byte
		mode os.FileMode
	}{
		"empty CA":           {ca: nil, mode: 0o444},
		"invalid PEM":        {ca: []byte("not a CA bundle"), mode: 0o444},
		"non-CA certificate": {ca: certificates.leafPEM, mode: 0o444},
		"trailing data":      {ca: append(append([]byte(nil), certificates.caPEM...), []byte("trailing")...), mode: 0o444},
		"broad CA":           {ca: certificates.caPEM, mode: 0o666},
		"too many CAs":       {ca: bytesRepeat(certificates.caPEM, maxCACertificates+1), mode: 0o444},
		"oversized CA":       {ca: bytesRepeat([]byte(" "), int(maxCABundleBytes)+1), mode: 0o444},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			root := newStableProjection(t, testCallerTokenOne, test.ca, 0o440, test.mode)
			if _, err := New(Config{APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: root}); !errors.Is(err, ErrTrust) {
				t.Fatalf("trust error = %v, want ErrTrust", err)
			}
		})
	}
	missingRoot := t.TempDir()
	writeFile(t, filepath.Join(missingRoot, TokenFileName), []byte(testCallerTokenOne), 0o440)
	if _, err := New(Config{APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: missingRoot}); !errors.Is(err, ErrTrust) {
		t.Fatalf("missing CA error = %v, want ErrTrust", err)
	}
	symlinkRoot := t.TempDir()
	writeFile(t, filepath.Join(symlinkRoot, TokenFileName), []byte(testCallerTokenOne), 0o440)
	outsideCA := filepath.Join(t.TempDir(), CAFileName)
	writeFile(t, outsideCA, certificates.caPEM, 0o444)
	if err := os.Symlink(outsideCA, filepath.Join(symlinkRoot, CAFileName)); err != nil {
		t.Fatalf("link arbitrary CA: %v", err)
	}
	if _, err := New(Config{APIServerURL: "https://kubernetes.default.svc", ProjectionRoot: symlinkRoot}); !errors.Is(err, ErrTrust) {
		t.Fatalf("arbitrary CA symlink error = %v, want ErrTrust", err)
	}
	validBundle := append(append([]byte(nil), certificates.caPEM...), otherCertificates.caPEM...)
	root := newStableProjection(t, testCallerTokenOne, validBundle, 0o440, 0o444)
	projection, err := openProjection(root)
	if err != nil {
		t.Fatalf("open multi-CA projection: %v", err)
	}
	if _, err := projection.loadCAPool(context.Background()); err != nil {
		t.Fatalf("strict multi-CA bundle rejected: %v", err)
	}
}

func TestRotatingTransportRejectsEndpointDriftBeforeNetwork(t *testing.T) {
	certificates := issueCertificateFixture(t, 61)
	root := newStableProjection(t, testCallerTokenOne, certificates.caPEM, 0o440, 0o444)
	projection, err := openProjection(root)
	if err != nil {
		t.Fatalf("open transport projection: %v", err)
	}
	transport := &rotatingCATransport{
		projection:        projection,
		expectedAuthority: "kubernetes.default.svc",
		handshakeTimeout:  time.Second,
	}
	for name, rawURL := range map[string]string{
		"plaintext":       "http://kubernetes.default.svc/api",
		"wrong authority": "https://127.0.0.1:1/api",
		"URL credentials": "https://user@kubernetes.default.svc/api",
	} {
		t.Run(name, func(t *testing.T) {
			request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, rawURL, nil)
			if err != nil {
				t.Fatalf("create drifted request: %v", err)
			}
			if _, err := transport.RoundTrip(request); !errors.Is(err, ErrTrust) {
				t.Fatalf("endpoint drift error = %v, want ErrTrust", err)
			}
		})
	}
}

func TestCredentialSourceObservesOnlyCompleteAtomicGenerations(t *testing.T) {
	root := t.TempDir()
	certificates := issueCertificateFixture(t, 51)
	firstGeneration := "..2026_07_30_12_00_00.000000001"
	secondGeneration := "..2026_07_30_12_05_00.000000002"
	writeProjectionGeneration(t, root, firstGeneration, testCallerTokenOne, certificates.caPEM, 0o440, 0o444)
	writeProjectionGeneration(t, root, secondGeneration, testCallerTokenTwo, certificates.caPEM, 0o440, 0o444)
	linkProjectionFiles(t, root)
	activateProjectionForTest(t, root, firstGeneration, 1)
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
			if err := activateProjection(root, generation, index+2); err != nil {
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
		if token != testCallerTokenOne && token != testCallerTokenTwo {
			t.Fatalf("read partial or foreign projected credential %q", token)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatalf("rotate atomic projection: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.Credential(canceled); !errors.Is(err, ErrCredential) {
		t.Fatalf("canceled credential read error = %v, want ErrCredential", err)
	}
}

func TestProjectedBootstrapDependencyBoundary(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-f", `{{.ImportPath}}`, ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list projected bootstrap dependencies: %v", err)
	}
	allowedLocal := map[string]bool{
		"fugue/internal/backupmaterializeridentity":         true,
		"fugue/internal/backupmaterializerreview":           true,
		"fugue/internal/backupmaterializerreview/projected": true,
	}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") && !allowedLocal[dependency] {
			t.Fatalf("projected TokenReview bootstrap crossed component boundary through %q", dependency)
		}
		for _, forbidden := range []string{"k8s.io/client-go", "database/sql"} {
			if dependency == forbidden || strings.HasPrefix(dependency, forbidden+"/") {
				t.Fatalf("projected TokenReview bootstrap gained forbidden dependency %q", dependency)
			}
		}
	}
	direct := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	directOutput, err := direct.Output()
	if err != nil {
		t.Fatalf("list direct projected bootstrap imports: %v", err)
	}
	for _, forbidden := range []string{"os/exec", "fugue/internal/api", "fugue/internal/store", "fugue/internal/model"} {
		if strings.Contains(string(directOutput), forbidden) {
			t.Fatalf("projected TokenReview bootstrap gained forbidden direct dependency %q", forbidden)
		}
	}
}

func newFixedTLSServer(t *testing.T, certificate *tls.Certificate) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		var review authenticationv1.TokenReview
		if err := json.NewDecoder(request.Body).Decode(&review); err != nil {
			http.Error(w, "invalid review", http.StatusBadRequest)
			return
		}
		response := review
		response.Status = authenticationv1.TokenReviewStatus{
			Authenticated: true,
			Audiences:     []string{backupmaterializeridentity.Audience},
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(response)
	}))
	server.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{*certificate},
	}
	server.StartTLS()
	return server
}

func issueCertificateFixture(t *testing.T, serial int64) certificateFixture {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate test CA key: %v", err)
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(serial*10 + 1),
		Subject:               pkix.Name{CommonName: "fugue projected test CA"},
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
		Subject:      pkix.Name{CommonName: "fugue projected test server"},
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

func newStableProjection(
	t *testing.T,
	token string,
	caPEM []byte,
	tokenMode os.FileMode,
	caMode os.FileMode,
) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, filepath.Join(root, TokenFileName), []byte(token), tokenMode)
	writeFile(t, filepath.Join(root, CAFileName), caPEM, caMode)
	return root
}

func writeProjectionGeneration(
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
	if err := os.Mkdir(directory, 0o750); err != nil {
		t.Fatalf("create projection generation: %v", err)
	}
	writeFile(t, filepath.Join(directory, TokenFileName), []byte(token), tokenMode)
	writeFile(t, filepath.Join(directory, CAFileName), caPEM, caMode)
}

func linkProjectionFiles(t *testing.T, root string) {
	t.Helper()
	for _, name := range []string{TokenFileName, CAFileName} {
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(root, name)); err != nil {
			t.Fatalf("link projected %s: %v", name, err)
		}
	}
}

func activateProjectionForTest(t *testing.T, root string, generation string, sequence int) {
	t.Helper()
	if err := activateProjection(root, generation, sequence); err != nil {
		t.Fatalf("activate projection generation: %v", err)
	}
}

func activateProjection(root string, generation string, sequence int) error {
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
		t.Fatalf("write test projection file: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("set test projection mode: %v", err)
	}
}

func bytesRepeat(value []byte, count int) []byte {
	result := make([]byte, 0, len(value)*count)
	for range count {
		result = append(result, value...)
	}
	return result
}

func assertNoTestCredential(t *testing.T, err error) {
	t.Helper()
	rendered := err.Error()
	for _, credential := range []string{testPresentedToken, testCallerTokenOne, testCallerTokenTwo} {
		if strings.Contains(rendered, credential) {
			t.Fatalf("projected review error retained credential material: %s", rendered)
		}
	}
}

package validationcomposition

import (
	"bytes"
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
	"io"
	"log"
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
	clientprojected "fugue/internal/backupmaterializer/client/projected"
	"fugue/internal/backupmaterializer/dryrunreconciler"
	"fugue/internal/backupmaterializer/materialization"
	readerprojected "fugue/internal/backupmaterializer/secretreader/projected"
	"fugue/internal/backupmaterializer/secretwriter"
	writerprojected "fugue/internal/backupmaterializer/secretwriter/projected"
	"fugue/internal/backupmaterializer/validationagent"
)

const (
	inputToken      = "header-input.workload-input.signature-input"
	readerToken     = "header-reader.kubernetes-reader.signature-reader"
	validationToken = "header-validation.kubernetes-validation.signature-validation"
)

type certificateFixture struct {
	caPEM      []byte
	serverCert tls.Certificate
}

func TestDisabledValidationCompositionIsInertAndRetainsNoCapability(t *testing.T) {
	t.Parallel()
	config := Config{
		Enabled: false, CellKey: "private-invalid-cell", RunID: "private-invalid-run",
		Interval: -1, AttemptTimeout: -1,
		InputProjection: clientprojected.Config{
			Enabled: true, BaseURL: "private invalid URL", ProjectionRoot: "/private/input",
			Now: func() time.Time { panic("disabled input clock") },
		},
		CurrentProjection: readerprojected.Config{
			Enabled: true, APIServerURL: "private invalid API", ProjectionRoot: "/private/reader",
		},
		ValidationProjection: writerprojected.Config{
			Enabled: true, APIServerURL: "private invalid API", ProjectionRoot: "/private/writer",
			Now: func() time.Time { panic("disabled writer clock") },
		},
		Now: func() time.Time { panic("disabled composition clock") },
	}
	service, err := New(config, log.New(&bytes.Buffer{}, "private-prefix", 0))
	if err != nil || service == nil || service.Enabled() {
		t.Fatalf("disabled composition drifted: service=%#v err=%v", service, err)
	}
	snapshot := service.Snapshot()
	if validationagent.ValidateSnapshot(snapshot) != nil || snapshot.Mode != validationagent.ModeDisabled ||
		snapshot.CellKey != "" || snapshot.AttemptCount != 0 {
		t.Fatalf("disabled snapshot drifted: %#v", snapshot)
	}
	rendered := strings.Join([]string{
		fmt.Sprint(config), fmt.Sprintf("%#v", config), fmt.Sprint(service), fmt.Sprintf("%#v", service),
	}, "\n")
	if !strings.Contains(rendered, "[REDACTED]") || strings.Contains(rendered, config.CellKey) ||
		strings.Contains(rendered, config.RunID) || strings.Contains(rendered, "private-prefix") {
		t.Fatalf("disabled composition diagnostics leaked input: %s", rendered)
	}
}

func TestValidationCompositionRejectsBoundaryDriftBeforeProjectionAccess(t *testing.T) {
	t.Parallel()
	cellKey := testCellKey(t)
	base := Config{
		Enabled: true, CellKey: cellKey, RunID: "run-boundary", Interval: 30 * time.Second,
		AttemptTimeout: 20 * time.Second,
		InputProjection: clientprojected.Config{
			Enabled: true, BaseURL: "https://input.example", ProjectionRoot: "/private/input-projection",
			ExpectedCellKey: cellKey, ExpectedRunID: "run-boundary", RequestTimeout: 5 * time.Second,
		},
		CurrentProjection: readerprojected.Config{
			Enabled: true, APIServerURL: "https://kube.example", ProjectionRoot: "/private/reader-projection",
			ExpectedCellKey: cellKey, RequestTimeout: 5 * time.Second,
		},
		ValidationProjection: writerprojected.Config{
			Enabled: true, APIServerURL: "https://kube.example", ProjectionRoot: "/private/writer-projection",
			ExpectedCellKey: cellKey, RequestTimeout: 5 * time.Second,
		},
	}
	mutations := map[string]func(*Config){
		"invalid cell":         func(value *Config) { value.CellKey = "private-invalid" },
		"invalid run":          func(value *Config) { value.RunID = "INVALID RUN"; value.InputProjection.ExpectedRunID = value.RunID },
		"input disabled":       func(value *Config) { value.InputProjection.Enabled = false },
		"reader disabled":      func(value *Config) { value.CurrentProjection.Enabled = false },
		"writer disabled":      func(value *Config) { value.ValidationProjection.Enabled = false },
		"input cell":           func(value *Config) { value.InputProjection.ExpectedCellKey += "0" },
		"reader cell":          func(value *Config) { value.CurrentProjection.ExpectedCellKey += "0" },
		"writer cell":          func(value *Config) { value.ValidationProjection.ExpectedCellKey += "0" },
		"input run":            func(value *Config) { value.InputProjection.ExpectedRunID += "-other" },
		"shared API authority": func(value *Config) { value.InputProjection.BaseURL = "https://KUBE.EXAMPLE.:443" },
		"writer API mismatch":  func(value *Config) { value.ValidationProjection.APIServerURL = "https://other-kube.example" },
		"input root shared":    func(value *Config) { value.InputProjection.ProjectionRoot = value.CurrentProjection.ProjectionRoot },
		"reader root shared": func(value *Config) {
			value.CurrentProjection.ProjectionRoot = value.ValidationProjection.ProjectionRoot
		},
		"writer root shared":     func(value *Config) { value.ValidationProjection.ProjectionRoot = value.InputProjection.ProjectionRoot },
		"relative root":          func(value *Config) { value.InputProjection.ProjectionRoot = "relative" },
		"unclean root":           func(value *Config) { value.CurrentProjection.ProjectionRoot = "/private/../reader" },
		"root filesystem":        func(value *Config) { value.ValidationProjection.ProjectionRoot = "/" },
		"invalid input URL":      func(value *Config) { value.InputProjection.BaseURL = "http://input.example" },
		"invalid API URL":        func(value *Config) { value.CurrentProjection.APIServerURL = "https://kube.example/path" },
		"short interval":         func(value *Config) { value.Interval = time.Millisecond },
		"long interval":          func(value *Config) { value.Interval = 11 * time.Minute },
		"unaligned interval":     func(value *Config) { value.Interval = time.Second + time.Microsecond },
		"short attempt":          func(value *Config) { value.AttemptTimeout = time.Millisecond },
		"long attempt":           func(value *Config) { value.AttemptTimeout = 61 * time.Second },
		"unaligned attempt":      func(value *Config) { value.AttemptTimeout = time.Second + time.Microsecond },
		"input exceeds attempt":  func(value *Config) { value.InputProjection.RequestTimeout = 21 * time.Second },
		"reader exceeds attempt": func(value *Config) { value.CurrentProjection.RequestTimeout = 21 * time.Second },
		"writer exceeds attempt": func(value *Config) { value.ValidationProjection.RequestTimeout = 21 * time.Second },
	}
	for name, mutate := range mutations {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			config := base
			mutate(&config)
			service, err := New(config, nil)
			if service != nil || !errors.Is(err, ErrConfig) || strings.Contains(fmt.Sprint(err), "private") {
				t.Fatalf("boundary drift reached projection: service=%#v err=%v", service, err)
			}
		})
	}
}

func TestValidationCompositionReportsFixedProjectionStageFailures(t *testing.T) {
	t.Parallel()
	fixture := issueCertificateFixture(t, 10)
	cellKey := testCellKey(t)
	inputRoot := writeProjection(t, inputToken, fixture.caPEM)
	readerRoot := writeProjection(t, readerToken, fixture.caPEM)
	writerRoot := writeProjection(t, validationToken, fixture.caPEM)
	base := Config{
		Enabled: true, CellKey: cellKey, RunID: "run-stages", Interval: 30 * time.Second,
		AttemptTimeout: 20 * time.Second,
		InputProjection: clientprojected.Config{
			Enabled: true, BaseURL: "https://input.example", ProjectionRoot: inputRoot,
			ExpectedCellKey: cellKey, ExpectedRunID: "run-stages",
		},
		CurrentProjection: readerprojected.Config{
			Enabled: true, APIServerURL: "https://kube.example", ProjectionRoot: readerRoot, ExpectedCellKey: cellKey,
		},
		ValidationProjection: writerprojected.Config{
			Enabled: true, APIServerURL: "https://kube.example", ProjectionRoot: writerRoot, ExpectedCellKey: cellKey,
		},
	}
	missing := func(name string) string { return filepath.Join(t.TempDir(), name) }
	for name, fixture := range map[string]struct {
		mutate func(*Config)
		want   error
	}{
		"input":      {mutate: func(value *Config) { value.InputProjection.ProjectionRoot = missing("input") }, want: ErrInputProjection},
		"current":    {mutate: func(value *Config) { value.CurrentProjection.ProjectionRoot = missing("current") }, want: ErrCurrentProjection},
		"validation": {mutate: func(value *Config) { value.ValidationProjection.ProjectionRoot = missing("validation") }, want: ErrValidationProjection},
	} {
		name, fixture := name, fixture
		t.Run(name, func(t *testing.T) {
			config := base
			fixture.mutate(&config)
			service, err := New(config, nil)
			if service != nil || !errors.Is(err, fixture.want) || strings.Contains(fmt.Sprint(err), inputToken) ||
				strings.Contains(fmt.Sprint(err), readerToken) || strings.Contains(fmt.Sprint(err), validationToken) {
				t.Fatalf("projection stage drifted: service=%#v err=%v", service, err)
			}
		})
	}
}

func TestValidationCompositionRejectsProjectionFilesystemAlias(t *testing.T) {
	t.Parallel()
	cellKey := testCellKey(t)
	realRoot := t.TempDir()
	aliasRoot := filepath.Join(t.TempDir(), "projection-alias")
	if err := os.Symlink(realRoot, aliasRoot); err != nil {
		t.Fatalf("create projection alias: %v", err)
	}
	config := Config{
		Enabled: true, CellKey: cellKey, RunID: "run-alias", Interval: 30 * time.Second,
		AttemptTimeout: 20 * time.Second,
		InputProjection: clientprojected.Config{
			Enabled: true, BaseURL: "https://input.example", ProjectionRoot: realRoot,
			ExpectedCellKey: cellKey, ExpectedRunID: "run-alias",
		},
		CurrentProjection: readerprojected.Config{
			Enabled: true, APIServerURL: "https://kube.example", ProjectionRoot: aliasRoot, ExpectedCellKey: cellKey,
		},
		ValidationProjection: writerprojected.Config{
			Enabled: true, APIServerURL: "https://kube.example", ProjectionRoot: t.TempDir(), ExpectedCellKey: cellKey,
		},
	}
	service, err := New(config, nil)
	if service != nil || !errors.Is(err, ErrConfig) {
		t.Fatalf("filesystem-aliased projections accepted: service=%#v err=%v", service, err)
	}
}

func TestValidationCompositionWiresConflictRecoveryAndAcceptedDryRunEndToEnd(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	runID := "run-validation-composition"
	bundle, plan := testBundle(t, runID, now)
	bundleDocument, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal bundle: %v", err)
	}
	inputCertificates := issueCertificateFixture(t, 20)
	kubeCertificates := issueCertificateFixture(t, 30)

	var inputRequests atomic.Int64
	var currentRequests atomic.Int64
	var validationRequests atomic.Int64
	var authorizationsMu sync.Mutex
	var authorizations []string
	inputServer := newTLSServer(t, inputCertificates.serverCert, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		inputRequests.Add(1)
		authorizationsMu.Lock()
		authorizations = append(authorizations, request.Header.Get("Authorization"))
		authorizationsMu.Unlock()
		if request.Method != http.MethodGet || request.URL.Path != "/v1/backup-control/runs/"+runID+"/observer-input-bundle" ||
			request.URL.RawQuery != "" || request.Header.Get("Authorization") != "Bearer "+inputToken {
			t.Errorf("input request crossed boundary: method=%s uri=%s headers=%v", request.Method, request.URL.RequestURI(), request.Header)
		}
		writePrivateJSON(writer, http.StatusOK, bundleDocument)
	}))
	defer inputServer.Close()
	identity, err := materialization.SecretIdentityForCell(plan.CellKey)
	if err != nil {
		t.Fatalf("derive Secret identity: %v", err)
	}
	kubeServer := newTLSServer(t, kubeCertificates.serverCert, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		authorizationsMu.Lock()
		authorizations = append(authorizations, request.Header.Get("Authorization"))
		authorizationsMu.Unlock()
		itemPath := "/api/v1/namespaces/" + identity.Namespace + "/secrets/" + identity.SecretName
		collectionPath := "/api/v1/namespaces/" + identity.Namespace + "/secrets"
		switch {
		case request.Method == http.MethodGet && request.URL.Path == itemPath && request.URL.RawQuery == "":
			currentRequests.Add(1)
			if request.Header.Get("Authorization") != "Bearer "+readerToken {
				t.Errorf("reader used wrong credential: %q", request.Header.Get("Authorization"))
			}
			writeNotFound(writer, identity.SecretName)
		case request.Method == http.MethodPost && request.URL.Path == collectionPath && request.URL.RawQuery == secretwriter.DryRunRawQuery:
			attempt := validationRequests.Add(1)
			if request.Header.Get("Authorization") != "Bearer "+validationToken {
				t.Errorf("validator used wrong credential: %q", request.Header.Get("Authorization"))
			}
			document, readErr := io.ReadAll(io.LimitReader(request.Body, secretwriter.MaximumRequestBytes+1))
			if readErr != nil || secretwriter.ValidateTransportRequest(
				request.Method, request.URL.Path, request.URL.RawQuery, plan.CellKey, document,
			) != nil {
				t.Errorf("dry-run request invalid: err=%v method=%s uri=%s", readErr, request.Method, request.URL.RequestURI())
			}
			if attempt == 1 {
				writeJSON(writer, http.StatusConflict, []byte(`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Conflict","code":409}`))
				return
			}
			writeJSON(writer, http.StatusCreated, document)
		default:
			t.Errorf("unexpected Kubernetes request: method=%s uri=%s", request.Method, request.URL.RequestURI())
			writeJSON(writer, http.StatusNotFound, []byte(`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"NotFound","code":404}`))
		}
	}))
	defer kubeServer.Close()

	inputRoot := writeProjection(t, inputToken, inputCertificates.caPEM)
	readerRoot := writeProjection(t, readerToken, kubeCertificates.caPEM)
	writerRoot := writeProjection(t, validationToken, kubeCertificates.caPEM)
	config := Config{
		Enabled: true, CellKey: plan.CellKey, RunID: runID, Interval: time.Second, AttemptTimeout: 5 * time.Second,
		InputProjection: clientprojected.Config{
			Enabled: true, BaseURL: inputServer.URL, ProjectionRoot: inputRoot,
			ExpectedCellKey: plan.CellKey, ExpectedRunID: runID, RequestTimeout: time.Second,
			HandshakeTimeout: time.Second, Now: func() time.Time { panic("nested input clock retained") },
		},
		CurrentProjection: readerprojected.Config{
			Enabled: true, APIServerURL: kubeServer.URL, ProjectionRoot: readerRoot,
			ExpectedCellKey: plan.CellKey, RequestTimeout: time.Second, HandshakeTimeout: time.Second,
		},
		ValidationProjection: writerprojected.Config{
			Enabled: true, APIServerURL: kubeServer.URL, ProjectionRoot: writerRoot,
			ExpectedCellKey: plan.CellKey, RequestTimeout: time.Second, HandshakeTimeout: time.Second,
			Now: func() time.Time { panic("nested writer clock retained") },
		},
		Now: func() time.Time { return now },
	}
	var logs bytes.Buffer
	service, err := New(config, log.New(&logs, "validation ", 0))
	if err != nil || service == nil || !service.Enabled() || inputRequests.Load() != 0 ||
		currentRequests.Load() != 0 || validationRequests.Load() != 0 {
		t.Fatalf("enabled composition construction performed I/O or failed: service=%#v err=%v input=%d current=%d validation=%d", service, err, inputRequests.Load(), currentRequests.Load(), validationRequests.Load())
	}
	initial := service.Snapshot()
	if validationagent.ValidateSnapshot(initial) != nil || initial.Mode != validationagent.ModeShadow ||
		initial.CellKey != plan.CellKey || initial.Ready || initial.AttemptCount != 0 {
		t.Fatalf("initial composition snapshot drifted: %#v", initial)
	}

	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("conflict cycle became supervisor failure: %v", err)
	}
	conflict := service.Snapshot()
	if validationagent.ValidateSnapshot(conflict) != nil || conflict.FailureCode != "" || !conflict.Blocked ||
		!conflict.Retryable || !conflict.ValidationAttempted || conflict.ValidationAccepted ||
		conflict.ValidationOutcome != dryrunreconciler.OutcomeConflict || conflict.CurrentStatus == nil ||
		conflict.CurrentStatus.PreparedCycle == nil || conflict.AttemptCount != 1 {
		t.Fatalf("conflict did not remain cell-local: %#v", conflict)
	}
	preparedDigest := conflict.CurrentStatus.PreparedCycleDigest
	candidateDigest := conflict.CurrentStatus.CandidatePlanDigest

	if err := service.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("accepted recovery cycle: %v", err)
	}
	accepted := service.Snapshot()
	if validationagent.ValidateSnapshot(accepted) != nil || accepted.Ready || accepted.Blocked || accepted.Retryable ||
		!accepted.MutationCandidate || !accepted.ValidationRequired || !accepted.ValidationAttempted ||
		!accepted.ValidationAccepted || accepted.ValidationOutcome != dryrunreconciler.OutcomeAccepted ||
		accepted.Persisted || accepted.DeleteAllowed || !accepted.ObservationOnly || accepted.ExecutionAllowed ||
		accepted.ProductionMutationAllowed || accepted.ConsecutiveFailures != 0 || accepted.AttemptCount != 2 ||
		accepted.CurrentStatus == nil || accepted.CurrentStatus.PreparedCycleDigest != preparedDigest ||
		accepted.CurrentStatus.CandidatePlanDigest != candidateDigest {
		t.Fatalf("accepted dry-run recovery drifted: %#v", accepted)
	}
	if inputRequests.Load() != 2 || currentRequests.Load() != 2 || validationRequests.Load() != 2 {
		t.Fatalf("composition request counts drifted: input=%d current=%d validation=%d", inputRequests.Load(), currentRequests.Load(), validationRequests.Load())
	}
	authorizationsMu.Lock()
	gotAuthorizations := append([]string(nil), authorizations...)
	authorizationsMu.Unlock()
	if !reflect.DeepEqual(gotAuthorizations, []string{
		"Bearer " + readerToken, "Bearer " + inputToken, "Bearer " + validationToken,
		"Bearer " + readerToken, "Bearer " + inputToken, "Bearer " + validationToken,
	}) {
		t.Fatalf("projection credentials crossed boundaries: %v", gotAuthorizations)
	}
	assertSecretFree(t, accepted, plan, now, logs.String())
}

func TestValidationCompositionUsesDefaultClockAndRequestTimeouts(t *testing.T) {
	t.Parallel()
	fixture := issueCertificateFixture(t, 40)
	cellKey := testCellKey(t)
	config := Config{
		Enabled: true, CellKey: cellKey, RunID: "run-defaults", Interval: 30 * time.Second,
		AttemptTimeout: 20 * time.Second,
		InputProjection: clientprojected.Config{
			Enabled: true, BaseURL: "https://input.example", ProjectionRoot: writeProjection(t, inputToken, fixture.caPEM),
			ExpectedCellKey: cellKey, ExpectedRunID: "run-defaults",
		},
		CurrentProjection: readerprojected.Config{
			Enabled: true, APIServerURL: "https://kube.example", ProjectionRoot: writeProjection(t, readerToken, fixture.caPEM),
			ExpectedCellKey: cellKey,
		},
		ValidationProjection: writerprojected.Config{
			Enabled: true, APIServerURL: "https://kube.example", ProjectionRoot: writeProjection(t, validationToken, fixture.caPEM),
			ExpectedCellKey: cellKey,
		},
	}
	service, err := New(config, nil)
	if err != nil || service == nil || !service.Enabled() || validationagent.ValidateSnapshot(service.Snapshot()) != nil {
		t.Fatalf("default composition drifted: service=%#v err=%v", service, err)
	}
}

func TestValidationCompositionDependencyBoundary(t *testing.T) {
	output, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("list validation composition dependencies: %v", err)
	}
	var local []string
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
		for _, forbidden := range []string{"database/sql", "os/exec", "fugue/internal/backupmaterializer"} {
			if dependency == forbidden {
				t.Fatalf("validation composition gained forbidden dependency %q", dependency)
			}
		}
		for _, prefix := range []string{
			"k8s.io/", "fugue/internal/api", "fugue/internal/auth", "fugue/internal/store", "fugue/internal/model",
			"fugue/internal/backupmaterializer/agent", "fugue/internal/backupmaterializer/composition",
			"fugue/internal/backupmaterializer/httpapi", "fugue/internal/backupmaterializer/legacysource",
			"fugue/internal/backupmaterializer/localissuer", "fugue/internal/backupmaterializer/storesource",
			"fugue/internal/backupmaterializeridentity", "fugue/internal/backupmaterializerreview",
		} {
			if strings.HasPrefix(dependency, prefix) {
				t.Fatalf("validation composition crossed component boundary through %q", dependency)
			}
		}
	}
	sort.Strings(local)
	wantLocal := []string{
		"fugue/internal/backupcontrol",
		"fugue/internal/backupmaterializer/client",
		"fugue/internal/backupmaterializer/client/projected",
		"fugue/internal/backupmaterializer/contract",
		"fugue/internal/backupmaterializer/dryrunreconciler",
		"fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconcile",
		"fugue/internal/backupmaterializer/reconciler",
		"fugue/internal/backupmaterializer/secretreader",
		"fugue/internal/backupmaterializer/secretreader/projected",
		"fugue/internal/backupmaterializer/secretwriter",
		"fugue/internal/backupmaterializer/secretwriter/projected",
		"fugue/internal/backupmaterializer/validationagent",
		"fugue/internal/backupmaterializer/validationcomposition",
		"fugue/internal/backupmaterializer/validationcycle",
	}
	if !reflect.DeepEqual(local, wantLocal) {
		t.Fatalf("validation composition local closure drifted: got=%v want=%v", local, wantLocal)
	}
	directOutput, err := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".").Output()
	if err != nil {
		t.Fatalf("list direct validation composition imports: %v", err)
	}
	gotDirect := strings.Fields(string(directOutput))
	sort.Strings(gotDirect)
	wantDirect := []string{
		"errors", "log", "net", "net/url", "os", "path/filepath", "regexp", "strconv", "strings", "time",
		"fugue/internal/backupmaterializer/client", "fugue/internal/backupmaterializer/client/projected",
		"fugue/internal/backupmaterializer/dryrunreconciler", "fugue/internal/backupmaterializer/materialization",
		"fugue/internal/backupmaterializer/reconciler", "fugue/internal/backupmaterializer/secretreader",
		"fugue/internal/backupmaterializer/secretreader/projected", "fugue/internal/backupmaterializer/secretwriter",
		"fugue/internal/backupmaterializer/secretwriter/projected", "fugue/internal/backupmaterializer/validationagent",
		"fugue/internal/backupmaterializer/validationcycle",
	}
	sort.Strings(wantDirect)
	if !reflect.DeepEqual(gotDirect, wantDirect) {
		t.Fatalf("validation composition direct imports drifted: got=%v want=%v", gotDirect, wantDirect)
	}
}

func testBundle(t *testing.T, runID string, now time.Time) (backupmaterializer.ObserverInputBundle, materialization.Plan) {
	t.Helper()
	spec, err := backupcontrol.NewShadowBackupRunSpec(
		runID, runID, testTarget(), "backend-1", "sha256:"+strings.Repeat("a", 64), 4, 120, 1800,
	)
	if err != nil {
		t.Fatalf("build backup spec: %v", err)
	}
	keyring := backupidentity.DeriveKeyring(strings.Repeat("k", 32), "backup-key-1", "", "", nil)
	bundle, err := backupmaterializer.IssueObserverInputBundle(keyring, spec, "tenant-1", now)
	if err != nil {
		t.Fatalf("issue observer input bundle: %v", err)
	}
	plan, err := materialization.Build(bundle, now)
	if err != nil {
		t.Fatalf("build materialization plan: %v", err)
	}
	return bundle, plan
}

func testCellKey(t *testing.T) string {
	t.Helper()
	cellKey := backupcontrol.BackupCellKey(testTarget())
	if cellKey == "" {
		t.Fatal("derive test cell key")
	}
	return cellKey
}

func testTarget() backupcontrol.BackupTarget {
	return backupcontrol.BackupTarget{Type: backupcontrol.TargetAppDatabase, ScopeKey: "app/app-1/database"}
}

func issueCertificateFixture(t *testing.T, serial int64) certificateFixture {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial*10 + 1), Subject: pkix.Name{CommonName: "fugue validation composition test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(serial*10 + 2), Subject: pkix.Name{CommonName: "fugue validation composition test server"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(12 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames: []string{"localhost"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}
	return certificateFixture{
		caPEM:      pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		serverCert: tls.Certificate{Certificate: [][]byte{leafDER, caDER}, PrivateKey: leafKey},
	}
}

func writeProjection(t *testing.T, token string, caPEM []byte) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, clientprojected.TokenFileName), []byte(token), 0o600); err != nil {
		t.Fatalf("write projected token: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, clientprojected.CAFileName), caPEM, 0o444); err != nil {
		t.Fatalf("write projected CA: %v", err)
	}
	return root
}

func newTLSServer(t *testing.T, certificate tls.Certificate, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}}
	server.StartTLS()
	return server
}

func writePrivateJSON(writer http.ResponseWriter, status int, document []byte) {
	writer.Header().Set("Cache-Control", "private, no-store, max-age=0")
	writer.Header().Set("Pragma", "no-cache")
	writer.Header().Set("Vary", "Authorization")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writeJSON(writer, status, document)
}

func writeJSON(writer http.ResponseWriter, status int, document []byte) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Content-Length", fmt.Sprint(len(document)))
	writer.WriteHeader(status)
	_, _ = writer.Write(document)
}

func writeNotFound(writer http.ResponseWriter, secretName string) {
	document := []byte(fmt.Sprintf(
		`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"NotFound","details":{"name":%q,"kind":"secrets","group":""},"code":404}`,
		secretName,
	))
	writeJSON(writer, http.StatusNotFound, document)
}

func assertSecretFree(t *testing.T, snapshot validationagent.Snapshot, plan materialization.Plan, now time.Time, logs string) {
	t.Helper()
	document, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	data, err := plan.Data(now)
	if err != nil {
		t.Fatalf("read private plan data: %v", err)
	}
	rendered := string(document) + fmt.Sprintf("%#v %v", snapshot, snapshot) + logs
	for _, sensitive := range []string{
		inputToken, readerToken, validationToken, string(data.SpecDocument), string(data.ObserverToken), "tenant-1",
	} {
		if strings.Contains(rendered, sensitive) {
			t.Fatalf("composition evidence leaked private input %q", sensitive)
		}
	}
}

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestConfigDefaultsAreLocalAndNonAuthoritative(t *testing.T) {
	t.Parallel()

	cfg, err := configFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Enabled || cfg.BindAddr != defaultBindAddr || cfg.ShutdownTimeout != defaultShutdownTimeout || cfg.AuthorityRuntimeEnabled {
		t.Fatalf("unsafe defaults: %+v", cfg)
	}
}
func TestConfigAndProcessWireGroupAuthority(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	values := map[string]string{
		"FUGUE_EDGE_CONTROL_ENABLED":                       "true",
		"FUGUE_EDGE_CONTROL_AUTHORITY_RUNTIME_ENABLED":     "true",
		"FUGUE_EDGE_CONTROL_AUTHORITY_STATE_DIR":           filepath.Join(root, "state"),
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_URL":              "https://fugue-api-tls.fugue-system.svc:8443/v1/edge/route-intents",
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_ISSUER_FILE":      "/var/run/secrets/fugue-edge-control/route-intent/token",
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_IDENTITY_NODE_ID": "edge-control-test",
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_CA_FILE":          writeRouteIntentCAFixture(t),
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_SERVER_NAME":      "fugue-api-tls.fugue-system.svc",
		"FUGUE_EDGE_CONTROL_INVENTORY_WRITER_KEYRING_DIR":  filepath.Join(root, "inventory"),
		"FUGUE_EDGE_CONTROL_AUTHORITY_GROUP_IDS":           "edge-group-country-us",
		"FUGUE_EDGE_CONTROL_AUTHORITY_RECONCILE_INTERVAL":  "30s",
		"FUGUE_EDGE_CONTROL_GROUP_SIGNING_KEYRING_DIR":     filepath.Join(root, "signing"),
		"FUGUE_EDGE_CONTROL_GROUP_READER_KEYRING_DIR":      filepath.Join(root, "readers"),
		"FUGUE_EDGE_CONTROL_GROUP_RECOVERY_KEYRING_DIR":    filepath.Join(root, "recovery"),
		"FUGUE_EDGE_CONTROL_GROUP_BUNDLE_VALIDITY":         "30m",
		"FUGUE_EDGE_CONTROL_CANDIDATE_PUBLISHER_ENABLED":   "true",
		"FUGUE_EDGE_CONTROL_SOURCE_SHA":                    strings.Repeat("1", 40),
		"FUGUE_EDGE_CONTROL_SELF_IMAGE_REF":                "ghcr.io/example/fugue-edge-control@sha256:" + strings.Repeat("2", 64),
		"FUGUE_EDGE_CONTROL_MANIFEST_DIGEST":               "sha256:" + strings.Repeat("3", 64),
		"FUGUE_EDGE_CONTROL_HEALTH_CONTRACT_DIGEST":        "sha256:" + strings.Repeat("4", 64),
		"FUGUE_EDGE_CONTROL_RELEASE_RECORD_DIGEST":         "sha256:" + strings.Repeat("5", 64),
	}
	cfg, err := configFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AuthorityRuntimeEnabled || !cfg.CandidatePublisher || cfg.CandidateIdentity.SourceSHA != strings.Repeat("1", 40) ||
		cfg.AuthorityStateDir != values["FUGUE_EDGE_CONTROL_AUTHORITY_STATE_DIR"] ||
		cfg.AuthorityPollInterval != 30*time.Second || cfg.GroupBundleValidity != 30*time.Minute ||
		!reflect.DeepEqual(cfg.AuthorityGroupIDs, []string{"edge-group-country-us"}) {
		t.Fatalf("unexpected authority config: %+v", cfg)
	}
	runtime, handler, err := buildAuthorityProcess(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if runtime == nil || handler == nil || len(runtime.GroupIDs) != 1 || runtime.Status == nil || runtime.Candidate == nil {
		t.Fatalf("incomplete authority process: runtime=%+v handler=%v", runtime, handler)
	}
	for path, wantCode := range map[string]int{
		"/v1/status":           http.StatusOK,
		"/v1/authority/status": http.StatusOK,
		"/readyz":              http.StatusOK,
		"/v1/shadow/status":    http.StatusNotFound,
		"/v1/edge/routes":      http.StatusNotFound,
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != wantCode {
			t.Fatalf("GET %s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
	for _, groupID := range cfg.AuthorityGroupIDs {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/authority/groups/"+groupID+"/readyz", nil))
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("unpublished group %s readiness=%d body=%s", groupID, recorder.Code, recorder.Body.String())
		}
	}
	statusRecorder := httptest.NewRecorder()
	handler.ServeHTTP(statusRecorder, httptest.NewRequest(http.MethodGet, "/v1/status", nil))
	if !strings.Contains(statusRecorder.Body.String(), `"authority":"edge-control"`) || !strings.Contains(statusRecorder.Body.String(), `"publication_enabled":true`) {
		t.Fatalf("authority boundary status=%s", statusRecorder.Body.String())
	}
	legacyInventoryRecorder := httptest.NewRecorder()
	handler.ServeHTTP(legacyInventoryRecorder, httptest.NewRequest(http.MethodPost, "/v1/shadow/group-inventory-heartbeats", strings.NewReader("{}")))
	if legacyInventoryRecorder.Code != http.StatusNotFound {
		t.Fatalf("authority process retained shadow inventory path: status=%d body=%s", legacyInventoryRecorder.Code, legacyInventoryRecorder.Body.String())
	}
	authorityInventoryRecorder := httptest.NewRecorder()
	handler.ServeHTTP(authorityInventoryRecorder, httptest.NewRequest(http.MethodPost, "/v1/authority/group-inventory-heartbeats", strings.NewReader("{}")))
	if authorityInventoryRecorder.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("authority inventory path status=%d body=%s", authorityInventoryRecorder.Code, authorityInventoryRecorder.Body.String())
	}
	recoveryRecorder := httptest.NewRecorder()
	handler.ServeHTTP(recoveryRecorder, httptest.NewRequest(http.MethodPost, "/v1/recovery/group-publications", strings.NewReader("{}")))
	if recoveryRecorder.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("unauthenticated recovery status=%d body=%s", recoveryRecorder.Code, recoveryRecorder.Body.String())
	}
	stageRecorder := httptest.NewRecorder()
	handler.ServeHTTP(stageRecorder, httptest.NewRequest(http.MethodPost, "/v1/authority/group-worker-candidates", strings.NewReader("{}")))
	if stageRecorder.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("unauthenticated Worker candidate stage status=%d body=%s", stageRecorder.Code, stageRecorder.Body.String())
	}
}

func TestConfigRejectsAmbiguousOrUnsafeAuthorityRuntime(t *testing.T) {
	t.Parallel()

	base := map[string]string{
		"FUGUE_EDGE_CONTROL_ENABLED":                       "true",
		"FUGUE_EDGE_CONTROL_AUTHORITY_RUNTIME_ENABLED":     "true",
		"FUGUE_EDGE_CONTROL_AUTHORITY_STATE_DIR":           "/var/lib/fugue-edge-control",
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_URL":              "https://fugue-api-tls.fugue-system.svc:8443/v1/edge/route-intents",
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_ISSUER_FILE":      "/var/run/secrets/fugue-edge-control/route-intent/token",
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_IDENTITY_NODE_ID": "edge-control-test",
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_CA_FILE":          "/var/run/secrets/fugue-edge-control/route-intent-ca/ca.crt",
		"FUGUE_EDGE_CONTROL_ROUTE_INTENT_SERVER_NAME":      "fugue-api-tls.fugue-system.svc",
		"FUGUE_EDGE_CONTROL_INVENTORY_WRITER_KEYRING_DIR":  "/var/run/secrets/fugue-edge-control/inventory",
		"FUGUE_EDGE_CONTROL_AUTHORITY_GROUP_IDS":           "edge-group-country-us",
		"FUGUE_EDGE_CONTROL_AUTHORITY_RECONCILE_INTERVAL":  "30s",
		"FUGUE_EDGE_CONTROL_GROUP_SIGNING_KEYRING_DIR":     "/var/run/secrets/fugue-edge-control/signing",
		"FUGUE_EDGE_CONTROL_GROUP_READER_KEYRING_DIR":      "/var/run/secrets/fugue-edge-control/readers",
		"FUGUE_EDGE_CONTROL_GROUP_RECOVERY_KEYRING_DIR":    "/var/run/secrets/fugue-edge-control/recovery",
		"FUGUE_EDGE_CONTROL_GROUP_BUNDLE_VALIDITY":         "30m",
	}
	for name, mutate := range map[string]func(map[string]string){
		"relative state": func(values map[string]string) { values["FUGUE_EDGE_CONTROL_AUTHORITY_STATE_DIR"] = "state" },
		"unsafe group path": func(values map[string]string) {
			values["FUGUE_EDGE_CONTROL_AUTHORITY_GROUP_IDS"] = "../edge-group-country-us"
		},
		"shared keyring dir": func(values map[string]string) {
			values["FUGUE_EDGE_CONTROL_GROUP_READER_KEYRING_DIR"] = values["FUGUE_EDGE_CONTROL_GROUP_SIGNING_KEYRING_DIR"]
		},
		"multiple groups share one process": func(values map[string]string) {
			values["FUGUE_EDGE_CONTROL_AUTHORITY_GROUP_IDS"] = "edge-group-country-de,edge-group-country-us"
		},
		"short validity": func(values map[string]string) { values["FUGUE_EDGE_CONTROL_GROUP_BUNDLE_VALIDITY"] = "4m59s" },
		"long validity":  func(values map[string]string) { values["FUGUE_EDGE_CONTROL_GROUP_BUNDLE_VALIDITY"] = "24h1s" },
	} {
		t.Run(name, func(t *testing.T) {
			values := make(map[string]string, len(base))
			for key, value := range base {
				values[key] = value
			}
			mutate(values)
			if _, err := configFromEnv(func(key string) string { return values[key] }); err == nil {
				t.Fatal("unsafe authority config unexpectedly accepted")
			}
		})
	}
}

func TestConfigAcceptsExplicitProcessSettings(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"FUGUE_EDGE_CONTROL_ENABLED":          "true",
		"FUGUE_EDGE_CONTROL_BIND_ADDR":        "0.0.0.0:8092",
		"FUGUE_EDGE_CONTROL_SHUTDOWN_TIMEOUT": "30s",
	}
	cfg, err := configFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Enabled || cfg.BindAddr != "0.0.0.0:8092" || cfg.ShutdownTimeout != 30*time.Second {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestConfigRejectsAmbiguousOrUnsafeValues(t *testing.T) {
	t.Parallel()

	for name, values := range map[string]map[string]string{
		"ambiguous bool": {"FUGUE_EDGE_CONTROL_ENABLED": "1"},
		"missing host":   {"FUGUE_EDGE_CONTROL_BIND_ADDR": ":8092"},
		"missing port":   {"FUGUE_EDGE_CONTROL_BIND_ADDR": "127.0.0.1"},
		"zero port":      {"FUGUE_EDGE_CONTROL_BIND_ADDR": "127.0.0.1:0"},
		"long shutdown":  {"FUGUE_EDGE_CONTROL_SHUTDOWN_TIMEOUT": "121s"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := configFromEnv(func(key string) string { return values[key] }); err == nil {
				t.Fatal("config unexpectedly accepted unsafe value")
			}
		})
	}
}

func TestRunHonorsCancelledContextWithoutSideEffects(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := run(ctx, config{BindAddr: "127.0.0.1:18092", ShutdownTimeout: time.Second}, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("run cancelled context: %v", err)
	}
}

func writeRouteIntentCAFixture(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Edge Control RouteIntent Test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), BasicConstraintsValid: true, IsCA: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

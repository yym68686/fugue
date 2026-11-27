package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	appsclient "k8s.io/client-go/kubernetes/typed/apps/v1"
	"k8s.io/client-go/rest"
)

func TestNativeDeploymentSendsOneCASWithoutRetryOrForcedOwnership(t *testing.T) {
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fixture", Name: "candidate"}
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{FieldManager: "fixture-guardian"}}
	const manifest = `{"apiVersion":"release.fugue.dev/v2","kind":"ComponentResourceSet","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"candidate","namespace":"fixture","uid":"proven-uid","resourceVersion":"42"},"spec":{}}]}`
	const success = `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"candidate","namespace":"fixture","uid":"proven-uid","resourceVersion":"43"}}`
	for _, tc := range []struct {
		name              string
		status            int
		body              string
		unknown, conflict bool
	}{
		{"success", 200, success, false, false},
		{"wrong UID", 200, strings.Replace(success, "proven-uid", "foreign", 1), true, false},
		{"unreadable result", 200, "{", true, false},
		{"CAS conflict", 409, `{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Conflict","code":409}`, false, true},
		{"timeout", 504, `{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Timeout","code":504}`, true, false},
		{"retry after", 429, `{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"TooManyRequests","code":429}`, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan struct{}, 8)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- struct{}{}
				raw, err := io.ReadAll(r.Body)
				if err != nil || r.Method != http.MethodPatch || r.URL.Path != "/apis/apps/v1/namespaces/fixture/deployments/candidate" ||
					r.URL.Query().Get("fieldManager") != "fixture-guardian" || r.URL.Query().Has("force") ||
					!strings.Contains(string(raw), `"uid":"proven-uid"`) || !strings.Contains(string(raw), `"resourceVersion":"42"`) {
					t.Errorf("request did not bind the one approved CAS: %s %s, %s, %v", r.Method, r.URL, raw, err)
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			apps, err := appsclient.NewForConfigAndClient(&rest.Config{Host: server.URL}, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			cluster := &kubectlCluster{nativeArtifacts: &nativeKubectlArtifacts{workload: identity}, nativeDeployment: apps.RESTClient()}
			err = cluster.applyNativeDeployment(context.Background(), release, []byte(manifest))
			if (err != nil) != (tc.unknown || tc.conflict) || errors.Is(err, declarativerelease.ErrWriteOutcomeUnknown) != tc.unknown ||
				apierrors.IsConflict(err) != tc.conflict || errors.Is(err, declarativerelease.ErrWriteRejected) != tc.conflict || len(requests) != 1 {
				t.Fatalf("attempts=%d error=%v", len(requests), err)
			}
			if err := cluster.applyNativeDeployment(context.Background(), release, []byte(strings.Replace(manifest, `,"resourceVersion":"42"`, "", 1))); err == nil || len(requests) != 1 {
				t.Fatalf("missing CAS reached the provider: attempts=%d error=%v", len(requests), err)
			}
		})
	}
}

func TestNativeArtifactPreflightNeverFallsBackToHistoricalReceipt(t *testing.T) {
	ctx := context.Background()
	workload := declarativerelease.Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fixture", Name: "candidate"}
	release := declarativerelease.PlanRelease{ComponentID: workload.Name, Workload: workload}
	fixture := func(char string) (declarativerelease.TargetIdentity, declarativerelease.ArtifactReceipt, []byte) {
		sha, digest := strings.Repeat(char, 40), "sha256:"+strings.Repeat(char, 64)
		image := "registry.fixture.invalid/release/app@" + digest
		manifest := []byte(fmt.Sprintf(`{"apiVersion":"release.fugue.dev/v2","kind":"ComponentResourceSet","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"candidate","namespace":"fixture"},"spec":{"template":{"spec":{"containers":[{"name":"app","image":%q}]}}}}]}`, image))
		return declarativerelease.TargetIdentity{Present: true, ImageRef: image, ConfigSHA: sha, OCIRevision: sha},
			declarativerelease.ArtifactReceipt{Component: release.ComponentID, ConfigSHA: sha, OCIRevision: sha, ImmutableRef: image, TopDigest: digest}, manifest
	}
	forward, forwardReceipt, forwardManifest := fixture("a")
	rollback, rollbackReceipt, lkgManifest := fixture("b")
	prepared := declarativerelease.ExecutionPlan{Forward: forward, LKG: rollback}
	recordDigest := "sha256:" + strings.Repeat("d", 64)
	registry, err := releaseguardian.NewIndependentRegistry("registry.fixture.invalid", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &nativeExecutionRuntime{binding: releaseguardian.NativeRuntimeBinding{LKGMonitorRecordDigest: recordDigest}, registry: registry}
	lkg := monitorBundle{
		Artifact: rollbackReceipt, Record: declarativerelease.MonitorRecord{RecordDigest: recordDigest},
		Prepared: declarativerelease.ExecutionPlan{Forward: rollback}, Forward: lkgManifest,
	}
	if _, err := runtime.artifactSet(release, prepared, forwardReceipt, &lkg, forwardManifest, []byte("other manifest")); err == nil {
		t.Fatal("replacement LKG manifest was accepted")
	}
	artifacts, err := runtime.artifactSet(release, prepared, forwardReceipt, &lkg, forwardManifest, lkgManifest)
	if err != nil {
		t.Fatal(err)
	}
	var reads []string
	var missing = true
	artifacts.verify = func(_ context.Context, receipt declarativerelease.ArtifactReceipt) (declarativerelease.RegistryVerification, error) {
		reads = append(reads, receipt.ImmutableRef)
		if missing && receipt == rollbackReceipt {
			return declarativerelease.RegistryVerification{}, errors.New("layer unavailable")
		}
		return declarativerelease.RegistryVerification{Image: receipt.ImmutableRef, OCIRevision: receipt.OCIRevision}, nil
	}
	marker := filepath.Join(t.TempDir(), "legacy-called")
	script := filepath.Join(t.TempDir(), "legacy")
	if err := os.WriteFile(script, []byte("import os\nopen(os.environ['NATIVE_TEST_FALLBACK'], 'w').close()\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NATIVE_TEST_FALLBACK", marker)
	cluster := &kubectlCluster{nativeArtifacts: artifacts, trustedCurrent: &rollbackReceipt, verifier: script}
	if err := cluster.preflightNativeArtifacts(ctx); err == nil || !strings.Contains(err.Error(), "preflight native rollback image") {
		t.Fatalf("unavailable rollback artifact was accepted: %v", err)
	}
	if err := cluster.Apply(ctx, release, forward, forwardManifest); err == nil || !strings.Contains(err.Error(), "native artifact preflight") {
		t.Fatalf("missing artifact reached Kubernetes apply: %v", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("native path invoked legacy verifier: %v", err)
	}
	missing = false
	if err := cluster.preflightNativeArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.VerifyTarget(ctx, rollback); err != nil {
		t.Fatal(err)
	}
	if want := []string{forward.ImageRef, rollback.ImageRef, rollback.ImageRef}; !reflect.DeepEqual(reads, want) {
		t.Fatalf("readbacks=%v, want=%v", reads, want)
	}
	if err := cluster.Apply(ctx, release, forward, lkgManifest); err == nil || !strings.Contains(err.Error(), "outside its artifact approval") {
		t.Fatalf("different declared image reached apply: %v", err)
	}
	if err := cluster.Delete(ctx, release, forwardManifest, declarativerelease.Observation{}); err == nil {
		t.Fatal("native deployment adapter permitted deletion")
	}
}

func TestNativeRuntimeConfigurationCannotDowngrade(t *testing.T) {
	for _, name := range []string{releaseguardian.NativeRuntimeBindingEnv, releaseguardian.NativeRuntimeRegistryEnv,
		releaseguardian.NativeRuntimeAuthFileEnv, "FUGUE_COMPONENT_LEASE_OWNER"} {
		t.Setenv(name, "")
	}
	if runtime, err := nativeExecutionRuntimeFromEnv(); runtime != nil || err != nil {
		t.Fatalf("legacy remains unchanged; runtime=%v, err=%v", runtime, err)
	}
	t.Setenv("FUGUE_COMPONENT_LEASE_OWNER", "guardian")
	t.Setenv(releaseguardian.NativeRuntimeBindingEnv, "fenced")
	if _, err := nativeExecutionRuntimeFromEnv(); err == nil {
		t.Fatal("fenced Guardian fell back to the legacy verifier")
	}
	t.Setenv(releaseguardian.NativeRuntimeBindingEnv, "")
	t.Setenv(releaseguardian.NativeRuntimeRegistryEnv, "registry.fixture.invalid")
	if _, err := nativeExecutionRuntimeFromEnv(); err == nil {
		t.Fatal("dedicated native runtime accepted an unfenced target")
	}
}

func TestNativeKubectlClusterDoesNotRequireLegacyFiles(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	t.Setenv("PATH", directory)
	t.Setenv(trustedCurrentArtifactEnv, "this is not a receipt")
	if err := os.WriteFile(filepath.Join(directory, "kubectl"), []byte("#!/bin/sh\nexit 9\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	kubeconfig := filepath.Join(directory, "kubeconfig")
	t.Setenv("KUBECONFIG", kubeconfig)
	if err := os.WriteFile(kubeconfig, []byte(`apiVersion: v1
kind: Config
clusters:
- name: fixture
  cluster:
    server: https://127.0.0.1:1234
contexts:
- name: fixture
  context:
    cluster: fixture
    user: fixture
current-context: fixture
users:
- name: fixture
  user: {}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	artifacts := &nativeKubectlArtifacts{}
	cluster, err := newKubectlClusterWithArtifacts(artifacts)
	if err != nil || cluster.nativeArtifacts != artifacts || cluster.trustedCurrent != nil || cluster.verifier != "" {
		t.Fatalf("native cluster constructor still depends on legacy files: %v", err)
	}
	if _, err := newKubectlCluster(); err == nil || !strings.Contains(err.Error(), "registry verifier is unavailable") {
		t.Fatalf("test fixture unexpectedly has the legacy verifier: %v", err)
	}
}

package releaseguardian

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/declarativerelease"
)

func TestProcessExecutorBindsGuardianLeaseAndCanonicalReceipt(t *testing.T) {
	configureInClusterExecutorFixture(t)
	snapshot := processSnapshot(t)
	verified := processResult(t, "verified", "forward-verified")
	binary := writeExecutorFixture(t, "execute", snapshot.Record.RecordDigest, verified)
	executor, err := NewProcessExecutor(binary, "pod-uid")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := executor.Rollout(context.Background(), snapshot)
	if err != nil || receipt.Status != "verified" || receipt.RecordDigest != snapshot.Record.RecordDigest || receipt.ReceiptDigest != verified.ReceiptDigest {
		t.Fatalf("rollout receipt=%+v err=%v", receipt, err)
	}

	compensated := processResult(t, "compensated", "continuous-health-threshold-lkg-restored")
	binary = writeExecutorFixture(t, "restore-monitor", snapshot.Record.RecordDigest, compensated)
	executor, err = NewProcessExecutor(binary, "pod-uid")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err = executor.Rollback(context.Background(), snapshot)
	if err != nil || receipt.Status != "compensated" || receipt.RecordDigest != snapshot.Record.LKGRecordDigest {
		t.Fatalf("rollback receipt=%+v err=%v", receipt, err)
	}
}

func TestProcessExecutorFailsClosedOnMissingReceipt(t *testing.T) {
	configureInClusterExecutorFixture(t)
	snapshot := processSnapshot(t)
	path := filepath.Join(t.TempDir(), "executor")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	executor, err := NewProcessExecutor(path, "pod-uid")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Rollout(context.Background(), snapshot); err == nil || !strings.Contains(err.Error(), "result is unknown") {
		t.Fatalf("unknown child result was accepted: %v", err)
	}
}

func TestProcessExecutorFailsClosedWithoutInClusterKubeconfigMaterial(t *testing.T) {
	t.Setenv("KUBECONFIG", "")
	t.Setenv("KUBERNETES_SERVICE_HOST", "not-an-ip")
	t.Setenv("KUBERNETES_SERVICE_PORT_HTTPS", "443")
	snapshot := processSnapshot(t)
	binary := writeExecutorFixture(t, "execute", snapshot.Record.RecordDigest, processResult(t, "verified", "forward-verified"))
	executor, err := NewProcessExecutor(binary, "pod-uid")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Rollout(context.Background(), snapshot); err == nil || !strings.Contains(err.Error(), "host is invalid") {
		t.Fatalf("invalid in-cluster identity was accepted: %v", err)
	}
}

func TestWriteInClusterKubeconfigUsesAProtectedTokenFileReference(t *testing.T) {
	directory := t.TempDir()
	caPath := filepath.Join(directory, "ca.crt")
	tokenPath := filepath.Join(directory, "token")
	if err := os.WriteFile(caPath, []byte("test-ca\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	const token = "test-service-account-token"
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.43.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT_HTTPS", "443")
	path, err := writeInClusterKubeconfig(directory, caPath, tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("kubeconfig mode=%v err=%v", info.Mode().Perm(), err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "tokenFile: "+tokenPath) || strings.Contains(string(raw), token) {
		t.Fatalf("kubeconfig did not preserve token-file-only authentication: %q", raw)
	}
}

func processSnapshot(t *testing.T) Snapshot {
	t.Helper()
	key := Key{Component: "edge-control-de", Group: "de"}
	record, err := NewReleaseRecord(key, testSHA, testDigest, testDigest, otherDigest, testDigest)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{}
	for _, name := range executionFileNames {
		files[name] = []byte("{}")
	}
	monitor := map[string]string{}
	for _, name := range []string{"artifact-receipt.json", "execution-plan.json", "forward.json", "lkg.json", "record.json", "release-plan.json", "terminal-result.json"} {
		monitor[name] = "{}"
	}
	return Snapshot{
		Key: key, Record: record,
		Bundle:             ExecutionBundle{Prepared: declarativerelease.ExecutionPlan{Component: key.Component, ConfigSHA: testSHA}, Files: files},
		CurrentMonitorData: monitor, LKGMonitorRecordDigest: testDigest,
	}
}

func processResult(t *testing.T, status, reason string) declarativerelease.ExecutionResult {
	t.Helper()
	result := declarativerelease.ExecutionResult{
		APIVersion: declarativerelease.ExecutionPlanAPIVersion, Kind: declarativerelease.ExecutionResultKind,
		Component: "edge-control-de", ConfigSHA: testSHA, ExecutionPlanDigest: testDigest,
		Status: status, Reason: reason,
	}
	raw, err := declarativerelease.CanonicalJSON(result)
	if err != nil {
		t.Fatal(err)
	}
	result.ReceiptDigest = digest(raw)
	return result
}

func writeExecutorFixture(t *testing.T, operation, recordDigest string, result declarativerelease.ExecutionResult) string {
	t.Helper()
	raw, err := declarativerelease.CanonicalJSON(result)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "executor")
	expectedFiles := len(executionFileNames)
	if operation == "restore-monitor" {
		expectedFiles = 7
	}
	script := fmt.Sprintf("#!/bin/sh\nset -eu\ntest \"$1\" = %q\ntest \"$FUGUE_COMPONENT_LEASE_OWNER\" = guardian\ntest \"$FUGUE_RELEASE_GUARDIAN_POD_UID\" = pod-uid\ntest \"$FUGUE_RELEASE_GUARDIAN_RECORD_DIGEST\" = %q\ntest -f \"$KUBECONFIG\"\ntest \"${KUBECONFIG%%/*}\" != \"$2\"\ntest \"$(find \"$2\" -mindepth 1 -maxdepth 1 -type f | wc -l | tr -d ' ')\" = %d\ngrep -F \"tokenFile: $FUGUE_TEST_TOKEN_FILE\" \"$KUBECONFIG\" >/dev/null\n! grep -F \"$FUGUE_TEST_TOKEN_VALUE\" \"$KUBECONFIG\" >/dev/null\nprintf '%%s\\n' %q\n", operation, recordDigest, expectedFiles, string(raw))
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func configureInClusterExecutorFixture(t *testing.T) {
	t.Helper()
	directory := t.TempDir()
	caPath := filepath.Join(directory, "ca.crt")
	tokenPath := filepath.Join(directory, "token")
	if err := os.WriteFile(caPath, []byte("test-ca\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	const token = "test-service-account-token"
	if err := os.WriteFile(tokenPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", "")
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.43.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT_HTTPS", "443")
	t.Setenv("FUGUE_TEST_TOKEN_FILE", tokenPath)
	t.Setenv("FUGUE_TEST_TOKEN_VALUE", token)
	oldCA, oldToken := serviceAccountCAPath, serviceAccountTokenPath
	serviceAccountCAPath, serviceAccountTokenPath = caPath, tokenPath
	t.Cleanup(func() {
		serviceAccountCAPath, serviceAccountTokenPath = oldCA, oldToken
	})
}

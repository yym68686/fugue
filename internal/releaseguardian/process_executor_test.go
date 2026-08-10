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
	script := fmt.Sprintf("#!/bin/sh\nset -eu\ntest \"$1\" = %q\ntest \"$FUGUE_COMPONENT_LEASE_OWNER\" = guardian\ntest \"$FUGUE_RELEASE_GUARDIAN_POD_UID\" = pod-uid\ntest \"$FUGUE_RELEASE_GUARDIAN_RECORD_DIGEST\" = %q\nprintf '%%s\\n' %q\n", operation, recordDigest, string(raw))
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

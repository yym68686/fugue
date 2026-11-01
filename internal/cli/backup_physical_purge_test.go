package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

type physicalHistoryFake struct {
	base         *physicalDisableFake
	job, receipt map[string]any
	stale        bool
	mutable      bool
}

func physicalHistoryFixture(t *testing.T) (*physicalHistoryFake, physicalBackupOptions) {
	t.Helper()
	f, opts := physicalDisableFixture()
	if state, err := disablePhysicalBarman(context.Background(), f.kube, opts, false); err != nil || !state.Completed {
		t.Fatalf("fixture shutdown: %v", err)
	}
	physicalMap(f.cron, "metadata")["annotations"] = map[string]any{physicalHistoryCapability: "v1"}
	physicalMap(f.cron, "spec")["jobTemplate"] = map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
		"serviceAccountName": opts.policy,
		"volumes":            []any{map[string]any{"name": "code", "configMap": map[string]any{"name": opts.policy + "-123456789abc"}}},
		"containers": []any{map[string]any{"name": "retention", "command": []any{"python3", "/retention/worker.py", "--worker"}, "args": []any{"old"}, "env": []any{
			map[string]any{"name": "POLICY_NAME", "value": "wrong"}, map[string]any{"name": "HISTORY_EXECUTE", "value": "true"},
			map[string]any{"name": "AWS_ACCESS_KEY_ID", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "sample-secret", "key": "access"}}},
		}}},
	}}}}
	return &physicalHistoryFake{base: f}, opts
}

func (f *physicalHistoryFake) kube(ctx context.Context, body any, args ...string) (map[string]any, error) {
	if args[0] == "get" && args[1] == "configmap" && strings.HasSuffix(args[2], "-history-receipt") {
		meta := physicalMap(f.base.cron, "metadata")
		raw := "{}"
		if f.receipt != nil {
			raw = mustPhysicalHistoryJSON(f.receipt)
		}
		return map[string]any{"metadata": map[string]any{"name": args[2], "namespace": meta["namespace"]}, "data": map[string]any{"receipt.json": raw}}, nil
	}
	if args[0] == "get" && args[1] == "configmap" && strings.HasSuffix(args[2], "-123456789abc") {
		return map[string]any{"immutable": !f.mutable, "data": map[string]any{"history.py": "installed-history-worker", "worker.py": "installed-retention-worker"}}, nil
	}
	if args[0] == "create" {
		f.job = physicalClone(body.(map[string]any))
		meta := physicalMap(f.job, "metadata")
		meta["name"], meta["uid"] = "sample-history-job", "job-uid"
		operation := physicalString(physicalMap(meta, "annotations"), "fugue.pro/history-operation")
		if f.stale {
			operation = "old-operation"
		}
		f.receipt = map[string]any{"state": "planned", "operation_id": operation, "job_uid": "job-uid"}
		return physicalClone(f.job), nil
	}
	if args[0] == "get" && args[1] == "job" {
		return map[string]any{"metadata": map[string]any{"uid": "job-uid"}, "status": map[string]any{"succeeded": float64(1)}}, nil
	}
	return f.base.kube(ctx, body, args...)
}

func mustPhysicalHistoryJSON(value map[string]any) string {
	// physicalClone already tests JSON handling throughout these fixtures.
	var buffer bytes.Buffer
	_ = json.NewEncoder(&buffer).Encode(value)
	return buffer.String()
}

func TestPhysicalHistoryDefaultsToPreviewAndBindsReceiptToJob(t *testing.T) {
	f, opts := physicalHistoryFixture(t)
	out, err := startPhysicalHistory(context.Background(), f.kube, opts, physicalHistoryOptions{wait: true})
	if err != nil || out["dry_run"] != true || out["completed"] != true {
		t.Fatalf("preview: %+v %v", out, err)
	}
	spec := physicalMap(f.job, "spec")
	pod := physicalMap(physicalMap(spec, "template"), "spec")
	container := pod["containers"].([]any)[0].(map[string]any)
	if fmt.Sprint(container["command"]) != "[python3 /retention/history.py]" || container["args"] != nil {
		t.Fatalf("wrong worker command: %+v", container)
	}
	env := map[string]map[string]any{}
	for _, item := range container["env"].([]any) {
		e := item.(map[string]any)
		name := physicalString(e, "name")
		if _, duplicate := env[name]; duplicate {
			t.Fatalf("duplicate env: %s", name)
		}
		env[name] = e
	}
	if env["HISTORY_EXECUTE"]["value"] != "false" || env["POLICY_NAME"]["value"] != opts.policy || env["AWS_ACCESS_KEY_ID"]["valueFrom"] == nil || env["HISTORY_JOB_UID"]["valueFrom"] == nil {
		t.Fatalf("unsafe Job environment: %+v", env)
	}
	f, opts = physicalHistoryFixture(t)
	f.stale = true
	if _, err := startPhysicalHistory(context.Background(), f.kube, opts, physicalHistoryOptions{wait: true}); err == nil || !strings.Contains(err.Error(), "matching receipt") {
		t.Fatalf("stale receipt accepted: %v", err)
	}
}

func TestPhysicalHistoryRefusesOldExecutorIncompleteShutdownAndStalePreview(t *testing.T) {
	for _, mutation := range []func(*physicalHistoryFake){
		func(f *physicalHistoryFake) {
			delete(physicalMap(physicalMap(f.base.cron, "metadata"), "annotations"), physicalHistoryCapability)
		},
		func(f *physicalHistoryFake) { physicalMap(f.base.cron, "spec")["suspend"] = false },
		func(f *physicalHistoryFake) { physicalMap(f.base.base.cluster, "status")["phase"] = "Failing over" },
		func(f *physicalHistoryFake) { f.mutable = true },
	} {
		f, opts := physicalHistoryFixture(t)
		mutation(f)
		if _, err := startPhysicalHistory(context.Background(), f.kube, opts, physicalHistoryOptions{}); err == nil || f.job != nil {
			t.Fatalf("unsafe preview launched: %v", err)
		}
	}
	f, opts := physicalHistoryFixture(t)
	options := physicalHistoryOptions{execute: true, ackAll: true, ackQuiet: true, planSHA: strings.Repeat("a", 64)}
	if _, err := startPhysicalHistory(context.Background(), f.kube, opts, options); err == nil || f.job != nil {
		t.Fatalf("execution without preview launched: %v", err)
	}
	f.receipt = map[string]any{"state": "planned", "can_execute": true, "plan_sha256": options.planSHA, "observed_at": time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)}
	if _, err := startPhysicalHistory(context.Background(), f.kube, opts, options); err == nil || f.job != nil {
		t.Fatalf("expired preview launched: %v", err)
	}
	f.receipt["observed_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	if out, err := startPhysicalHistory(context.Background(), f.kube, opts, options); err != nil || out["dry_run"] != false || f.job == nil {
		t.Fatalf("confirmed Job not launched: %+v %v", out, err)
	}
}

func TestPhysicalHistoryAcknowledgementsCheckedBeforeKubernetes(t *testing.T) {
	for _, flags := range [][]string{{"--execute"}, {"--execute", "--ack-delete-all-history"}, {"--plan-sha256", strings.Repeat("a", 64)}, {"--status", "--execute"}, {"--execute", "--dry-run"}} {
		var stdout, stderr bytes.Buffer
		args := append([]string{"--base-url", "http://127.0.0.1:1", "admin", "backup", "physical", "purge"}, flags...)
		err := runWithStreams(args, &stdout, &stderr)
		if err == nil || strings.Contains(err.Error(), "Kubernetes") {
			t.Fatalf("expected local flag validation: %v", err)
		}
	}
	f, opts := physicalHistoryFixture(t)
	if _, err := startPhysicalHistory(context.Background(), f.kube, opts, physicalHistoryOptions{status: true}); err != nil || f.job != nil {
		t.Fatalf("status should only read receipt: %v", err)
	}
}

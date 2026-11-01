package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

type physicalDisableFake struct {
	base                       *physicalFake
	cron                       map[string]any
	schedules, backups, others []map[string]any
	writes                     []string
	failResource               string
	truncated                  bool
}

func physicalDisableFixture() (*physicalDisableFake, physicalBackupOptions) {
	base, opts := physicalFixture()
	meta := func(name string) map[string]any {
		return map[string]any{"name": name, "namespace": opts.namespace, "uid": name + "-uid", "resourceVersion": "1"}
	}
	return &physicalDisableFake{base: base,
		cron:      map[string]any{"metadata": meta(opts.policy), "spec": map[string]any{"suspend": false}},
		schedules: []map[string]any{{"metadata": meta("daily-physical"), "spec": map[string]any{"cluster": map[string]any{"name": "sample-db"}, "method": "barmanObjectStore"}}},
	}, opts
}

func (f *physicalDisableFake) kube(ctx context.Context, body any, args ...string) (map[string]any, error) {
	if args[0] == "get" {
		if args[1] == "cronjob" {
			return physicalClone(f.cron), nil
		}
		var objects []map[string]any
		switch args[1] {
		case "scheduledbackups.postgresql.cnpg.io":
			objects = f.schedules
		case "backups.postgresql.cnpg.io":
			objects = f.backups
		case "clusters.postgresql.cnpg.io":
			if args[2] != "--chunk-size=0" {
				return f.base.kube(ctx, body, args...)
			}
			objects = append([]map[string]any{f.base.cluster}, f.others...)
		default:
			return f.base.kube(ctx, body, args...)
		}
		items := []any{}
		for _, object := range objects {
			items = append(items, physicalClone(object))
		}
		meta := map[string]any{}
		if f.truncated {
			meta["continue"] = "next-page"
		}
		return map[string]any{"metadata": meta, "items": items}, nil
	}
	if args[0] == "replace" {
		if physicalMap(body.(map[string]any), "data")["policy.json"] != nil {
			f.writes = append(f.writes, "intent")
		}
		return f.base.kube(ctx, body, args...)
	}
	if args[0] != "patch" {
		return nil, fmt.Errorf("unexpected operation %v", args)
	}
	resource := args[1]
	if f.failResource == resource {
		return nil, fmt.Errorf("synthetic CAS conflict")
	}
	var target map[string]any
	switch resource {
	case "clusters.postgresql.cnpg.io":
		target = f.base.cluster
	case "cronjob":
		target = f.cron
	case "scheduledbackups.postgresql.cnpg.io":
		for _, object := range f.schedules {
			if physicalString(physicalMap(object, "metadata"), "name") == args[2] {
				target = object
			}
		}
	}
	if target == nil {
		return nil, fmt.Errorf("unknown patch target")
	}
	var patch []map[string]any
	if err := json.Unmarshal([]byte(args[5]), &patch); err != nil {
		return nil, err
	}
	for _, operation := range patch {
		parts := strings.Split(strings.TrimPrefix(physicalString(operation, "path"), "/"), "/")
		parent := target
		for i, part := range parts {
			part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
			if i < len(parts)-1 {
				parent = physicalMap(parent, part)
				continue
			}
			switch operation["op"] {
			case "test":
				if parent[part] != operation["value"] {
					return nil, fmt.Errorf("CAS test failed")
				}
			case "add":
				parent[part] = operation["value"]
			case "remove":
				if _, ok := parent[part]; !ok {
					return nil, fmt.Errorf("remove path missing")
				}
				delete(parent, part)
			default:
				return nil, fmt.Errorf("unexpected patch operation")
			}
		}
	}
	physicalMap(target, "metadata")["resourceVersion"] = physicalString(physicalMap(target, "metadata"), "resourceVersion") + "1"
	f.writes = append(f.writes, resource)
	return physicalClone(target), nil
}

func TestPhysicalBarmanDisablePreservesHistoryAndUnrelatedSettings(t *testing.T) {
	f, opts := physicalDisableFixture()
	beforeStorage := physicalString(physicalMap(physicalMap(f.base.cluster, "spec"), "storage"), "size")
	state, err := disablePhysicalBarman(context.Background(), f.kube, opts, false)
	if err != nil || !state.Completed || state.State != "disabled" || state.WALArchivingConfigured || state.HistoryDeleted || state.RuntimeVerified {
		t.Fatalf("invalid shutdown result: %+v %v", state, err)
	}
	if physicalString(physicalMap(physicalMap(f.base.cluster, "spec"), "storage"), "size") != beforeStorage || physicalMap(physicalMap(f.base.cluster, "spec"), "backup")["target"] != "prefer-standby" {
		t.Fatal("changed unrelated database settings")
	}
	_, _, policy, err := loadPhysicalBackup(context.Background(), f.kube, opts)
	if err != nil || !policy.BarmanDisabled || policy.Enabled {
		t.Fatalf("disable intent is unreadable: %+v %v", policy, err)
	}
	if _, err := configurePhysicalBackup(context.Background(), f.kube, opts, 3); err == nil {
		t.Fatal("retention configure unexpectedly re-enabled Barman")
	}
	writes := len(f.writes)
	state, err = disablePhysicalBarman(context.Background(), f.kube, opts, false)
	if err != nil || !state.Completed || len(f.writes) != writes {
		t.Fatalf("disable is not idempotent: %+v %v %v", state, err, f.writes)
	}
}

func TestPhysicalBarmanDisableDrainsRunningBackupAndCanResume(t *testing.T) {
	f, opts := physicalDisableFixture()
	f.backups = []map[string]any{{"metadata": map[string]any{"name": "upload-in-progress"}, "spec": map[string]any{"cluster": map[string]any{"name": "sample-db"}}, "status": map[string]any{"phase": "running"}}}
	state, err := disablePhysicalBarman(context.Background(), f.kube, opts, false)
	if err != nil || state.Completed || state.State != "disable_pending" || !state.WALArchivingConfigured || !state.RetentionSuspended || !state.SchedulesSuspended || len(state.ActiveBackups) != 1 {
		t.Fatalf("active backup was not drained safely: %+v %v", state, err)
	}
	physicalMap(f.backups[0], "status")["phase"] = "completed"
	state, err = disablePhysicalBarman(context.Background(), f.kube, opts, false)
	if err != nil || !state.Completed {
		t.Fatalf("resume failed: %+v %v", state, err)
	}
}

func TestPhysicalBarmanDisableDryRunHasNoWritesAndDoesNotAcquireLease(t *testing.T) {
	f, opts := physicalDisableFixture()
	physicalMap(f.base.lease, "spec")["holderIdentity"] = "another-worker"
	state, err := disablePhysicalBarman(context.Background(), f.kube, opts, true)
	if err != nil || state.DisableRequested || len(f.writes) != 0 || len(f.base.writes) != 0 {
		t.Fatalf("dry run mutated resources: %+v %v", state, err)
	}
}

func TestPhysicalBarmanDisableConflictIsResumableWithoutFalseSuccess(t *testing.T) {
	for _, resource := range []string{"clusters.postgresql.cnpg.io", "cronjob", "scheduledbackups.postgresql.cnpg.io"} {
		t.Run(resource, func(t *testing.T) {
			f, opts := physicalDisableFixture()
			f.failResource = resource
			if _, err := disablePhysicalBarman(context.Background(), f.kube, opts, false); err == nil {
				t.Fatal("conflict reported success")
			}
			if physicalMap(physicalMap(f.base.cluster, "spec"), "backup")["barmanObjectStore"] == nil {
				t.Fatal("removed archive after partial shutdown")
			}
			f.failResource = ""
			if state, err := disablePhysicalBarman(context.Background(), f.kube, opts, false); err != nil || !state.Completed {
				t.Fatalf("cannot retry partial shutdown: %+v %v", state, err)
			}
		})
	}
}

func TestPhysicalBarmanDisableRejectsUnsafeInventoriesOwnersPluginsAndRestores(t *testing.T) {
	for _, mutation := range []string{"truncated", "owner", "plugin", "restore", "replica", "database-identity"} {
		t.Run(mutation, func(t *testing.T) {
			f, opts := physicalDisableFixture()
			switch mutation {
			case "truncated":
				f.truncated = true
			case "owner":
				physicalMap(f.base.cluster, "metadata")["annotations"] = map[string]any{physicalBarmanDisabledAnnotation: "foreign-uid"}
			case "plugin":
				physicalMap(f.base.cluster, "spec")["plugins"] = []any{map[string]any{"isWALArchiver": true}}
			case "database-identity":
				physicalMap(f.base.cluster, "status")["systemID"] = "other"
			case "restore", "replica":
				f.others = []map[string]any{{"metadata": map[string]any{"uid": "restore-uid"}, "spec": map[string]any{"externalClusters": []any{map[string]any{"name": "sample-db", "barmanObjectStore": map[string]any{"destinationPath": "s3://sample/archive"}}}}, "status": map[string]any{"phase": "Setting up primary"}}}
				if mutation == "replica" {
					physicalMap(f.others[0], "status")["phase"] = "Cluster in healthy state"
					physicalMap(f.others[0], "spec")["replica"] = map[string]any{"enabled": true}
				}
			}
			if _, err := disablePhysicalBarman(context.Background(), f.kube, opts, false); err == nil || len(f.writes) != 0 {
				t.Fatalf("unsafe mutation accepted: %v %v", err, f.writes)
			}
		})
	}
}

func TestPhysicalBarmanDisableDoesNotTouchOtherSchedulesAndWaitsForHealth(t *testing.T) {
	f, opts := physicalDisableFixture()
	other := physicalClone(f.schedules[0])
	physicalMap(other, "metadata")["name"] = "other-database"
	physicalMap(physicalMap(other, "spec"), "cluster")["name"] = "other-db"
	snapshot := physicalClone(f.schedules[0])
	physicalMap(snapshot, "metadata")["name"] = "csi-snapshot"
	physicalMap(snapshot, "spec")["method"] = "volumeSnapshot"
	f.schedules = append(f.schedules, other, snapshot)
	physicalMap(f.base.cluster, "status")["phase"] = "Not enough disk space"
	state, err := disablePhysicalBarman(context.Background(), f.kube, opts, false)
	if err != nil || state.Completed || !state.WALArchivingConfigured {
		t.Fatalf("unhealthy database archive removed: %+v %v", state, err)
	}
	if physicalMap(other, "spec")["suspend"] != nil || physicalMap(snapshot, "spec")["suspend"] != nil {
		t.Fatal("changed unrelated backup schedules")
	}
}

func TestPhysicalBarmanDisableRequiresExplicitAcknowledgement(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", "http://127.0.0.1:1", "admin", "backup", "physical", "disable"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "--ack-loss-of-pitr") {
		t.Fatalf("expected recovery acknowledgement, not an API request: %v", err)
	}
}

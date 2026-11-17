package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type retirementFake struct {
	t                                              *testing.T
	pvc, pv, node, system, cluster, cm, lease      map[string]any
	objects, extraVolumes                          []any
	writes                                         []string
	hostCalls                                      int
	failHost, failCompletion, referenceAfterDelete bool
}

func retirementClone(m map[string]any) map[string]any {
	raw, _ := json.Marshal(m)
	var result map[string]any
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	_ = d.Decode(&result)
	if result == nil {
		result = map[string]any{}
	}
	return result
}

func newRetirementFake(t *testing.T) *retirementFake {
	f := &retirementFake{t: t}
	f.pvc = map[string]any{"metadata": map[string]any{"name": "archive-db-2", "namespace": "test-system", "uid": "claim-uid", "resourceVersion": "1", "annotations": map[string]any{"cnpg.io/pvcStatus": "detached"}, "labels": map[string]any{"cnpg.io/cluster": "archive-db"}}, "spec": map[string]any{"volumeName": "archive-pv"}}
	f.pv = map[string]any{"metadata": map[string]any{"name": "archive-pv", "uid": "volume-uid", "resourceVersion": "2"}, "spec": map[string]any{"persistentVolumeReclaimPolicy": "Retain", "local": map[string]any{"path": "/var/lib/storage/archive"}, "claimRef": map[string]any{"namespace": "test-system", "name": "archive-db-2", "uid": "claim-uid"}, "nodeAffinity": map[string]any{"required": map[string]any{"nodeSelectorTerms": []any{map[string]any{"matchExpressions": []any{map[string]any{"key": "kubernetes.io/hostname", "operator": "In", "values": []any{"storage-node"}}}}}}}}, "status": map[string]any{"phase": "Bound"}}
	f.node = map[string]any{"metadata": map[string]any{"uid": "node-uid", "labels": map[string]any{"kubernetes.io/hostname": "storage-node"}}, "status": map[string]any{"nodeInfo": map[string]any{"machineID": "machine-id"}, "conditions": []any{map[string]any{"type": "Ready", "status": "True"}}}}
	f.system = map[string]any{"metadata": map[string]any{"uid": "cluster-uid"}}
	f.cluster = map[string]any{"metadata": map[string]any{"uid": "database-uid"}, "spec": map[string]any{}, "status": map[string]any{"phase": "Cluster in healthy state", "readyInstances": json.Number("1"), "currentPrimary": "archive-db-5", "targetPrimary": "archive-db-5", "healthyPVC": []any{"archive-db-5"}}}
	return f
}

func (f *retirementFake) kube(_ context.Context, body any, args ...string) (map[string]any, error) {
	if args[0] == "get" {
		var value map[string]any
		switch args[1] {
		case "pvc":
			value = f.pvc
		case "pv":
			if args[2] == "-o" {
				items := append([]any{}, f.extraVolumes...)
				if len(f.pv) > 0 {
					items = append(items, f.pv)
				}
				return retirementClone(map[string]any{"items": items}), nil
			}
			value = f.pv
		case "node":
			value = f.node
		case "namespace":
			value = f.system
		case "configmap":
			value = f.cm
		case "lease":
			value = f.lease
		case "clusters.postgresql.cnpg.io":
			if args[2] == "-o" {
				return retirementClone(map[string]any{"items": []any{f.cluster}}), nil
			}
			value = f.cluster
		default:
			return retirementClone(map[string]any{"items": f.objects}), nil
		}
		return retirementClone(value), nil
	}
	f.writes = append(f.writes, strings.Join(args, " "))
	object, _ := body.(map[string]any)
	if args[0] == "delete" {
		var target *map[string]any
		if strings.Contains(args[2], "/persistentvolumeclaims/") {
			target = &f.pvc
		} else {
			target = &f.pv
		}
		meta := physicalMap(*target, "metadata")
		pre := physicalMap(object, "preconditions")
		if pre["uid"] != meta["uid"] || pre["resourceVersion"] != meta["resourceVersion"] {
			return nil, fmt.Errorf("delete precondition conflict")
		}
		if len(f.cm) == 0 {
			f.t.Fatal("delete attempted without durable receipt")
		}
		if target == &f.pv && f.hostCalls == 0 {
			f.t.Fatal("PV deleted before physical verification")
		}
		*target = nil
		if target == &f.pvc {
			physicalMap(f.pv, "status")["phase"] = "Released"
			if f.referenceAfterDelete {
				f.objects = []any{map[string]any{"kind": "Job", "spec": map[string]any{"source": "archive-db-2"}}}
			}
		}
		return map[string]any{}, nil
	}
	if physicalString(object, "kind") == "Lease" {
		f.lease = retirementClone(object)
		return retirementClone(f.lease), nil
	}
	var receipt map[string]any
	_ = json.Unmarshal([]byte(physicalString(physicalMap(object, "data"), "receipt.json")), &receipt)
	if physicalString(receipt, "phase") == "completed" && f.failCompletion {
		return nil, fmt.Errorf("completion write unavailable")
	}
	f.cm = retirementClone(object)
	return retirementClone(f.cm), nil
}

func (f *retirementFake) host(_ context.Context, request map[string]any) (map[string]any, error) {
	if request["mode"] == "inspect" {
		return map[string]any{"host": map[string]any{"hostname": "storage-node", "machine_id": "machine-id", "boot_id": "boot-id"}, "tree": map[string]any{"device": 42, "inode": 9007199254740993, "allocated_bytes": 8192, "entry_count": 2, "tree_sha256": "tree-hash"}, "blockers": []any{}}, nil
	}
	f.hostCalls++
	if len(f.pvc) > 0 || physicalString(physicalMap(f.pv, "status"), "phase") != "Released" && len(f.pv) > 0 {
		f.t.Fatal("data deletion without a released, retained PV")
	}
	if f.failHost {
		return nil, fmt.Errorf("node interruption")
	}
	return map[string]any{"state": "data_deleted", "plan_sha256": request["plan_sha256"], "pv_uid": "volume-uid", "verified_absent": true, "deleted_allocated_bytes": 8192}, nil
}

func (f *retirementFake) plan() retirementPreview {
	f.t.Helper()
	p, err := planVolumeRetirement(context.Background(), f.kube, f.host, volumeRetirementOptions{namespace: "test-system", storageRoot: "/var/lib/storage"}, "archive-db-2")
	if err != nil {
		f.t.Fatal(err)
	}
	return p
}

func TestVolumeRetirementPreviewAndComplete(t *testing.T) {
	f := newRetirementFake(t)
	p := f.plan()
	if !p.CanExecute || len(f.writes) != 0 || p.Plan.Tree.Inode != 9007199254740993 {
		t.Fatalf("unsafe or imprecise preview: %+v", p)
	}
	r, err := executeVolumeRetirement(context.Background(), f.kube, f.host, p, "authorized old replica retirement")
	if err != nil || r["phase"] != "completed" || len(f.pv) != 0 || len(f.pvc) != 0 {
		t.Fatalf("result=%v err=%v", r, err)
	}
	calls := f.hostCalls
	_, err = executeVolumeRetirement(context.Background(), f.kube, f.host, p, "repeat")
	if err != nil || f.hostCalls != calls {
		t.Fatalf("repeat was not idempotent: %v", err)
	}
}

func TestVolumeRetirementChangedReferencesBlockBeforeDeletion(t *testing.T) {
	for _, kind := range []string{"workload", "hold", "node", "primary", "affinity", "overlap", "template"} {
		t.Run(kind, func(t *testing.T) {
			f := newRetirementFake(t)
			p := f.plan()
			switch kind {
			case "workload":
				f.objects = []any{map[string]any{"kind": "CronJob", "spec": map[string]any{"persistentVolumeClaim": map[string]any{"claimName": "archive-db-2"}}}}
			case "hold":
				physicalMap(f.pv, "metadata")["annotations"] = map[string]any{"fugue.pro/retirement-protected": "legal hold"}
			case "node":
				physicalMap(f.node, "metadata")["uid"] = "replacement"
			case "primary":
				physicalMap(f.cluster, "status")["currentPrimary"] = "archive-db-2"
			case "affinity":
				physicalMap(f.pv, "spec")["nodeAffinity"] = map[string]any{}
			case "overlap":
				f.extraVolumes = []any{map[string]any{"metadata": map[string]any{"uid": "other-volume"}, "spec": map[string]any{"hostPath": map[string]any{"path": "/var/lib/storage"}}}}
			case "template":
				f.objects = []any{map[string]any{"kind": "StatefulSet", "metadata": map[string]any{"name": "db"}, "spec": map[string]any{"volumeClaimTemplates": []any{map[string]any{"metadata": map[string]any{"name": "archive"}}}}}}
			}
			_, err := executeVolumeRetirement(context.Background(), f.kube, f.host, p, "test")
			if err == nil || len(f.pvc) == 0 || f.hostCalls != 0 {
				t.Fatalf("unsafe execution: err=%v", err)
			}
		})
	}
}

func TestVolumeRetirementResumesNodeFailureAndLostCompletion(t *testing.T) {
	for _, failure := range []string{"node", "completion"} {
		t.Run(failure, func(t *testing.T) {
			f := newRetirementFake(t)
			p := f.plan()
			f.failHost = failure == "node"
			f.failCompletion = failure == "completion"
			_, err := executeVolumeRetirement(context.Background(), f.kube, f.host, p, "test")
			if err == nil || len(f.cm) == 0 {
				t.Fatal("failure was not durable")
			}
			if failure == "node" && len(f.pv) == 0 {
				t.Fatal("PV lost before successful node deletion")
			}
			f.failHost = false
			f.failCompletion = false
			r, err := executeVolumeRetirement(context.Background(), f.kube, f.host, p, "resume")
			if err != nil || r["phase"] != "completed" {
				t.Fatalf("resume=%v %v", r, err)
			}
		})
	}
}

func TestVolumeRetirementReferenceAfterClaimDeletionPreservesData(t *testing.T) {
	f := newRetirementFake(t)
	p := f.plan()
	f.referenceAfterDelete = true
	_, err := executeVolumeRetirement(context.Background(), f.kube, f.host, p, "test")
	if err == nil || f.hostCalls != 0 || len(f.pv) == 0 || len(f.pvc) != 0 {
		t.Fatalf("unsafe partial retirement: %v", err)
	}
}

func TestVolumeRetirementPlanTamperRejected(t *testing.T) {
	f := newRetirementFake(t)
	p := f.plan()
	filename := filepath.Join(t.TempDir(), "plan.json")
	raw, _ := json.Marshal(p)
	if err := os.WriteFile(filename, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRetirementPlan(filename, p.SHA256); err != nil {
		t.Fatal(err)
	}
	p.Plan.Path = "/var/lib/storage/other"
	raw, _ = json.Marshal(p)
	_ = os.WriteFile(filename, raw, 0600)
	if _, err := loadRetirementPlan(filename, p.SHA256); err == nil {
		t.Fatal("tampered target accepted")
	}
}

func TestVolumeRetirementTerminalJobRequiresExplicitPolicy(t *testing.T) {
	f := newRetirementFake(t)
	job := map[string]any{"kind": "Job", "spec": map[string]any{"source": "archive-db-2"}, "status": map[string]any{"conditions": []any{map[string]any{"type": "Failed", "status": "True"}}}}
	f.objects = []any{job}
	p := f.plan()
	if p.CanExecute {
		t.Fatal("terminal Job history silently ignored")
	}
	p.Plan.AllowTerminalJobs = true
	blockers, err := reviewRetirement(context.Background(), f.kube, p.Plan, false)
	if err != nil || len(blockers) > 0 {
		t.Fatalf("explicit terminal history policy blocked: %v %v", blockers, err)
	}
	physicalMap(job, "status")["active"] = json.Number("1")
	blockers, err = reviewRetirement(context.Background(), f.kube, p.Plan, false)
	if err != nil || len(blockers) == 0 {
		t.Fatal("active Job allowed")
	}
	delete(physicalMap(job, "status"), "active")
	physicalMap(job, "status")["conditions"] = []any{map[string]any{"type": "FailureTarget", "status": "True"}}
	if retirementTerminalJob(job) {
		t.Fatal("FailureTarget is not terminal completion")
	}
}

func TestVolumeRetirementBroadAdministrativeMountIsNotOwnership(t *testing.T) {
	for _, source := range []string{"/", "/var/lib", "/var/lib/storage"} {
		if retirementHasReference(map[string]any{"hostPath": map[string]any{"path": source}}, "archive-db-2", "/var/lib/storage/archive") {
			t.Fatalf("broad admin mount counted as volume ownership: %s", source)
		}
	}
	for _, source := range []string{"/var/lib/storage/archive", "/var/lib/storage/archive/data"} {
		if !retirementHasReference(map[string]any{"hostPath": map[string]any{"path": source}}, "archive-db-2", "/var/lib/storage/archive") {
			t.Fatalf("missed direct volume mount: %s", source)
		}
	}
}

func TestVolumeRetirementReceiptPreservesLargeInode(t *testing.T) {
	f := newRetirementFake(t)
	p := f.plan()
	if _, err := executeVolumeRetirement(context.Background(), f.kube, f.host, p, "test"); err != nil {
		t.Fatal(err)
	}
	receipt, err := executeVolumeRetirement(context.Background(), f.kube, f.host, p, "read existing receipt")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(receipt)
	if err != nil || !strings.Contains(string(raw), `"inode":9007199254740993`) {
		t.Fatalf("receipt rounded a pinned inode identity: %s %v", raw, err)
	}
}

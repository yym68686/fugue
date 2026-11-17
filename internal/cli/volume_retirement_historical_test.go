package cli

import (
	"context"
	"strings"
	"testing"
)

func historicalRetirementFake(t *testing.T, source string) (*retirementFake, retirementPreview) {
	t.Helper()
	f := newRetirementFake(t)
	opts := volumeRetirementOptions{namespace: "test-system", storageRoot: "/var/lib/storage", databaseCluster: "archive-db"}
	if source == "released-pv" {
		f.pvc = nil
		physicalMap(f.pv, "status")["phase"] = "Released"
		opts.releasedPV = "archive-pv"
	} else {
		// A replacement PVC can reuse the old instance name. It must never be
		// deleted just because an unrelated historical directory has that name.
		f.pv = nil
		opts.directory, opts.formerPV, opts.node = "/var/lib/storage/archive", "archive-pv", "storage-node"
		f.extraVolumes = []any{map[string]any{"metadata": map[string]any{"uid": "current-volume"}, "spec": map[string]any{"local": map[string]any{"path": "/var/lib/storage/current"}}}}
	}
	p, err := planSelectedVolumeRetirement(context.Background(), f.kube, f.host, opts, nil)
	if err != nil || !p.CanExecute || len(f.writes) != 0 {
		t.Fatalf("historical preview=%+v err=%v writes=%v", p, err, f.writes)
	}
	return f, p
}

func TestHistoricalVolumeRetirementCompletesAndResumes(t *testing.T) {
	for _, source := range []string{"released-pv", "orphan-directory"} {
		for _, failure := range []string{"", "node", "completion"} {
			t.Run(source+"/"+failure, func(t *testing.T) {
				f, p := historicalRetirementFake(t, source)
				f.failHost, f.failCompletion = failure == "node", failure == "completion"
				originalPVC := retirementClone(f.pvc)
				_, err := executeVolumeRetirement(context.Background(), f.kube, f.host, p, "authorized historical data retirement")
				if failure != "" && (err == nil || len(f.cm) == 0) {
					t.Fatalf("failure was not durable: %v", err)
				}
				if failure == "" && err != nil {
					t.Fatal(err)
				}
				f.failHost, f.failCompletion = false, false
				r, err := executeVolumeRetirement(context.Background(), f.kube, f.host, p, "resume same plan")
				if err != nil || r["phase"] != "completed" || len(f.pv) != 0 || len(f.pvc) != len(originalPVC) {
					t.Fatalf("result=%v err=%v", r, err)
				}
				for _, write := range f.writes {
					if strings.Contains(write, "/persistentvolumeclaims/") {
						t.Fatal("historical cleanup deleted a PVC")
					}
				}
				calls := f.hostCalls
				_, err = executeVolumeRetirement(context.Background(), f.kube, f.host, p, "repeat")
				if err != nil || calls != f.hostCalls {
					t.Fatalf("repeat was not idempotent: %v", err)
				}
			})
		}
	}
}

func TestHistoricalVolumeRetirementBlocksNewOwners(t *testing.T) {
	for _, source := range []string{"released-pv", "orphan-directory"} {
		for _, change := range []string{"overlap", "workload-path", "database", "source", "registration"} {
			t.Run(source+"/"+change, func(t *testing.T) {
				f, p := historicalRetirementFake(t, source)
				switch change {
				case "overlap":
					f.extraVolumes = append(f.extraVolumes, map[string]any{"metadata": map[string]any{"uid": "new-owner"}, "spec": map[string]any{"local": map[string]any{"path": p.Plan.Path + "/data"}}})
				case "workload-path":
					f.objects = []any{map[string]any{"kind": "Job", "spec": map[string]any{"source": p.Plan.Path}}}
				case "database":
					physicalMap(f.cluster, "metadata")["uid"] = "replacement-cluster"
				case "source":
					p.Plan.Source = "unknown"
					p.SHA256 = retirementDigest(p.Plan)
				case "registration":
					if source == "released-pv" {
						f.pvc = map[string]any{"metadata": map[string]any{"uid": "replacement-claim"}}
					} else {
						f.pv = map[string]any{"metadata": map[string]any{"uid": "replacement-volume"}}
					}
				}
				_, err := executeVolumeRetirement(context.Background(), f.kube, f.host, p, "test")
				if err == nil || f.hostCalls != 0 {
					t.Fatalf("unsafe historical deletion: %v", err)
				}
				for _, write := range f.writes {
					if strings.HasPrefix(write, "delete ") {
						t.Fatal("blocked target deleted Kubernetes ownership")
					}
				}
			})
		}
	}
}

func TestHistoricalVolumeRetirementSelectors(t *testing.T) {
	f := newRetirementFake(t)
	for _, opts := range []volumeRetirementOptions{
		{},
		{releasedPV: "archive-pv", directory: "/var/lib/storage/archive"},
		{directory: "/var/lib/storage/archive"},
		{node: "storage-node"},
	} {
		if _, err := planSelectedVolumeRetirement(context.Background(), f.kube, f.host, opts, nil); err == nil {
			t.Fatalf("ambiguous selectors accepted: %+v", opts)
		}
	}
	if retirementHasReference(map[string]any{"unrelated": ""}, "", "/var/lib/storage/archive") {
		t.Fatal("empty orphan claim matched unrelated objects")
	}
}

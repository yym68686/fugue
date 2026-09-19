package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type storagePlacementFixture struct {
	deployment      *kubeDeployment
	rsDeploymentUID string
	readyEndpoint   bool

	pv          corev1.PersistentVolume
	pvc         corev1.PersistentVolumeClaim
	class       storagev1.StorageClass
	ready       []string
	drivers     []string
	volumeNode  string
	volumeState string
	attachments storagev1.VolumeAttachmentList
	capacities  storagev1.CSIStorageCapacityList
	pods        []kubePod
	failPath    string
	reads       map[string]int
	client      *kubeClient
}

func newStoragePlacementFixture(t *testing.T) *storagePlacementFixture {
	t.Helper()
	class := "network-storage"
	f := &storagePlacementFixture{ready: []string{"storage-ready"}, drivers: []string{"compute-only", "storage-ready"}, volumeState: "detached", reads: map[string]int{}}
	f.class = storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: class}, Provisioner: longhornCSIDriver}
	f.pvc = corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "tenant", UID: "claim-uid"}, Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &class, VolumeName: "pv-data", AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}}}
	f.pv = corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-data"}, Spec: corev1.PersistentVolumeSpec{StorageClassName: class, ClaimRef: &corev1.ObjectReference{Name: "data", Namespace: "tenant", UID: "claim-uid"}, PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: longhornCSIDriver, VolumeHandle: "volume-data"}}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("preflight attempted %s", r.Method)
			http.Error(w, "writes forbidden", 405)
			return
		}
		f.reads[r.URL.Path]++
		if f.failPath != "" && strings.Contains(r.URL.Path, f.failPath) {
			http.Error(w, "observation unavailable", 403)
			return
		}
		var obj any
		switch {
		case strings.Contains(r.URL.Path, "/persistentvolumeclaims/"):
			if f.pvc.Name == "" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"kind":"Status","reason":"NotFound","code":404}`)
				return
			}
			obj = f.pvc
		case strings.Contains(r.URL.Path, "/persistentvolumes/"):
			obj = f.pv
		case strings.HasPrefix(r.URL.Path, "/apis/storage.k8s.io/v1/storageclasses/"):
			obj = f.class
		case r.URL.Path == "/apis/storage.k8s.io/v1/storageclasses":
			obj = storagev1.StorageClassList{Items: []storagev1.StorageClass{f.class}}
		case r.URL.Path == "/apis/storage.k8s.io/v1/csidrivers/"+longhornCSIDriver:
			obj = storagev1.CSIDriver{ObjectMeta: metav1.ObjectMeta{Name: longhornCSIDriver}}
		case r.URL.Path == "/apis/storage.k8s.io/v1/csinodes":
			list := storagev1.CSINodeList{}
			for _, name := range f.drivers {
				list.Items = append(list.Items, storagev1.CSINode{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{Name: longhornCSIDriver, NodeID: name}}}})
			}
			obj = list
		case r.URL.Path == "/apis/storage.k8s.io/v1/csistoragecapacities":
			obj = f.capacities
		case r.URL.Path == "/apis/longhorn.io/v1beta2/nodes":
			items := []any{}
			for _, name := range f.ready {
				items = append(items, map[string]any{"metadata": map[string]string{"name": name}, "spec": map[string]any{"allowScheduling": false, "disks": map[string]any{}}, "status": map[string]any{"conditions": []map[string]string{{"type": "Ready", "status": "True"}}}})
			}
			obj = map[string]any{"items": items}
		case r.URL.Path == "/apis/longhorn.io/v1beta2/volumes":
			if r.URL.Query().Get("fieldSelector") != "metadata.name=volume-data" {
				t.Error("volume identity not scoped")
			}
			v := longhornPlacementVolume{}
			v.Metadata.Name = "volume-data"
			v.Status.CurrentNodeID = f.volumeNode
			v.Status.State = f.volumeState
			obj = map[string]any{"items": []longhornPlacementVolume{v}}
		case r.URL.Path == "/apis/storage.k8s.io/v1/volumeattachments":
			obj = f.attachments
		case strings.HasSuffix(r.URL.Path, "/pods"):
			items := []map[string]any{}
			for _, pod := range f.pods {
				data, _ := json.Marshal(pod)
				var raw map[string]any
				_ = json.Unmarshal(data, &raw)
				meta := raw["metadata"].(map[string]any)
				meta["uid"] = pod.ObservedUID
				meta["ownerReferences"] = pod.ObservedOwnerReferences
				items = append(items, raw)
			}
			obj = map[string]any{"items": items}
		case r.URL.Path == "/api/v1/nodes":
			obj = map[string]any{"items": []any{map[string]any{"metadata": map[string]string{"name": "compute-only"}}, map[string]any{"metadata": map[string]string{"name": "storage-ready"}}}}
		case strings.HasPrefix(r.URL.Path, "/api/v1/nodes/"):
			obj = storageFixtureNode(strings.TrimPrefix(r.URL.Path, "/api/v1/nodes/"))
		case strings.Contains(r.URL.Path, "/replicasets/"):
			obj = map[string]any{"metadata": map[string]any{"uid": "rs-uid", "ownerReferences": []map[string]any{{"apiVersion": "apps/v1", "kind": "Deployment", "uid": f.rsDeploymentUID, "controller": true}}}}
		case strings.Contains(r.URL.Path, "/endpointslices"):
			obj = map[string]any{"items": []any{}}
		case strings.Contains(r.URL.Path, "/endpoints/"):
			if f.readyEndpoint {
				obj = map[string]any{"subsets": []any{map[string]any{"addresses": []any{map[string]string{"ip": "192.0.2.1"}}}}}
			} else {
				obj = map[string]any{"subsets": []any{}}
			}
		case strings.Contains(r.URL.Path, "/deployments/"):
			if f.deployment != nil {
				obj = f.deployment
			} else {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"kind":"Status","reason":"NotFound","code":404}`)
				return
			}
		default:
			t.Errorf("unexpected read %s", r.URL)
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(obj)
	}))
	t.Cleanup(srv.Close)
	f.client = &kubeClient{baseURL: srv.URL, client: srv.Client()}
	return f
}

func storageFixtureNode(name string) kubeNode {
	var n kubeNode
	n.Metadata.Name = name
	n.Metadata.Labels = map[string]string{kubeHostnameLabelKey: name, runtime.SharedPoolLabelKey: runtime.SharedPoolLabelValue, "topology.kubernetes.io/zone": name}
	n.Status.Conditions = []kubeNodeCondition{{Type: "Ready", Status: "True"}}
	n.Status.Allocatable = map[string]string{"cpu": "8", "memory": "32Gi", "ephemeral-storage": "200Gi"}
	if name == "compute-only" {
		n.Status.Allocatable["memory"] = "64Gi"
	}
	return n
}

func TestStoragePlacementRejectsCSIOnlyNodeAndAllowsDisklessManager(t *testing.T) {
	f := newStoragePlacementFixture(t)
	plan, err := loadStoragePlacement(context.Background(), f.client, "tenant", []storageClaimRequest{{Name: "data", Class: "network-storage"}})
	if err != nil {
		t.Fatal(err)
	}
	if reason := plan.rejection(storageFixtureNode("compute-only")); !strings.Contains(reason, "Ready Longhorn") {
		t.Fatalf("CSI-only node accepted: %q", reason)
	}
	if reason := plan.rejection(storageFixtureNode("storage-ready")); reason != "" {
		t.Fatalf("diskless manager must support attachment: %s", reason)
	}
	// A pre-existing Pending pin is not evidence that the node can attach.
	app := managedAppLiveGuardTestApp(nil)
	var pod kubePod
	pod.Metadata.Name = runtime.RuntimeAppResourceName(app) + "-pending"
	pod.Spec.NodeName = "compute-only"
	pod.Status.Phase = "Pending"
	f.pods = []kubePod{pod}
	svc := &Service{newKubeClient: func(string) (*kubeClient, error) { return f.client, nil }}
	node, found, err := svc.selectManagedAppNode(context.Background(), app, runtime.SchedulingConstraints{NodeSelector: map[string]string{runtime.SharedPoolLabelKey: runtime.SharedPoolLabelValue}}, plan)
	if err != nil || !found || node != "storage-ready" {
		t.Fatalf("selected %q found=%t err=%v", node, found, err)
	}
	_, _, err = svc.selectManagedAppNode(context.Background(), app, schedulingPinnedToNode(runtime.SchedulingConstraints{}, "compute-only"), plan)
	if err == nil {
		t.Fatal("explicit incompatible pin was silently broadened")
	}
}

func TestStoragePlacementFailsClosedAndHonorsAttachmentOwnership(t *testing.T) {
	for _, scenario := range []string{"missing-manager", "missing-driver", "forbidden-pv", "forbidden-csi", "forbidden-volume", "foreign-claim", "attached", "attaching", "unknown-attachment", "detaching", "active-writer"} {
		t.Run(scenario, func(t *testing.T) {
			f := newStoragePlacementFixture(t)
			wantError := false
			switch scenario {
			case "missing-manager":
				f.ready = nil
			case "missing-driver":
				f.drivers = []string{"compute-only"}
			case "forbidden-pv":
				f.failPath = "persistentvolumes"
				wantError = true
			case "forbidden-csi":
				f.failPath = "csinodes"
				wantError = true
			case "forbidden-volume":
				f.failPath = "longhorn.io/v1beta2/volumes"
				wantError = true
			case "foreign-claim":
				f.pv.Spec.ClaimRef.UID = "other-claim"
				wantError = true
			case "attached":
				f.volumeNode = "compute-only"
				f.volumeState = "attached"
			case "detaching":
				pv := "pv-data"
				now := metav1.Now()
				f.attachments.Items = []storagev1.VolumeAttachment{{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now}, Spec: storagev1.VolumeAttachmentSpec{NodeName: "compute-only", Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv}}}}
			case "attaching":
				pv := "pv-data"
				f.attachments.Items = []storagev1.VolumeAttachment{{Spec: storagev1.VolumeAttachmentSpec{NodeName: "compute-only", Source: storagev1.VolumeAttachmentSource{PersistentVolumeName: &pv}}}}
			case "unknown-attachment":
				f.volumeState = "attaching"
				wantError = true
			case "active-writer":
				var pod kubePod
				pod.Spec.NodeName = "compute-only"
				pod.Status.Phase = "Running"
				pod.Spec.Volumes = []kubePodVolume{{PersistentVolumeClaim: &kubePersistentVolumeRef{ClaimName: "data"}}}
				f.pods = []kubePod{pod}
			}
			plan, err := loadStoragePlacement(context.Background(), f.client, "tenant", []storageClaimRequest{{Name: "data", Class: "network-storage"}})
			if wantError {
				if err == nil {
					t.Fatal("missing evidence accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if reason := plan.rejection(storageFixtureNode("storage-ready")); reason == "" {
				t.Fatal("unsafe attachment accepted")
			}
		})
	}
}

func TestStoragePlacementIntersectsAllPVTermsAndClaims(t *testing.T) {
	n := storageFixtureNode("storage-ready")
	makeClaim := func(name string, terms ...corev1.NodeSelectorTerm) storagePlacementClaim {
		return storagePlacementClaim{name: name, pv: &corev1.PersistentVolume{Spec: corev1.PersistentVolumeSpec{NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: terms}}}}}
	}
	term := func(key, value string) corev1.NodeSelectorTerm {
		return corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: key, Operator: corev1.NodeSelectorOpIn, Values: []string{value}}}}
	}
	p := &storagePlacement{claims: []storagePlacementClaim{makeClaim("one", term(kubeHostnameLabelKey, "compute-only"), term(kubeHostnameLabelKey, "storage-ready")), makeClaim("two", term("topology.kubernetes.io/zone", "compute-only"))}}
	if p.rejection(n) == "" {
		t.Fatal("two incompatible claims were treated as alternatives")
	}
	p.claims = p.claims[:1]
	if p.rejection(n) != "" {
		t.Fatal("OR nodeSelectorTerms were interpreted as AND")
	}
	p.claims[0] = makeClaim("one", corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: kubeHostnameLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{"storage-ready"}}, {Key: "topology.kubernetes.io/zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"compute-only"}}}})
	if p.rejection(n) == "" {
		t.Fatal("AND expressions were not all enforced")
	}
	p.claims[0] = makeClaim("one", corev1.NodeSelectorTerm{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"compute-only"}}}})
	if p.rejection(n) == "" {
		t.Fatal("metadata.name node field ignored")
	}
}

func TestStoragePlacementNewPVCRespectsDelayedBindingAndTopology(t *testing.T) {
	f := newStoragePlacementFixture(t)
	f.pvc = corev1.PersistentVolumeClaim{}
	mode := storagev1.VolumeBindingWaitForFirstConsumer
	f.class.VolumeBindingMode = &mode
	f.class.AllowedTopologies = []corev1.TopologySelectorTerm{{MatchLabelExpressions: []corev1.TopologySelectorLabelRequirement{{Key: kubeHostnameLabelKey, Values: []string{"compute-only"}}}}}
	plan, err := loadStoragePlacement(context.Background(), f.client, "tenant", []storageClaimRequest{{Name: "new-data", Class: "network-storage"}})
	if err != nil {
		t.Fatal(err)
	}
	if plan.rejection(storageFixtureNode("storage-ready")) == "" {
		t.Fatal("allowedTopologies ignored")
	}
	if f.reads["/apis/storage.k8s.io/v1/volumeattachments"] > 0 {
		t.Fatal("new delayed-binding PVC incorrectly required an existing attachment")
	}
}

func TestStoragePlacementBoundNonCSIVolumeDoesNotRequireCSINode(t *testing.T) {
	f := newStoragePlacementFixture(t)
	f.class.Provisioner = "example.test/nfs"
	f.pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
	f.pv.Spec.CSI = nil
	f.pv.Spec.NFS = &corev1.NFSVolumeSource{Server: "nfs.example.test", Path: "/exports"}
	f.failPath = "/apis/storage.k8s.io/v1/csi"
	plan, err := loadStoragePlacement(context.Background(), f.client, "tenant", []storageClaimRequest{{Name: "data", Class: "network-storage"}})
	if err != nil {
		t.Fatal(err)
	}
	if reason := plan.rejection(storageFixtureNode("compute-only")); reason != "" {
		t.Fatalf("non-CSI shared volume incorrectly requires a CSI attachment node: %s", reason)
	}
}

func TestStoragePlacementRechecksBeforeWritingAndDoesNotBlockStop(t *testing.T) {
	f := newStoragePlacementFixture(t)
	svc := &Service{newKubeClient: func(string) (*kubeClient, error) { return f.client, nil }}
	app := managedAppLiveGuardTestApp(&model.AppPersistentStorageSpec{Mode: model.AppPersistentStorageModeDedicatedPVC, StorageClassName: "network-storage", StorageSize: "10Gi", Mounts: []model.AppPersistentStorageMount{{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"}}})
	f.pvc.Spec.VolumeName = ""
	f.pvc.Spec.StorageClassName = &f.class.Name // Newly provisioned shape for the rendered claim name.
	scheduling := schedulingPinnedToNode(runtime.SchedulingConstraints{}, "storage-ready")
	objects := svc.Renderer.BuildManagedAppChildObjects(app, scheduling, nil)
	if err := svc.validateAppStoragePlacement(context.Background(), f.client, app, scheduling, objects); err != nil {
		t.Fatal(err)
	}
	f.ready = nil
	if err := svc.validateAppStoragePlacement(context.Background(), f.client, app, scheduling, objects); err == nil {
		t.Fatal("stale placement accepted after storage node disappeared")
	}
	f.failPath = "/"
	app.Spec.Replicas = 0
	if err := svc.validateAppStoragePlacement(context.Background(), f.client, app, scheduling, objects); err != nil {
		t.Fatalf("stop depends on storage health: %v", err)
	}
}

func TestManagedStoragePlacementValidatesOwnedRuntimeAndPreservesServingNode(t *testing.T) {
	f := newStoragePlacementFixture(t)
	state := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := state.CreateTenant("storage tenant")
	if err != nil {
		t.Fatal(err)
	}
	rt, _, err := state.CreateRuntime(tenant.ID, "storage runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	app := managedAppLiveGuardTestApp(&model.AppPersistentStorageSpec{Mode: model.AppPersistentStorageModeDedicatedPVC, StorageClassName: "network-storage", StorageSize: "10Gi", Mounts: []model.AppPersistentStorageMount{{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/data"}}})
	app.Spec.RuntimeID = rt.ID
	app.TenantID = tenant.ID
	f.pvc.Spec.VolumeName = ""
	svc := &Service{Store: state, newKubeClient: func(string) (*kubeClient, error) { return f.client, nil }}
	if _, err := svc.managedSchedulingConstraintsForApp(context.Background(), app); err == nil {
		t.Fatal("owned runtime bypassed storage eligibility")
	}
	app.Spec.RuntimeID = model.DefaultManagedRuntimeID
	scheduling, err := svc.managedSchedulingConstraintsForApp(context.Background(), app)
	if err != nil {
		t.Fatal(err)
	}
	if scheduling.NodeSelector[kubeHostnameLabelKey] != "storage-ready" {
		t.Fatal(scheduling)
	}
	var pod kubePod
	pod.Metadata.Name = runtime.RuntimeAppResourceName(app) + "-ready"
	pod.Spec.NodeName = "compute-only"
	pod.Status.Phase = "Running"
	pod.Status.Conditions = []kubePodCondition{{Type: "Ready", Status: "True"}}
	f.pods = []kubePod{pod}
	if _, err := svc.managedSchedulingConstraintsForApp(context.Background(), app); err == nil || !strings.Contains(err.Error(), "preserving serving app") {
		t.Fatalf("serving app was silently moved: %v", err)
	}
	app.Spec.Replicas = 0
	f.failPath = "/"
	if _, err := svc.managedSchedulingConstraintsForApp(context.Background(), app); err != nil {
		t.Fatalf("stop cannot need eligible storage: %v", err)
	}
}

func TestStoragePlacementErrorsNameTheClaimAndNode(t *testing.T) {
	p := &storagePlacement{claims: []storagePlacementClaim{{name: "workspace", driver: longhornCSIDriver}}}
	reason := p.rejection(storageFixtureNode("worker"))
	if !strings.Contains(reason, "workspace") || !strings.Contains(reason, longhornCSIDriver) {
		t.Fatal(fmt.Sprintf("unhelpful rejection %q", reason))
	}
}

func TestStoragePlacementRejectsInsufficientCSIStorageCapacity(t *testing.T) {
	selector := &metav1.LabelSelector{MatchLabels: map[string]string{kubeHostnameLabelKey: "storage-ready"}}
	p := &storagePlacement{
		claims:     []storagePlacementClaim{{name: "data", requestedBytes: 10 * 1024 * 1024 * 1024, class: storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "network-storage"}}, singleNode: true, attachedNodes: map[string]bool{}}},
		capacities: []storagev1.CSIStorageCapacity{{StorageClassName: "network-storage", Capacity: resource.NewQuantity(5*1024*1024*1024, resource.BinarySI), NodeTopology: selector}},
	}
	n := storageFixtureNode("storage-ready")
	if reason := p.rejection(n); !strings.Contains(reason, "CSIStorageCapacity") {
		t.Fatalf("expected capacity rejection, got %q", reason)
	}
}

func TestStoragePlacementLoadsRequestedSizeAndCapacityTopology(t *testing.T) {
	f := newStoragePlacementFixture(t)
	f.pvc.Spec.VolumeName = ""
	f.capacities.Items = []storagev1.CSIStorageCapacity{{StorageClassName: f.class.Name, Capacity: resource.NewQuantity(5<<30, resource.BinarySI), NodeTopology: &metav1.LabelSelector{MatchLabels: map[string]string{kubeHostnameLabelKey: "storage-ready"}}}}
	plan, err := loadStoragePlacement(context.Background(), f.client, "tenant", []storageClaimRequest{{Name: "data", Class: f.class.Name, RequestedBytes: 10 << 30}})
	if err != nil {
		t.Fatal(err)
	}
	if reason := plan.rejection(storageFixtureNode("storage-ready")); !strings.Contains(reason, "CSIStorageCapacity") {
		t.Fatalf("oversized claim accepted: %s", reason)
	}
	// Separate pools in the same topology are alternatives, not one shared pool.
	enough := f.capacities.Items[0]
	enough.Capacity = resource.NewQuantity(20<<30, resource.BinarySI)
	plan.capacities = append(plan.capacities, enough)
	if reason := plan.rejection(storageFixtureNode("storage-ready")); reason != "" {
		t.Fatalf("available pool rejected: %s", reason)
	}
	plan.capacities = plan.capacities[1:]
	plan.capacities[0].MaximumVolumeSize = resource.NewQuantity(1<<30, resource.BinarySI)
	if reason := plan.rejection(storageFixtureNode("storage-ready")); !strings.Contains(reason, "CSIStorageCapacity") {
		t.Fatalf("maximum volume size ignored: %s", reason)
	}
	plan.capacities = nil
	if reason := plan.rejection(storageFixtureNode("storage-ready")); reason != "" {
		t.Fatalf("unknown capacity treated as zero: %s", reason)
	}
	plan.claims[0].pv = &f.pv
	plan.capacities = f.capacities.Items
	if reason := plan.rejection(storageFixtureNode("storage-ready")); reason != "" {
		t.Fatalf("existing allocation charged again: %s", reason)
	}
}

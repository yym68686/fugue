package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseproof"
	"fugue/internal/runtime"
)

func TestManagedRolloutCapacityDomainKeyIgnoresMapAndTolerationOrder(t *testing.T) {
	t.Parallel()

	left := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{"node": "ns101351", "pool": "shared"},
		Tolerations: []runtime.Toleration{
			{Key: "dedicated", Operator: "Equal", Value: "apps", Effect: "NoSchedule"},
			{Key: "maintenance", Operator: "Exists", Effect: "NoExecute"},
		},
	}
	right := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{"pool": "shared", "node": "ns101351"},
		Tolerations: []runtime.Toleration{
			{Key: "maintenance", Operator: "Exists", Effect: "NoExecute"},
			{Key: "dedicated", Operator: "Equal", Value: "apps", Effect: "NoSchedule"},
		},
	}
	if got, want := managedRolloutCapacityDomainKey(left), managedRolloutCapacityDomainKey(right); got != want {
		t.Fatalf("equivalent scheduling constraints produced different capacity domains: %q != %q", got, want)
	}
}

func TestManagedRolloutCapacityDomainLockSerializesSameDomain(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	scheduling := runtime.SchedulingConstraints{NodeSelector: map[string]string{"kubernetes.io/hostname": "ns101351"}}
	unlockFirst := svc.lockManagedRolloutCapacityDomain(scheduling)
	acquired := make(chan func(), 1)
	go func() {
		acquired <- svc.lockManagedRolloutCapacityDomain(scheduling)
	}()

	select {
	case unlockSecond := <-acquired:
		unlockSecond()
		t.Fatal("second rollout acquired the same capacity domain concurrently")
	case <-time.After(25 * time.Millisecond):
	}
	unlockFirst()
	unlockSecond := <-acquired
	unlockSecond()
}

func TestManagedRolloutCapacityDomainLockCancellationDoesNotStealSlot(t *testing.T) {
	svc := &Service{}
	scheduling := runtime.SchedulingConstraints{}
	first := svc.lockManagedRolloutCapacityDomain(scheduling)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if unlock, err := svc.lockManagedRolloutCapacityDomainContext(ctx, scheduling, "second"); unlock != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled waiter acquired slot: %v", err)
	}
	first()
	first()
	if unlock, err := svc.lockManagedRolloutCapacityDomainContext(context.Background(), scheduling, "third"); err != nil {
		t.Fatal(err)
	} else {
		unlock()
	}
}

func TestManagedRolloutHandsOffOnlyAccountedSurge(t *testing.T) {
	for _, scenario := range []string{"capacity available", "drain consumes capacity", "unbound", "pending", "foreign owner", "incomplete namespace list", "incomplete cluster list", "missing cluster pod", "stale deployment", "duplicate current pod"} {
		t.Run(scenario, func(t *testing.T) {
			app, _ := trafficReadinessFixture(t)
			app.Spec.Resources = &model.ResourceSpec{MemoryMebibytes: 512}
			namespace := runtime.NamespaceForTenant(app.TenantID)
			dep, found := expectedManagedAppDeployment(app, runtime.SchedulingConstraints{})
			if !found {
				t.Fatal("expected deployment")
			}
			dep.Metadata.UID, dep.Metadata.ResourceVersion, dep.Metadata.Generation = "dep", "10", 2
			dep.Status.ObservedGeneration, dep.Status.Replicas, dep.Status.UpdatedReplicas = 2, 1, 1
			dep.Status.AvailableReplicas, dep.Status.ReadyReplicas = 1, 1
			pod := readyTemplatePod("current", dep, kubeResourceRequirements{})
			pod.Spec.Containers = dep.Spec.Template.Spec.Containers
			pod.Spec.InitContainers = dep.Spec.Template.Spec.InitContainers
			pod.Spec.NodeName, pod.ObservedUID = "node-a", "new-pod"
			pod.ObservedOwnerReferences = []kubePodOwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "new-rs", UID: "rs", Controller: true}}
			old := pod
			old.Metadata.Name, old.Metadata.DeletionTimestamp, old.ObservedUID = "old", "2026-01-01T00:00:00Z", "old-pod"
			var rs releaseproof.ReplicaSet
			yes := true
			rs.Metadata.Name, rs.Metadata.Namespace, rs.Metadata.UID = "new-rs", namespace, "rs"
			rs.Metadata.OwnerReferences = []releaseproof.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: dep.Metadata.Name, UID: dep.Metadata.UID, Controller: &yes}}
			rs.Spec.Template.Metadata.Annotations.ReleaseKey = deploymentReleaseKey(dep)
			if scenario == "foreign owner" {
				rs.Metadata.OwnerReferences[0].UID = "different-deployment"
			}
			if scenario == "unbound" {
				pod.Spec.NodeName = ""
			}
			if scenario == "pending" {
				pod.Status.Phase = "Pending"
			}
			confirmed := dep
			if scenario == "stale deployment" {
				confirmed.Metadata.ResourceVersion = "11"
			}
			snapshot := kubePodList{Items: []kubePod{pod}}
			snapshot.Metadata.ResourceVersion = "20"
			if scenario == "incomplete namespace list" {
				snapshot.Metadata.Continue = "page2"
			}
			if scenario == "duplicate current pod" {
				snapshot.Items = append(snapshot.Items, pod)
			}
			inventory := []map[string]any{}
			for _, p := range []kubePod{pod, old} {
				if scenario == "missing cluster pod" && p.ObservedUID == pod.ObservedUID {
					continue
				}
				raw, _ := json.Marshal(p)
				var entry map[string]any
				if err := json.Unmarshal(raw, &entry); err != nil {
					t.Fatal(err)
				}
				entry["metadata"].(map[string]any)["uid"] = p.ObservedUID
				inventory = append(inventory, entry)
			}
			meta := map[string]string{"resourceVersion": "21"}
			if scenario == "incomplete cluster list" {
				meta["continue"] = "page2"
			}
			node := kubeNode{}
			node.Metadata.Name = "node-a"
			node.Status.Conditions = []kubeNodeCondition{{Type: "Ready", Status: "True"}}
			node.Status.Allocatable = map[string]string{"memory": "2Gi"}
			if scenario == "drain consumes capacity" {
				node.Status.Allocatable["memory"] = "1Gi"
			}
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("capacity proof attempted mutation")
				}
				var out any
				switch r.URL.Path {
				case "/api/v1/pods":
					out = map[string]any{"metadata": meta, "items": inventory}
				case "/api/v1/nodes":
					out = kubeNodeList{Items: []kubeNode{node}}
				case "/api/v1/nodes/node-a":
					out = node
				case "/apis/apps/v1/namespaces/" + namespace + "/replicasets/new-rs":
					out = rs
				case deploymentAPIPath(namespace, dep.Metadata.Name):
					out = confirmed
				default:
					http.NotFound(w, r)
					return
				}
				_ = json.NewEncoder(w).Encode(out)
			}))
			defer kube.Close()
			client := &kubeClient{client: kube.Client(), baseURL: kube.URL}
			svc := &Service{}
			scheduling := runtime.SchedulingConstraints{}
			first := svc.lockManagedRolloutCapacityDomain(scheduling)
			defer first()
			releases := 0
			handoff := rolloutCapacityHandoff{release: func(reason string) {
				if reason != "surge_accounted" {
					t.Errorf("unexpected release reason %q", reason)
				}
				releases++
				first()
			}}
			handoff.try(context.Background(), client, namespace, dep, snapshot)
			handoff.try(context.Background(), client, namespace, dep, snapshot)
			want := scenario == "capacity available" || scenario == "drain consumes capacity"
			if (releases == 1) != want {
				t.Fatalf("handoff count=%d, want handoff=%t", releases, want)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			unlock, err := svc.lockManagedRolloutCapacityDomainContext(ctx, scheduling, "second")
			cancel()
			if !want {
				if unlock != nil || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("unsafe concurrent admission: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer unlock()
			message, err := zeroDowntimeRolloutCapacityBlockMessage(context.Background(), client, dep, 1)
			if err != nil || (strings.Contains(message, "no eligible node has capacity") != (scenario == "drain consumes capacity")) {
				t.Fatalf("second rollout preflight: %q (%v)", message, err)
			}
		})
	}
}

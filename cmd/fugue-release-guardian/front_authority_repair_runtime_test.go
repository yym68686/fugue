package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/frontrepairstate"
	"fugue/internal/model/edgecohort"
	"fugue/internal/releaseguardian"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stest "k8s.io/client-go/testing"
)

type repairRuntimeExecFixture func(context.Context, string, string, string, ...string) ([]byte, error)

func (exec repairRuntimeExecFixture) Exec(ctx context.Context, namespace, pod, container string, command ...string) ([]byte, error) {
	return exec(ctx, namespace, pod, container, command...)
}

func newFrontRepairRuntimeFixture(t *testing.T) (frontRepairRuntime, *fake.Clientset, releaseguardian.CurrentAuthority, []frontRepairNode,
	map[string]frontRepairWitness, map[string]frontRepairWitness) {
	t.Helper()
	const namespace = "fixture-system"
	before, cohort, fixture := partialFrontRollbackFixture()
	var objects []runtime.Object
	var podsByName = map[string]frontRepairWitness{}
	var endpoints = map[string]frontRepairWitness{}
	container := func(name string, worker bool) corev1.Container {
		c := corev1.Container{Name: name, Image: "example.test/worker@" + before.CurrentWorkerImageDigest}
		if name != "caddy" {
			c.VolumeMounts = []corev1.VolumeMount{{Name: "activation", MountPath: path.Dir(frontActivationStatePath), ReadOnly: !worker}}
		}
		if name == "edge-front" {
			c.Ports = []corev1.ContainerPort{{HostPort: 443}}
		}
		return c
	}
	for i, row := range fixture {
		identity := row.Node
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: identity.Name, UID: types.UID(identity.UID), ResourceVersion: "2",
			Labels: map[string]string{"fugue.io/edge-group-id": before.GroupID, "fugue.io/role.edge": "true", "fugue.io/edge-workload": "dynamic"}},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
		if identity.Admitted {
			node.Labels[edgecohort.PhaseLabel] = edgecohort.Admitted
		}
		newPod := func(role, uid, ip string, containers ...corev1.Container) *corev1.Pod {
			owner := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: identity.Name + "-" + role, UID: types.UID(uid),
				Labels: map[string]string{"fugue.io/edge-group-id": before.GroupID}, Annotations: map[string]string{"fugue.pro/source-commit": before.CurrentWorkerSourceSHA}},
				Spec: corev1.PodSpec{NodeName: identity.Name, Containers: containers, Volumes: []corev1.Volume{{Name: "activation", VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{Path: "/var/lib/fixture-activation"}}}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
			ids := map[string]string{"edge-front": row.FrontProcessID, "edge": row.TargetWorkerProcessID, "caddy": row.TargetCaddyProcessID}
			for _, c := range containers {
				owner.Status.ContainerStatuses = append(owner.Status.ContainerStatuses, corev1.ContainerStatus{Name: c.Name, ImageID: c.Image, ContainerID: ids[c.Name], Ready: true,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}})
			}
			return owner
		}
		front := newPod("front", row.FrontPodUID, fmt.Sprintf("192.0.2.%d", 10+i), container("edge-front", false))
		worker := newPod("worker", row.TargetWorkerPodUID, fmt.Sprintf("192.0.2.%d", 20+i), container("edge", true), container("caddy", true))
		worker.Labels["fugue.io/edge-slot"] = string(before.CurrentWorkerSlot)
		podsByName[worker.Name] = row
		endpoints[repairHealthEndpoint(front.Status.PodIP, frontHealthPort, "/readyz")] = row
		endpoints[repairHealthEndpoint(worker.Status.PodIP, workerHealthPort, "/healthz")] = row
		objects = append(objects, node, front, worker)
	}
	client := fake.NewSimpleClientset(objects...)
	activator, err := newFrontAuthorityActivator(client, repairRuntimeExecFixture(func(_ context.Context, ns, name, container string, args ...string) ([]byte, error) {
		row, exists := podsByName[name]
		if ns != namespace || !exists || container != "edge" || strings.Join(args, " ") != "cat "+frontActivationStatePath {
			return nil, fmt.Errorf("unexpected or mutating exec for %s", name)
		}
		return json.Marshal(row.Activation)
	}), frontAuthorityConfig{Namespace: namespace, GroupID: before.GroupID, ExpectedNodes: 2}, "fixture-holder")
	if err != nil {
		t.Fatal(err)
	}
	reader := newFrontRepairRuntime(activator)
	reader.read = func(_ context.Context, endpoint string, value any) error {
		row, exists := endpoints[endpoint]
		if !exists {
			return fmt.Errorf("unexpected probe %s", endpoint)
		}
		switch result := value.(type) {
		case *observedFront:
			*result = row.LiveFront
		case *authorityWorkerHealth:
			*result = row.TargetWorkerHealth
		default:
			return fmt.Errorf("unexpected health destination %T", value)
		}
		return nil
	}
	return reader, client, before, cohort, podsByName, endpoints
}

func TestFrontRepairRuntimeBindsRealCohortAndRejectsSamePodProcessReplacement(t *testing.T) {
	ctx := context.Background()
	reader, client, before, cohort, _, _ := newFrontRepairRuntimeFixture(t)
	namespace := reader.activator.config.Namespace
	if _, err := reader.observe(ctx, before, cohort[1:]); err == nil {
		t.Fatal("allowed enrollment subset excluding the admitted member")
	}
	result, err := reader.observe(ctx, before, cohort)
	if err != nil || len(result) != len(cohort) {
		t.Fatalf("could not read the whole serving cohort: %v", err)
	}
	witnesses := make([]frontRepairWitness, 0, len(result))
	for _, member := range result {
		witnesses = append(witnesses, member.frontRepairWitness)
	}
	if _, err := planPartialFrontRollbackRepair(before, "pointer-uid", "101", cohort, witnesses); err != nil {
		t.Fatalf("trusted reads do not bind an exact partial rollback: %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("observer performed a mutating Kubernetes action: %v", action)
		}
	}
	kept := result[0]
	pod := kept.worker.DeepCopy()
	pod.Status.ContainerStatuses[1].ContainerID = "replacement-caddy-process"
	if _, err := client.CoreV1().Pods(namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := reader.requireSameMember(ctx, kept); err == nil {
		t.Fatal("same Pod UID with changed Caddy process was accepted")
	}
}

func TestFrontRepairHealthReadDoesNotFollowRedirects(t *testing.T) {
	read := newFrontRepairRuntime(nil).read
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/healthz", http.StatusFound)
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok","edge_group_id":"fixture","stale_cache":false,"another_worker_health_field":1337}`))
	}))
	defer server.Close()
	var result authorityWorkerHealth
	if err := read(context.Background(), server.URL+"/healthz", &result); err != nil || result.StaleCache == nil || *result.StaleCache {
		t.Fatalf("current Worker health schema was not readable: %v", err)
	}
	if err := read(context.Background(), server.URL+"/redirect", &result); err == nil {
		t.Fatal("health proof followed an untrusted redirect")
	}
}

func TestExecuteReviewedFrontRepairUsesDiskAndDoesNotReplay(t *testing.T) {
	ctx := context.Background()
	reader, client, before, cohort, workers, endpoints := newFrontRepairRuntimeFixture(t)
	ns, group := reader.activator.config.Namespace, before.GroupID
	release, component, authority := frontRepairLeaseFixtures(ns, group)
	leases := client.CoordinationV1().Leases(ns)
	for _, object := range []*coordinationv1.Lease{component, authority} {
		if _, err := leases.Create(ctx, object, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	trackFrontRepairLeaseCAS(t, client, ns)
	fenceName := releaseguardian.FrontRepairFenceName(group)
	pointerName := "fugue-current-authority-" + group
	cms := client.CoreV1().ConfigMaps(ns)
	raw, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cms.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: pointerName, UID: "pointer-uid", ResourceVersion: "101",
		Labels: map[string]string{"fugue.pro/group": group, "fugue.pro/authority-store": "true"}}, Data: map[string]string{"authority.json": string(raw)}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("create", "configmaps", func(action k8stest.Action) (bool, runtime.Object, error) {
		cm := action.(k8stest.CreateAction).GetObject().(*corev1.ConfigMap)
		if cm.Name == fenceName {
			cm.UID, cm.ResourceVersion = "repair-fence-uid", "41"
		}
		return false, nil, nil
	})
	paths := make(map[string]string)
	for _, worker := range workers {
		file := filepath.Join(t.TempDir(), "activation.json")
		raw, err := json.Marshal(worker.Activation)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, raw, 0600); err != nil {
			t.Fatal(err)
		}
		paths[worker.Node.Name] = file
	}
	var commands []string
	var preparations []frontrepairstate.PublicationPreparation
	simulateStoppedNode := true
	reader.activator.executor = repairRuntimeExecFixture(func(_ context.Context, namespace, pod, container string, args ...string) ([]byte, error) {
		worker, found := workers[pod]
		if namespace != ns || container != "edge" || !found {
			t.Fatalf("unexpected exec target %s/%s/%s", namespace, pod, container)
		}
		file := paths[worker.Node.Name]
		switch {
		case slices.Equal(args, []string{"cat", frontActivationStatePath}):
			return os.ReadFile(file)
		case slices.Equal(args, []string{"date", "-u", "+%s"}):
			return []byte(strconv.FormatInt(time.Now().Unix(), 10) + "\n"), nil
		case len(args) == 5 && slices.Equal(args[:3], []string{frontActivationCASBinary, "--state-file", frontActivationStatePath}):
			decoded, err := base64.RawURLEncoding.Strict().DecodeString(args[4])
			var write frontrepairstate.ActivationRepairWrite
			if err != nil || decodeStrictJSON(decoded, &write) != nil || write.Schema != frontrepairstate.ActivationRepairWriteSchemaV1 ||
				!time.Now().Before(write.ExpiresAt) || time.Until(write.ExpiresAt) > frontRepairMutationTimeout {
				t.Fatalf("unbounded or ambiguous repair command: %q, err=%v", decoded, err)
			}
			commands = append(commands, args[3]+":"+worker.Node.Name)
			if args[3] == "--repair-prepare-request" {
				p, err := frontrepairstate.PrepareActivationRepairPublication(file, write.Request, write.ExpiresAt)
				if err != nil {
					t.Fatal(err)
				}
				preparations = append(preparations, p)
				return json.Marshal(p)
			}
			if args[3] != "--repair-request" {
				t.Fatalf("unexpected mutation flag %q", args[3])
			}
			if simulateStoppedNode && worker.Node.Name == "edge-b" {
				return []byte("unknown result without local write"), nil
			}
			receipt, err := frontrepairstate.ApplyActivationRepairCAS(file, write.Request, write.ExpiresAt)
			if err != nil {
				t.Fatal(err)
			}
			if worker.Node.Name == "edge-a" {
				return []byte("missing activation receipt"), nil
			}
			return json.Marshal(receipt)
		default:
			t.Fatalf("unknown repair command: %q", args)
			return nil, nil
		}
	})
	reader.read = func(_ context.Context, endpoint string, result any) error {
		row, ok := endpoints[endpoint]
		if !ok {
			t.Fatalf("unknown probe %q", endpoint)
		}
		file := paths[row.Node.Name]
		disk, found, err := edgegroupfront.ReadActivationState(file)
		if err != nil || !found {
			return fmt.Errorf("fixture disk unavailable: %v", err)
		}
		switch value := result.(type) {
		case *observedFront:
			*value = liveFrontRepairFixture(disk)
		case *authorityWorkerHealth:
			*value = row.TargetWorkerHealth
			if disk.ActiveSlot != string(before.CurrentWorkerSlot) {
				p, exists, err := edgegroupfront.ReadPublicationPreparation(file)
				if err != nil {
					return err
				}
				if !exists || !frontRepairPreparationReady(p, disk, before, time.Now().UTC()) {
					value.Status = "degraded"
				}
			}
		default:
			t.Fatalf("unexpected read destination %T", result)
		}
		return nil
	}
	witnesses, err := reader.observe(ctx, before, cohort)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planPartialFrontRollbackRepair(before, "pointer-uid", "101", cohort, frontRepairWitnesses(witnesses))
	if err != nil {
		t.Fatal(err)
	}
	intent := frontRepairReviewedIntent{Group: group, PlanDigest: plan.Digest, AuthorizationDigest: "sha256:" + strings.Repeat("a", 64), Nodes: cohort}
	run := func() (frontRepairOpened, error) { return reader.runReviewed(ctx, release, intent) }
	first, err := run()
	if err == nil || first.completed || !strings.Contains(err.Error(), "disk activation did not advance exactly") ||
		!slices.Equal(commands, []string{"--repair-prepare-request:edge-c", "--repair-request:edge-a", "--repair-request:edge-b"}) {
		t.Fatalf("unexpected interrupted repair: commands=%v result=%+v err=%v", commands, first, err)
	}
	if _, err := cms.Get(ctx, fenceName, metav1.GetOptions{}); err != nil {
		t.Fatalf("disappeared partial repair fence: %v", err)
	}
	store, err := releaseguardian.NewAuthorityStore(client, ns)
	if err != nil {
		t.Fatal(err)
	}
	if current, _, _, err := store.LoadCurrent(ctx, group); current != before || err != nil {
		t.Fatalf("pointer advanced before live convergence: %+v err=%v", current, err)
	}
	simulateStoppedNode = false
	commands = nil
	applied, err := run()
	if err != nil || !applied.completed || applied.journal.Digest != first.journal.Digest ||
		!slices.Equal(commands, []string{"--repair-prepare-request:edge-c", "--repair-request:edge-b", "--repair-request:edge-c"}) ||
		len(preparations) != 2 || preparations[0] != preparations[1] {
		t.Fatalf("explicit resume replayed a completed write or renewed preparation: commands=%v result=%+v err=%v", commands, applied, err)
	}
	if _, err := cms.Get(ctx, fenceName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("converged repair retained live fence: %v", err)
	}
	if current, _, _, err := store.LoadCurrent(ctx, group); current != applied.journal.committedAuthority() || err != nil {
		t.Fatalf("group pointer did not converge: %+v err=%v", current, err)
	}
	commands = nil
	completed, err := run()
	if err != nil || !completed.completed || completed.journal.Digest != first.journal.Digest || len(commands) != 0 {
		t.Fatalf("completed approval dispatched more commands: %v err=%v", commands, err)
	}
}

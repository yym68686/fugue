package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestCandidateAdmissionRequiresOtherRevisionToReleaseItsSlot(t *testing.T) {
	for _, scenario := range []string{"running", "pending", "terminating", "zero_deployment_live_rs", "zero_controllers_live_pod", "zero_unobserved", "default_replicas", "cold_history", "absent_history", "terminal_pod", "stable_with_candidate_label", "previous_with_candidate_label", "same_operation", "orphan_revision", "orphan_pod", "drifted_child_labels", "other_app", "other_namespace", "retired_residue", "legacy_canonical_alias"} {
		t.Run(scenario, func(t *testing.T) {
			app := model.App{ID: "app-example", TenantID: "tenant-example"}
			r := model.AppRelease{ID: "old-release", AppID: app.ID, TenantID: app.TenantID, Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusFailed, DeploymentName: "old-workload"}
			zero, one := 0, 1
			metadata := func(name, uid string) metav1.ObjectMeta {
				return metav1.ObjectMeta{Name: name, Namespace: runtime.NamespaceForTenant(app.TenantID), UID: types.UID(uid), Generation: 1, Labels: map[string]string{
					runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID, runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue,
					runtime.FugueLabelAppReleaseID: r.ID, runtime.FugueLabelAppReleaseRole: runtime.AppRevisionRoleCandidate,
				}}
			}
			owned := func(kind, uid string) []metav1.OwnerReference {
				controller := true
				return []metav1.OwnerReference{{Kind: kind, Name: "owner", UID: types.UID(uid), Controller: &controller}}
			}
			d := candidateAdmissionObject{Metadata: metadata("old-workload", "dep-uid")}
			d.Spec.Replicas, d.Status.ObservedGeneration = &zero, 1
			rs := candidateAdmissionObject{Metadata: metadata("old-rs", "rs-uid")}
			rs.Metadata.OwnerReferences, rs.Spec.Replicas, rs.Status.ObservedGeneration = owned("Deployment", "dep-uid"), &zero, 1
			pod := candidateAdmissionObject{Metadata: metadata("old-pod", "pod-uid")}
			pod.Metadata.OwnerReferences, pod.Status.Phase = owned("ReplicaSet", "rs-uid"), "Running"
			current, wantBlock := "new-release", true
			deps, sets, pods := []candidateAdmissionObject{d}, []candidateAdmissionObject{rs}, []candidateAdmissionObject{pod}
			releases := []model.AppRelease{r}
			switch scenario {
			case "running":
				deps[0].Spec.Replicas = &one
			case "pending":
				pods[0].Status.Phase = "Pending"
			case "terminating":
				now := metav1.Now()
				pods[0].Metadata.DeletionTimestamp = &now
			case "zero_deployment_live_rs":
				sets[0].Spec.Replicas, pods = &one, nil
			case "zero_unobserved":
				deps[0].Metadata.Generation, pods = 2, nil
			case "default_replicas":
				deps[0].Spec.Replicas, pods = nil, nil
			case "cold_history":
				pods, wantBlock = nil, false
			case "absent_history":
				deps, sets, pods, wantBlock = nil, nil, nil, false
			case "terminal_pod":
				pods[0].Status.Phase, wantBlock = "Failed", false
			case "stable_with_candidate_label":
				releases[0].Role, releases[0].Status, deps[0].Spec.Replicas, wantBlock = model.AppReleaseRoleStable, model.AppReleaseStatusServing, &one, false
			case "previous_with_candidate_label":
				releases[0].Role, releases[0].Status, deps[0].Spec.Replicas, wantBlock = model.AppReleaseRolePrevious, model.AppReleaseStatusDraining, &one, false
			case "same_operation":
				current, deps[0].Spec.Replicas, wantBlock = r.ID, &one, false
			case "orphan_revision":
				releases, deps[0].Spec.Replicas = nil, &one
			case "orphan_pod":
				releases, deps, sets = nil, nil, nil
			case "drifted_child_labels":
				sets[0].Metadata.Labels, pods[0].Metadata.Labels = nil, nil
			case "other_app":
				releases = nil
				for _, objects := range [][]candidateAdmissionObject{deps, sets, pods} {
					objects[0].Metadata.Labels[runtime.FugueLabelAppID] = "other-app"
				}
				wantBlock = false
			case "other_namespace":
				for _, objects := range [][]candidateAdmissionObject{deps, sets, pods} {
					objects[0].Metadata.Namespace = "other-namespace"
				}
				wantBlock = false
			case "retired_residue":
				releases[0].Role, releases[0].Status = model.AppReleaseRoleRetired, model.AppReleaseStatusRetired
			case "legacy_canonical_alias":
				releases[0].DeploymentName, deps[0].Metadata.Name = runtime.RuntimeAppResourceName(app), runtime.RuntimeAppResourceName(app)
				deps[0].Metadata.Labels, sets, pods, wantBlock = nil, nil, nil, false
			}
			err := candidateAdmissionBlock(app, current, releases, deps, sets, pods)
			if (err != nil) != wantBlock {
				t.Fatalf("block=%v want=%v: %v", err != nil, wantBlock, err)
			}
		})
	}
}

func TestCandidateAdmissionRejectsBeforeCreatingReleaseOrChangingTraffic(t *testing.T) {
	for _, scenario := range []string{"occupied", "read_failure", "continued_list"} {
		t.Run(scenario, func(t *testing.T) {
			s, app, _, f := newDrainWorkloadFixtureWithOutcome(t, true)
			before, _ := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
			releases, _ := s.Store.ListAppReleaseMetadata(model.AppReleaseFilter{TenantID: app.TenantID, AppID: app.ID, IncludeRetired: true})
			f.extra = func(w http.ResponseWriter, r *http.Request) bool {
				if strings.HasSuffix(r.URL.Path, "/replicasets") || strings.HasSuffix(r.URL.Path, "/pods") {
					_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
					return true
				}
				if strings.HasSuffix(r.URL.Path, "/deployments") {
					if scenario == "read_failure" {
						http.Error(w, "unavailable", 503)
					} else {
						continuation := ""
						if scenario == "continued_list" {
							continuation = "next-page"
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]string{"continue": continuation}, "items": []map[string]any{f.deployment}})
					}
					return true
				}
				return false
			}
			candidate := app
			candidate.Spec.Image = "ghcr.io/example/api:v3"
			op := model.Operation{ID: "next-deploy", AppID: app.ID, TenantID: app.TenantID, Type: model.OperationTypeDeploy}
			if _, err := s.prepareSafeZeroDowntimeRollout(context.Background(), op, app, candidate); err == nil {
				t.Fatal("another candidate was admitted")
			}
			after, _ := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
			now, _ := s.Store.ListAppReleaseMetadata(model.AppReleaseFilter{TenantID: app.TenantID, AppID: app.ID, IncludeRetired: true})
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(releases, now) || f.deleteCalls != 0 {
				t.Fatal("admission changed traffic, releases or workloads")
			}
			if scenario == "occupied" {
				// Retirement, not admission, has now removed the Pod/ReplicaSet
				// and observed the historical Deployment at zero replicas.
				objectMapField(f.deployment, "spec")["replicas"] = 0
				f.deployment["status"] = map[string]any{"observedGeneration": 1, "replicas": 0}
				if err := s.checkSafeRolloutCandidateAdmission(context.Background(), candidate, ""); err != nil {
					t.Fatalf("cold history permanently occupied the candidate slot: %v", err)
				}
			}
		})
	}
}

func TestCandidateApplyRechecksAdmissionBeforeKubernetesMutation(t *testing.T) {
	st, _, app, op := newSafeRolloutTestState(t)
	s := &Service{Store: st, Renderer: runtime.Renderer{StrictDrain: runtime.DefaultStrictDrainConfig()}}
	s.Config.KubectlApply = true
	reads, writes := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writes++
			http.Error(w, "unexpected write", 500)
			return
		}
		reads++
		// An orphan revision appeared after preparation. No release record is
		// needed to prevent the next apply from increasing its resource demand.
		items := []map[string]any{}
		if strings.HasSuffix(r.URL.Path, "/deployments") {
			items = append(items, map[string]any{"metadata": map[string]any{"name": "orphan-workload", "namespace": runtime.NamespaceForTenant(app.TenantID), "uid": "orphan-uid", "labels": map[string]string{
				runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID, runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue,
				runtime.FugueLabelAppReleaseID: "orphan-release", runtime.FugueLabelAppReleaseRole: runtime.AppRevisionRoleCandidate,
			}}, "spec": map[string]int{"replicas": 1}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
	}))
	defer server.Close()
	s.newKubeClient = func(string) (*kubeClient, error) {
		return &kubeClient{baseURL: server.URL, client: server.Client()}, nil
	}
	state := &safeRolloutState{Enabled: true, CandidateApp: app, Candidate: model.AppRelease{ID: "new-release", AppID: app.ID, TenantID: app.TenantID}}
	err := s.applySafeZeroDowntimeCandidateRevision(context.Background(), op, state, runtime.SchedulingConstraints{}, nil)
	if err == nil || !strings.Contains(err.Error(), "candidate admission blocked") || reads != 3 || writes != 0 {
		t.Fatalf("apply bypassed admission: reads=%d writes=%d err=%v", reads, writes, err)
	}
}

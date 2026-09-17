package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestCandidateSupersessionRequiresPositiveConfigurationEvidence(t *testing.T) {
	stage := edgeCandidateStageReceipt{GroupID: "edge-group-test", AuthoritySequence: 12, CurrentPublicationSequence: 10, CandidateEpoch: 13, CurrentRecoveryEpoch: 2, CandidateBundleGeneration: "old-config"}
	status := edgeCandidateStageStatus{GroupID: stage.GroupID, Ready: true, ServingHealthy: true, LKGState: "current", PublicationDecision: "published", AuthoritySequence: 14, CurrentPublicationSequence: 14, RecoveryEpoch: 2, BundleGeneration: "new-config", PublishedBundleDigest: "sha256:" + strings.Repeat("a", 64)}
	if !edgeCandidateSupersededByPublication(status, stage) {
		t.Fatal("proven supersession not recognized")
	}
	for name, mutate := range map[string]func(*edgeCandidateStageStatus){
		"missing candidate alone": func(s *edgeCandidateStageStatus) { s.BundleGeneration = stage.CandidateBundleGeneration },
		"other group":             func(s *edgeCandidateStageStatus) { s.GroupID = "edge-group-other" },
		"unready":                 func(s *edgeCandidateStageStatus) { s.Ready = false },
		"unhealthy":               func(s *edgeCandidateStageStatus) { s.ServingHealthy = false },
		"preserved only":          func(s *edgeCandidateStageStatus) { s.LKGState = "preserved" },
		"other candidate":         func(s *edgeCandidateStageStatus) { s.CandidateEpoch = 15 },
		"candidate owner":         func(s *edgeCandidateStageStatus) { s.CandidateWorkerSourceSHA = strings.Repeat("b", 40) },
		"old publication":         func(s *edgeCandidateStageStatus) { s.CurrentPublicationSequence = 12 },
		"recovery changed":        func(s *edgeCandidateStageStatus) { s.RecoveryEpoch++ },
		"no digest":               func(s *edgeCandidateStageStatus) { s.PublishedBundleDigest = "" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := status
			mutate(&changed)
			if edgeCandidateSupersededByPublication(changed, stage) {
				t.Fatal("unproven supersession accepted")
			}
		})
	}
}

func TestCommittedCandidateAcceptsNewConfigurationWithExactCodeAndFreshFacts(t *testing.T) {
	now := time.Now().UTC()
	target := edgeTargetFixture("2", "b")
	stage := edgeCandidateStageReceipt{GroupID: "edge-group-test", CandidateRecordDigest: "sha256:" + strings.Repeat("1", 64), CandidateBundleGeneration: "old-config", WorkerSlot: "b", WorkerSourceSHA: target.ConfigSHA, WorkerImageDigest: digestFromTarget(t, target)}
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind, GroupID: stage.GroupID, CurrentRecordDigest: stage.CandidateRecordDigest, CurrentWorkerSlot: releaseguardian.AuthoritySlotB, CurrentFrontGeneration: 12, CurrentBundleGeneration: "old-config.p20.r2", CurrentWorkerSourceSHA: stage.WorkerSourceSHA, CurrentWorkerImageDigest: stage.WorkerImageDigest, PreviousRecordDigest: "sha256:" + strings.Repeat("4", 64), PreviousWorkerSlot: releaseguardian.AuthoritySlotA, PreviousFrontGeneration: 11, PreviousBundleGeneration: "previous.p19.r2", PreviousWorkerSourceSHA: strings.Repeat("5", 40), PreviousWorkerImageDigest: "sha256:" + strings.Repeat("6", 64), AuthorityEpoch: 7}
	pod := edgeGroupPod{Ready: true, SourceCommit: target.ConfigSHA, ImageRef: target.ImageRef, RouteBundleSource: edgeGroupAuthoritySource, BundleGeneration: "new-config.p21.r2", PublicationSequence: 21, ServingGeneration: "new-config", InventoryProducerActive: true, InventoryHeartbeatGeneration: 1, InventoryHeartbeatAt: now}
	check := func(p edgeGroupPod, c releaseguardian.CurrentAuthority) bool {
		return edgeCommittedCandidateCohortReady(map[string]edgeGroupPod{"node": p}, target, stage, c, 1, now)
	}
	if !check(pod, current) {
		t.Fatal("exact committed code rejected after independent config change")
	}
	for name, mutate := range map[string]func(*edgeGroupPod){
		"wrong code":  func(p *edgeGroupPod) { p.SourceCommit = strings.Repeat("7", 40) },
		"wrong image": func(p *edgeGroupPod) { p.ImageRef = "registry/image@sha256:" + strings.Repeat("7", 64) },
		"not ready":   func(p *edgeGroupPod) { p.Ready = false },
		"restart":     func(p *edgeGroupPod) { p.RestartCount = 1 },
		"inactive":    func(p *edgeGroupPod) { p.InventoryProducerActive = false },
		"stale":       func(p *edgeGroupPod) { p.InventoryHeartbeatAt = now.Add(-3 * time.Minute) },
		"regressed publication": func(p *edgeGroupPod) {
			p.BundleGeneration = "old-config.p19.r2"
			p.PublicationSequence = 19
			p.ServingGeneration = "old-config"
		},
		"same publication different config": func(p *edgeGroupPod) { p.BundleGeneration = "new-config.p20.r2"; p.PublicationSequence = 20 },
		"recovery regression":               func(p *edgeGroupPod) { p.BundleGeneration = "new-config.p21.r1" },
	} {
		t.Run(name, func(t *testing.T) {
			p := pod
			mutate(&p)
			if check(p, current) {
				t.Fatal("unbound committed fact accepted")
			}
		})
	}
	current.CurrentRecordDigest = "sha256:" + strings.Repeat("8", 64)
	if check(pod, current) {
		t.Fatal("unrelated committed release accepted")
	}
}

func restagingFixture(t *testing.T) (*fakeEdgeGroupRuntime, declarativerelease.PlanRelease, declarativerelease.EdgeGroupABTransition, declarativerelease.TargetIdentity, edgeGroupABPlan) {
	t.Helper()
	transition := edgeTransitionFixture()
	old, target := edgeTargetFixture("1", "a"), edgeTargetFixture("2", "b")
	before := edgeStateFixture("a", old, edgeFrontHealth{ActiveSlot: "a"})
	before.FrontActivation = &edgeActivationState{Schema: edgeActivationStateSchema, GroupID: transition.GroupID, Generation: 1, ActiveSlot: "a", BundleGeneration: "original.p1.r0", WorkerSourceCommit: old.ConfigSHA, WorkerImageDigest: digestFromTarget(t, old), Authority: edgeActivationAuthority}
	final := edgeStateFixture("b", target, edgeFrontHealth{ActiveSlot: "b"})
	r := &fakeEdgeGroupRuntime{snapshots: []edgeGroupState{before}, rolls: map[string]map[string]edgeGroupPod{transition.WorkerBName: final.WorkerB, transition.FrontName: final.Front}, waits: []map[string]edgeFrontHealth{final.FrontHealth}}
	plan := edgeGroupABPlan{before: before, frontTarget: target, activeSlot: "a", activeName: transition.WorkerAName, activeTarget: old, inactiveSlot: "b", inactiveName: transition.WorkerBName, desiredDigest: digestFromTarget(t, target)}
	return r, declarativerelease.PlanRelease{ExpectedPreviousConfigSHA: old.ConfigSHA}, transition, target, plan
}

func TestCodeReleaseRestagesOnlySupersededConfigurationBeforeTrafficChanges(t *testing.T) {
	for _, phase := range []string{"roll", "candidate", "commit"} {
		t.Run(phase, func(t *testing.T) {
			r, release, transition, target, plan := restagingFixture(t)
			if phase == "roll" {
				r.candidateRollErrors = []error{errEdgeCandidateConfigurationAdvanced}
			} else if phase == "candidate" {
				r.candidateWaitErrors = []error{errEdgeCandidateConfigurationAdvanced}
			} else {
				r.currentWaitErrors = []error{errEdgeCandidateConfigurationAdvanced}
			}
			if _, _, _, err := stageAndCommitEdgeGroupAuthority(context.Background(), r, release, transition, target, plan); err != nil {
				t.Fatal(err)
			}
			stages := 0
			for _, call := range r.calls {
				if call == "stage:b" {
					stages++
				}
			}
			if stages != 2 || len(r.requests) != 0 {
				t.Fatal("restaging mutated traffic or skipped fresh candidate", r.calls)
			}
		})
	}
}

func TestCodeReleaseRestagingIsBoundedAndRejectsChangedAuthority(t *testing.T) {
	for _, mode := range []string{"changed activation", "changed active code", "stale inventory", "unknown failure", "continuous supersession"} {
		t.Run(mode, func(t *testing.T) {
			r, release, transition, target, plan := restagingFixture(t)
			r.candidateWaitErrors = []error{errEdgeCandidateConfigurationAdvanced}
			wantStages := 1
			switch mode {
			case "changed activation":
				a := *r.snapshots[0].FrontActivation
				a.Generation++
				r.snapshots[0].FrontActivation = &a
			case "unknown failure":
				r.candidateWaitErrors = []error{errors.New("candidate unavailable")}
			case "changed active code", "stale inventory":
				workers := map[string]edgeGroupPod{}
				for node, worker := range r.snapshots[0].WorkerA {
					if mode == "changed active code" {
						worker.SourceCommit = strings.Repeat("9", 40)
					} else {
						worker.InventoryHeartbeatAt = time.Now().Add(-3 * time.Minute)
					}
					workers[node] = worker
				}
				r.snapshots[0].WorkerA = workers
			case "continuous supersession":
				wantStages = edgeCandidateStageAttempts
				r.snapshots = nil
				r.candidateWaitErrors = nil
				for i := 0; i < edgeCandidateStageAttempts; i++ {
					r.snapshots = append(r.snapshots, plan.before)
					r.candidateWaitErrors = append(r.candidateWaitErrors, errEdgeCandidateConfigurationAdvanced)
				}
			}
			if _, _, _, err := stageAndCommitEdgeGroupAuthority(context.Background(), r, release, transition, target, plan); err == nil {
				t.Fatal("unproven release continued")
			}
			stages := 0
			for _, call := range r.calls {
				if call == "stage:b" {
					stages++
				}
				if call == "wait-front:b" {
					t.Fatal("failed restaging reached traffic switch")
				}
			}
			if stages != wantStages || len(r.requests) != 0 {
				t.Fatal("unbounded or unsafe restaging", r.calls)
			}
		})
	}
}

func TestWaitCurrentAuthorityReadsSupersessionWithoutWritingTraffic(t *testing.T) {
	old := edgeTargetFixture("1", "a")
	current := releaseguardian.CurrentAuthority{APIVersion: releaseguardian.APIVersion, Kind: releaseguardian.CurrentAuthorityKind, GroupID: "edge-group-test", CurrentRecordDigest: "sha256:" + strings.Repeat("a", 64), CurrentWorkerSlot: releaseguardian.AuthoritySlotA, AuthorityEpoch: 1, CurrentFrontGeneration: 1, CurrentBundleGeneration: "old-config.p10.r2", CurrentWorkerSourceSHA: old.ConfigSHA, CurrentWorkerImageDigest: digestFromTarget(t, old)}
	if err := current.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(current)
	object := &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]interface{}{"name": "fugue-current-authority-edge-group-test", "namespace": "test-system", "uid": "current-uid", "resourceVersion": "1"}, "data": map[string]interface{}{"authority.json": string(raw)}}}
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	status := edgeCandidateStageStatus{GroupID: current.GroupID, Ready: true, ServingHealthy: true, AuthoritySequence: 14, CurrentPublicationSequence: 14, RecoveryEpoch: 2, LKGState: "current", PublicationDecision: "published", BundleGeneration: "new-config", PublishedBundleDigest: "sha256:" + strings.Repeat("b", 64)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Error("progress observation attempted a mutation")
		}
		json.NewEncoder(w).Encode(status)
	}))
	defer server.Close()
	r := kubectlEdgeGroupRuntime{client: client, cluster: &kubectlCluster{timeout: time.Second}, release: declarativerelease.PlanRelease{Workload: declarativerelease.Workload{Namespace: "test-system"}}, transition: declarativerelease.EdgeGroupABTransition{GroupID: current.GroupID, CandidateStageURL: server.URL + edgeCandidateStagePath}}
	stage := edgeCandidateStageReceipt{GroupID: current.GroupID, AuthoritySequence: 12, CurrentPublicationSequence: 10, CandidateEpoch: 13, CurrentRecoveryEpoch: 2, CandidateBundleGeneration: "old-config", CurrentWorkerSlot: "a", WorkerSlot: "b"}
	if err := r.WaitCurrentAuthority(context.Background(), stage); !errors.Is(err, errEdgeCandidateConfigurationAdvanced) {
		t.Fatalf("lost typed configuration progress: %v", err)
	}
	for _, phase := range []string{"prepared", "activated", "unreadable", "committed while observing"} {
		t.Run(phase, func(t *testing.T) {
			c := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object.DeepCopy())
			journalName := "fugue-authority-transition-" + phase + "-" + current.GroupID
			c.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				name := action.(ktesting.GetAction).GetName()
				if name == journalName {
					return true, &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap"}}, nil
				}
				if phase == "unreadable" && strings.HasPrefix(name, "fugue-authority-transition-") {
					return true, nil, errors.New("journal observation unavailable")
				}
				if phase == "committed while observing" && name == object.GetName() {
					changed := current
					changed.CurrentWorkerSlot = releaseguardian.AuthoritySlotB
					raw, _ := json.Marshal(changed)
					observed := object.DeepCopy()
					_ = unstructured.SetNestedField(observed.Object, string(raw), "data", "authority.json")
					return true, observed, nil
				}
				return false, nil, nil
			})
			observer := r
			observer.client = c
			if observer.candidateConfigurationAdvanced(context.Background(), stage) {
				t.Fatal("configuration advancement retired an in-flight or committed code transaction")
			}
			for _, action := range c.Actions() {
				if action.GetVerb() != "get" {
					t.Fatal("transaction observation mutated authority", action.GetVerb())
				}
			}
		})
	}
	t.Run("waits for exact journal commit", func(t *testing.T) {
		target := edgeTargetFixture("2", "b")
		staged := stage
		staged.WorkerSourceSHA, staged.WorkerImageDigest = target.ConfigSHA, digestFromTarget(t, target)
		staged.CandidateRecordDigest = "sha256:" + strings.Repeat("c", 64)
		committed := current
		committed.PreviousRecordDigest, committed.PreviousWorkerSlot = current.CurrentRecordDigest, current.CurrentWorkerSlot
		committed.PreviousWorkerSourceSHA, committed.PreviousWorkerImageDigest = current.CurrentWorkerSourceSHA, current.CurrentWorkerImageDigest
		committed.PreviousFrontGeneration, committed.PreviousBundleGeneration = current.CurrentFrontGeneration, current.CurrentBundleGeneration
		committed.CurrentRecordDigest, committed.CurrentWorkerSlot = staged.CandidateRecordDigest, releaseguardian.AuthoritySlotB
		committed.CurrentWorkerSourceSHA, committed.CurrentWorkerImageDigest = staged.WorkerSourceSHA, staged.WorkerImageDigest
		committed.CurrentFrontGeneration, committed.CurrentBundleGeneration = 2, "old-config.p13.r2"
		committed.AuthorityEpoch++
		if err := committed.Validate(); err != nil {
			t.Fatal(err)
		}
		currentReads := 0
		c := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object.DeepCopy())
		c.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
			name := action.(ktesting.GetAction).GetName()
			if strings.HasPrefix(name, "fugue-authority-transition-prepared-") {
				return true, &unstructured.Unstructured{Object: map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap"}}, nil
			}
			if name == object.GetName() {
				currentReads++
				if currentReads > 1 {
					raw, _ := json.Marshal(committed)
					observed := object.DeepCopy()
					_ = unstructured.SetNestedField(observed.Object, string(raw), "data", "authority.json")
					return true, observed, nil
				}
			}
			return false, nil, nil
		})
		observer := r
		observer.client, observer.cluster = c, &kubectlCluster{timeout: 3 * time.Second}
		if err := observer.WaitCurrentAuthority(context.Background(), staged); err != nil {
			t.Fatalf("in-flight exact commit became a restage/rollback error: %v", err)
		}
		if currentReads != 2 {
			t.Fatalf("expected fresh observation of exact commit, got %d reads", currentReads)
		}
		for _, action := range c.Actions() {
			if action.GetVerb() != "get" {
				t.Fatal("commit observation mutated authority", action.GetVerb())
			}
		}
	})
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" {
			t.Fatal("progress mutated authority", action.GetVerb())
		}
	}
}

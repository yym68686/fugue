package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

func startupBudgetFixture(t *testing.T) (*Service, model.App, model.Operation, kubeDeployment, kubePod) {
	t.Helper()
	st, _, _, app := newImageTrackingSyncTestStore(t)
	op, err := st.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{Store: st}
	deployment, found := s.expectedManagedAppRevisionDeployment(app, runtime.SchedulingConstraints{}, safeRolloutCandidateRevision("release"))
	if !found {
		t.Fatal("missing candidate deployment")
	}
	deployment.Metadata.UID = "deployment-uid"
	pod := readyTemplatePod("revision-pod", deployment, kubeResourceRequirements{})
	pod.ObservedUID = "pod-uid"
	pod.Status.ContainerStatuses = []kubeContainerStatus{{Name: runtime.RuntimeResourceName(app.Name)}}
	return s, app, op, deployment, pod
}

func crashCandidate(pod *kubePod, restarts int) {
	pod.Status.Phase = "Running"
	pod.Status.Conditions = []kubePodCondition{{Type: "Ready", Status: "False"}}
	pod.Status.ContainerStatuses[0].Ready = false
	pod.Status.ContainerStatuses[0].RestartCount = restarts
	pod.Status.ContainerStatuses[0].State = kubeRuntimeState{Waiting: &kubeStateDetail{Reason: "CrashLoopBackOff"}}
	pod.Status.ContainerStatuses[0].LastState = kubeRuntimeState{Terminated: &kubeStateDetail{Reason: "Error", ExitCode: 1}}
}

func TestCandidateStartupBudgetPersistsAcrossTakeoverAndPodReplacement(t *testing.T) {
	s, app, op, deployment, pod := startupBudgetFixture(t)
	ctx := context.Background()
	for _, count := range []int{1, 1, 2} {
		crashCandidate(&pod, count)
		memory := candidateStartupObservation{}
		message, err := s.candidateStartupFailureMessage(ctx, app, op.ID, deployment, []kubePod{pod}, &memory)
		if err != nil || message != "" || memory.Failures != count {
			t.Fatalf("retry count=%d message=%q state=%+v err=%v", count, message, memory, err)
		}
		// A new controller instance cannot reset the persisted budget.
		s = &Service{Store: s.Store}
	}
	pod.Metadata.Name, pod.ObservedUID = "replacement-pod", "replacement-uid"
	crashCandidate(&pod, 1)
	memory := candidateStartupObservation{}
	message, err := s.candidateStartupFailureMessage(ctx, app, op.ID, deployment, []kubePod{pod}, &memory)
	if err != nil || !strings.Contains(message, "budget exhausted (3/3)") {
		t.Fatalf("replacement bypassed retry budget: %q %v", message, err)
	}
	deployment.Metadata.UID = "different-deployment"
	if _, err := s.candidateStartupFailureMessage(ctx, app, op.ID, deployment, []kubePod{pod}, &memory); !errors.Is(err, errOperationPersistenceInterrupted) {
		t.Fatalf("identity drift silently reset observation: %v", err)
	}
}

func TestCandidateStartupBudgetRecoveryDoesNotUseLifetimeRestartCount(t *testing.T) {
	s, app, op, deployment, pod := startupBudgetFixture(t)
	now := time.Now()
	s.now = func() time.Time { return now }
	ready := func() {
		pod.Status.Conditions = []kubePodCondition{{Type: "Ready", Status: "True"}}
		pod.Status.ContainerStatuses[0].Ready = true
		pod.Status.ContainerStatuses[0].State = kubeRuntimeState{Running: &kubeStateDetail{StartedAt: now.Format(time.RFC3339)}}
	}
	pod.Status.ContainerStatuses[0].RestartCount = 47
	ready()
	memory := candidateStartupObservation{}
	observe := func() {
		t.Helper()
		message, err := s.candidateStartupFailureMessage(context.Background(), app, op.ID, deployment, []kubePod{pod}, &memory)
		if err != nil || message != "" {
			t.Fatalf("recovery rejected: %q %v", message, err)
		}
	}
	observe()
	if memory.Failures != 0 {
		t.Fatal("historical restarts counted against healthy candidate")
	}
	crashCandidate(&pod, 48)
	observe()
	if memory.Failures != 1 {
		t.Fatal("new failure was not counted")
	}
	ready()
	observe()
	now = now.Add(candidateStartupHealthyReset + time.Second)
	observe()
	crashCandidate(&pod, 49)
	observe()
	if memory.Failures != 1 {
		t.Fatal("stable readiness did not reset consecutive failures")
	}
}

func TestCandidateStartupBudgetPreservesOtherFailureGates(t *testing.T) {
	for _, scenario := range []string{"image", "init", "sidecar", "foreign", "deleting", "fatal-pod", "main-exit-zero", "running-last-exit"} {
		t.Run(scenario, func(t *testing.T) {
			s, app, op, deployment, pod := startupBudgetFixture(t)
			crashCandidate(&pod, 1)
			switch scenario {
			case "image":
				pod.Status.ContainerStatuses[0].State.Waiting.Reason = "ImagePullBackOff"
			case "init":
				pod.Status.InitContainerStatuses = []kubeContainerStatus{{Name: "init", State: kubeRuntimeState{Waiting: &kubeStateDetail{Reason: "CrashLoopBackOff"}}}}
			case "sidecar":
				pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, kubeContainerStatus{Name: "sidecar", State: kubeRuntimeState{Waiting: &kubeStateDetail{Reason: "CrashLoopBackOff"}}})
			case "foreign":
				pod.Metadata.Labels = map[string]string{"unrelated": "true"}
			case "deleting":
				pod.Metadata.DeletionTimestamp = time.Now().Format(time.RFC3339)
			case "fatal-pod":
				pod.Status.Phase, pod.Status.Reason = "Failed", "Evicted"
			case "main-exit-zero":
				pod.Status.ContainerStatuses[0].LastState.Terminated = &kubeStateDetail{Reason: "Completed", ExitCode: 0}
			case "running-last-exit":
				pod.Status.ContainerStatuses[0].RestartCount = 3
				pod.Status.ContainerStatuses[0].State = kubeRuntimeState{Running: &kubeStateDetail{StartedAt: time.Now().Format(time.RFC3339)}}
			}
			memory := candidateStartupObservation{}
			message, err := s.candidateStartupFailureMessage(context.Background(), app, op.ID, deployment, []kubePod{pod}, &memory)
			if err != nil {
				t.Fatal(err)
			}
			wantFailure := scenario == "image" || scenario == "init" || scenario == "sidecar" || scenario == "fatal-pod"
			if (message != "") != wantFailure {
				t.Fatalf("unexpected failure gate: %q", message)
			}
			if (scenario == "foreign" || scenario == "deleting") && memory.Failures != 0 {
				t.Fatal("unowned/deleting Pod consumed budget")
			}
		})
	}
}

func TestCandidateStartupFailedReleaseBudgetAndExplicitReset(t *testing.T) {
	s, app, first, _, _ := startupBudgetFixture(t)
	ctx := context.Background()
	digest := "sha256:" + strings.Repeat("a", 64)
	app.Spec.Env = map[string]string{"SETTING": "private-setting"}
	if _, err := s.Store.FailOperation(first.ID, "fixture complete"); err != nil {
		t.Fatal(err)
	}
	makeOperation := func(actor, requested string) model.Operation {
		t.Helper()
		op, err := s.Store.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, RequestedByType: actor, RequestedByID: requested, DesiredSpec: &app.Spec})
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.Store.CreateReleaseAttempt(model.ReleaseAttempt{TenantID: app.TenantID, AppID: app.ID, ProjectID: app.ProjectID, SourceOperationID: op.ID, RootOperationID: op.ID, TargetDigest: digest, TriggerType: model.ReleaseAttemptTriggerImageTrackingAuto})
		if err != nil {
			t.Fatal(err)
		}
		return op
	}
	for i := 0; i < 3; i++ {
		op := makeOperation(model.ActorTypeSystem, model.OperationRequestedByImageTracking)
		for duplicate := 0; duplicate < 2; duplicate++ {
			if err := s.updateCandidateStartupFailures(ctx, app, op, false); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Store.FailOperation(op.ID, "candidate startup budget exhausted"); err != nil {
			t.Fatal(err)
		}
		reason, err := s.candidateStartupRetrySuppression(ctx, app, digest)
		if err != nil || (reason != "") != (i == 2) {
			t.Fatalf("failed releases=%d suppression=%q err=%v", i+1, reason, err)
		}
	}
	s = &Service{Store: s.Store}
	if reason, err := s.candidateStartupRetrySuppression(ctx, app, "sha256:"+strings.Repeat("b", 64)); err != nil || reason != "" {
		t.Fatal("new image inherited old budget", reason, err)
	}
	changed := app
	changed.Spec = *cloneControllerAppSpec(&app.Spec)
	changed.Spec.Env["SETTING"] = "changed"
	if reason, err := s.candidateStartupRetrySuppression(ctx, changed, digest); err != nil || reason != "" {
		t.Fatal("new configuration inherited old budget", reason, err)
	}
	changed = app
	changed.Spec.RestartToken = "new-pod-token"
	if reason, err := s.candidateStartupRetrySuppression(ctx, changed, digest); err != nil || reason == "" {
		t.Fatal("mutable token reset automatic retry budget", reason, err)
	}
	for _, requested := range []string{model.OperationRequestedByImageTracking, model.OperationRequestedByGitHubSyncController, model.OperationRequestedByImageRebuild} {
		auto := makeOperation(model.ActorTypeSystem, requested)
		if err := s.checkCandidateStartupRetryBudget(ctx, app, auto); err == nil {
			t.Fatal("direct automatic creation bypassed budget", requested)
		}
		_, _ = s.Store.FailOperation(auto.ID, "blocked")
	}
	manual := makeOperation(model.ActorTypeAPIKey, "explicit-deploy")
	if err := s.checkCandidateStartupRetryBudget(ctx, app, manual); err != nil {
		t.Fatal("explicit retry blocked", err)
	}
	if err := s.updateCandidateStartupFailures(ctx, app, manual, false); err != nil {
		t.Fatal(err)
	}
	// Resuming this explicit operation cannot repeatedly reset its own failure.
	if err := s.checkCandidateStartupRetryBudget(ctx, app, manual); err != nil {
		t.Fatal(err)
	}
	raw, err := s.Store.ReadObservationCheckpoint(ctx, candidateStartupLedgerKey(app))
	if err != nil || strings.Contains(string(raw), "private-setting") || strings.Contains(string(raw), digest) {
		t.Fatal("checkpoint exposed executable input", err)
	}
	ledger, err := decodeCandidateStartupLedger(raw)
	if err != nil || len(ledger.Inputs[s.candidateStartupFingerprint(app, digest)].Operations) != 1 {
		t.Fatal("explicit reset was not idempotent", err)
	}
	manual.Status = model.OperationStatusCompleted
	s.completeReleaseAttemptForOperation(manual, app, "verified successful deployment")
	raw, _ = s.Store.ReadObservationCheckpoint(ctx, candidateStartupLedgerKey(app))
	ledger, _ = decodeCandidateStartupLedger(raw)
	if len(ledger.Inputs[s.candidateStartupFingerprint(app, digest)].Operations) != 0 {
		t.Fatal("successful deployment did not clear consecutive release failures")
	}
}

func TestCandidateStartupFirstExplicitOperationResetIsIdempotent(t *testing.T) {
	s, app, op, _, _ := startupBudgetFixture(t)
	op.RequestedByType = model.ActorTypeAPIKey
	ctx := context.Background()
	if err := s.checkCandidateStartupRetryBudget(ctx, app, op); err != nil {
		t.Fatal(err)
	}
	if err := s.updateCandidateStartupFailures(ctx, app, op, false); err != nil {
		t.Fatal(err)
	}
	if err := s.checkCandidateStartupRetryBudget(ctx, app, op); err != nil {
		t.Fatal(err)
	}
	raw, err := s.Store.ReadObservationCheckpoint(ctx, candidateStartupLedgerKey(app))
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := decodeCandidateStartupLedger(raw)
	fingerprint, fingerprintErr := s.candidateStartupOperationFingerprint(app, op)
	if err != nil || fingerprintErr != nil || len(ledger.Inputs[fingerprint].Operations) != 1 {
		t.Fatalf("first explicit operation reset its own failure: ledger=%+v err=%v fingerprintErr=%v", ledger, err, fingerprintErr)
	}
}

func TestCandidateStartupWaitDoesNotAcceptStaleDeploymentReadiness(t *testing.T) {
	s, app, _, deployment, _ := startupBudgetFixture(t)
	s.Store = nil
	s.Config = config.ControllerConfig{ManagedAppRolloutTimeout: time.Second}
	revision := safeRolloutCandidateRevision("release")
	expected, _ := s.expectedManagedAppRevisionDeployment(app, runtime.SchedulingConstraints{}, revision)
	deployment.Metadata.Generation = 1
	deployment.Metadata.ResourceVersion = "1"
	deployment.Status.ObservedGeneration = 1
	deployment.Status.UpdatedReplicas, deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas = 1, 1, 1
	reads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Query().Get("watch") == "true":
			fmt.Fprintln(w, `{"type":"MODIFIED","object":{"metadata":{"resourceVersion":"next"}}}`)
		case strings.Contains(r.URL.Path, "/deployments/"):
			_ = json.NewEncoder(w).Encode(deployment)
		case strings.HasSuffix(r.URL.Path, "/pods"):
			reads++
			if reads == 1 {
				list := kubePodList{}
				list.Metadata.ResourceVersion = "1"
				_ = json.NewEncoder(w).Encode(list)
				return
			}
			pod := readyTemplatePod("candidate-pod", expected, kubeResourceRequirements{})
			pod.Status.ContainerStatuses = []kubeContainerStatus{{Name: runtime.RuntimeResourceName(app.Name), Ready: true}}
			if reads == 2 {
				crashCandidate(&pod, 1)
			}
			list := kubePodList{Items: []kubePod{pod}}
			list.Metadata.ResourceVersion = "1"
			_ = json.NewEncoder(w).Encode(list)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	s.newKubeClient = func(string) (*kubeClient, error) {
		return &kubeClient{baseURL: server.URL, client: server.Client()}, nil
	}
	if err := s.waitForManagedAppRevisionRolloutErrorWithScheduling(context.Background(), app, "", runtime.SchedulingConstraints{}, revision, expected); err != nil || reads < 3 {
		t.Fatalf("candidate readiness bypassed retry observation: reads=%d err=%v", reads, err)
	}
}

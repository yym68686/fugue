package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type activationFixtureExecutor struct {
	raw []byte
}

func (executor activationFixtureExecutor) Exec(context.Context, string, string, string, ...string) ([]byte, error) {
	return append([]byte(nil), executor.raw...), nil
}

func TestPostActivationRouteRequiresConsecutiveExactAttestations(t *testing.T) {
	body := []byte("ok")
	bodyDigest := shaDigest(body)
	record := "sha256:" + strings.Repeat("1", 64)
	matching := http.Header{
		"X-Fugue-Candidate-Record-Digest": []string{record},
		"X-Fugue-Candidate-Worker-Slot":   []string{"a"},
	}
	responses := []struct {
		status  int
		headers http.Header
		err     error
	}{
		{status: http.StatusServiceUnavailable},
		{status: http.StatusOK, headers: matching},
		{status: http.StatusOK, headers: matching},
		{status: http.StatusBadGateway},
		{status: http.StatusOK, headers: matching},
		{status: http.StatusOK, headers: matching},
		{status: http.StatusOK, headers: matching},
	}
	calls := 0
	err := waitForAuthorityRoute(context.Background(), canaryProbe{}, bodyDigest, record, releaseguardian.AuthoritySlotA,
		false, len(responses), 3, 0, func(context.Context, canaryProbe) (int, []byte, http.Header, error) {
			response := responses[calls]
			calls++
			return response.status, body, response.headers, response.err
		})
	if err != nil || calls != len(responses) {
		t.Fatalf("exact route did not converge after transient failures: calls=%d err=%v", calls, err)
	}
}

func TestPostActivationRouteFailsClosedWithoutExactAttestation(t *testing.T) {
	body := []byte("ok")
	bodyDigest := shaDigest(body)
	record := "sha256:" + strings.Repeat("1", 64)
	wrong := http.Header{
		"X-Fugue-Candidate-Record-Digest": []string{"sha256:" + strings.Repeat("2", 64)},
		"X-Fugue-Candidate-Worker-Slot":   []string{"a"},
	}
	calls := 0
	err := waitForAuthorityRoute(context.Background(), canaryProbe{}, bodyDigest, record, releaseguardian.AuthoritySlotA,
		false, 4, 2, 0, func(context.Context, canaryProbe) (int, []byte, http.Header, error) {
			calls++
			if calls == 2 {
				return 0, nil, nil, errors.New("transient transport failure")
			}
			return http.StatusOK, body, wrong, nil
		})
	if err == nil || calls != 4 {
		t.Fatalf("wrong route attestation was accepted: calls=%d err=%v", calls, err)
	}
	message := err.Error()
	for _, want := range []string{"attempts=4", "status=200", "bodyDigest=" + bodyDigest,
		"recordDigest=sha256:" + strings.Repeat("2", 64), "slot=a", "transport=none"} {
		if !strings.Contains(message, want) {
			t.Fatalf("typed route failure omits %q: %s", want, message)
		}
	}
}

func TestFrontLKGGenerationAcceptsOnlyExactCompensationChain(t *testing.T) {
	base := uint64(34)
	if !frontLKGGenerationMatches(edgegroupfront.ActivationState{Generation: base}, base, edgegroupfront.ActivationOperationPromote) {
		t.Fatal("exact baseline generation was rejected")
	}
	compensated := edgegroupfront.ActivationState{Generation: base + 2, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 1}
	if !frontLKGGenerationMatches(compensated, base, edgegroupfront.ActivationOperationPromote) {
		t.Fatal("exact adjacent compensation was rejected")
	}
	secondCompensation := edgegroupfront.ActivationState{Generation: base + 4, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 3}
	if !frontLKGGenerationMatches(secondCompensation, base, edgegroupfront.ActivationOperationPromote) {
		t.Fatal("second exact compensation in one durable journal was rejected")
	}
	for name, state := range map[string]edgegroupfront.ActivationState{
		"generation gap":   {Generation: base + 3, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 2},
		"wrong link":       {Generation: base + 2, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base},
		"later wrong link": {Generation: base + 4, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 2},
		"not rollback":     {Generation: base + 2, Operation: edgegroupfront.ActivationOperationPromote},
	} {
		t.Run(name, func(t *testing.T) {
			if frontLKGGenerationMatches(state, base, edgegroupfront.ActivationOperationPromote) {
				t.Fatal("non-adjacent compensation was accepted")
			}
		})
	}
	if !frontLKGGenerationMatches(compensated, base, edgegroupfront.ActivationOperationRollback) {
		t.Fatal("restore retry rejected exact compensation chain")
	}
}

func TestFrontRestoreGenerationAcceptsOnlyExactRetryChain(t *testing.T) {
	base := uint64(95)
	if !frontTargetGenerationMatches(edgegroupfront.ActivationState{Generation: base + 1}, base, edgegroupfront.ActivationOperationRollback) {
		t.Fatal("first restore generation was rejected")
	}
	retry := edgegroupfront.ActivationState{Generation: base + 3, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 2}
	if !frontTargetGenerationMatches(retry, base, edgegroupfront.ActivationOperationRollback) {
		t.Fatal("exact compensated restore retry was rejected")
	}
	for name, state := range map[string]edgegroupfront.ActivationState{
		"even target":  {Generation: base + 2, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 1},
		"wrong link":   {Generation: base + 3, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: base + 1},
		"not rollback": {Generation: base + 3, Operation: edgegroupfront.ActivationOperationPromote},
	} {
		t.Run(name, func(t *testing.T) {
			if frontTargetGenerationMatches(state, base, edgegroupfront.ActivationOperationRollback) {
				t.Fatal("invalid restore retry generation was accepted")
			}
		})
	}
}

func TestRollbackReplayBundleAcceptsOnlyMonotonicSignedPublication(t *testing.T) {
	committed := "edgegroupbundle_old.p41138.r175"
	for _, observed := range []string{
		committed,
		"edgegroupbundle_old.p41139.r176",
		"edgegroupbundle_new.p41672.r175",
		"edgegroupbundle_standby.p41672.r0",
	} {
		if !authorityBundleAtOrAfter(observed, committed) {
			t.Fatalf("valid rollback replay bundle %q was rejected", observed)
		}
	}
	for _, observed := range []string{
		"edgegroupbundle_new.p41138.r176",
		"edgegroupbundle_new.p41672.r174",
		"edgegroupbundle_old.p41137.r176",
		"edgegroupbundle_standby.p41138.r0",
		"edgegroupbundle_standby.p41137.r0",
		"edgegroupbundle_new",
	} {
		if authorityBundleAtOrAfter(observed, committed) {
			t.Fatalf("invalid rollback replay bundle %q was accepted", observed)
		}
	}
}

func TestObservedFrontFromActivationPreservesRecoveryWitness(t *testing.T) {
	state := edgegroupfront.ActivationState{GroupID: "edge-group-country-de", Generation: 136, ActiveSlot: "b",
		BundleGeneration: "bundle.p42.r7", WorkerSourceCommit: strings.Repeat("1", 40),
		WorkerImageDigest: "sha256:" + strings.Repeat("2", 64), Authority: edgegroupfront.ActivationAuthority}
	observed := observedFrontFromActivation(state)
	if observed.Status != "recovery-witness" || observed.Generation != state.Generation || observed.ActiveSlot != state.ActiveSlot ||
		observed.BundleGeneration != state.BundleGeneration || observed.WorkerSourceCommit != state.WorkerSourceCommit ||
		observed.WorkerImageDigest != state.WorkerImageDigest || observed.RouteAuthority != state.Authority {
		t.Fatalf("activation witness was not preserved: observed=%+v state=%+v", observed, state)
	}
}

func TestAuthorityRuntimeRequiresExactWorkerAndFrontIdentity(t *testing.T) {
	source := strings.Repeat("1", 40)
	digest := "sha256:" + strings.Repeat("2", 64)
	worker := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Annotations: map[string]string{"fugue.pro/source-commit": source}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "edge", Ready: true,
			ImageID: "ghcr.io/example/edge@" + digest}}}}
	workers := map[string]corev1.Pod{"node-a": worker}
	cohort := releaseguardian.CandidateWorkerCohort{WorkerSourceSHA: source, WorkerImageDigest: digest}
	fronts := map[string]observedFront{"node-a": {Generation: 12, ActiveSlot: "a", BundleGeneration: "bundle-a.p8.r1",
		WorkerSourceCommit: source, WorkerImageDigest: digest}}
	if !authorityRuntimeMatches(workers, cohort, fronts, releaseguardian.AuthoritySlotA, source, digest, 12, "bundle-a.p8.r1", true) {
		t.Fatal("exact current authority runtime was rejected")
	}
	if !authorityRuntimeMatches(workers, cohort, nil, releaseguardian.AuthoritySlotA, source, digest, 0, "bundle-a.p8.r1", false) {
		t.Fatal("exact inactive LKG Worker was rejected")
	}
	oldSource := strings.Repeat("3", 40)
	oldDigest := "sha256:" + strings.Repeat("4", 64)
	oldWorker := worker
	oldWorker.Annotations = map[string]string{"fugue.pro/source-commit": oldSource}
	oldWorker.Status.ContainerStatuses[0].ImageID = "ghcr.io/example/edge@" + oldDigest
	if authorityRuntimeMatches(map[string]corev1.Pod{"node-a": oldWorker}, cohort, fronts, releaseguardian.AuthoritySlotA, source, digest, 12, "bundle-a.p8.r1", true) {
		t.Fatal("candidate pointer accepted a compensated LKG Worker runtime")
	}
	if authorityRuntimeMatches(workers, cohort, map[string]observedFront{"node-a": {
		Generation: 12, ActiveSlot: "a", BundleGeneration: "bundle-a.p8.r1", WorkerSourceCommit: oldSource, WorkerImageDigest: oldDigest,
	}}, releaseguardian.AuthoritySlotA, source, digest, 12, "bundle-a.p8.r1", true) {
		t.Fatal("candidate pointer accepted a compensated LKG Front runtime")
	}
}

func TestCompensatedCurrentRuntimeAcceptsOnlyExactRollbackChain(t *testing.T) {
	group := "edge-group-country-us"
	source := strings.Repeat("1", 40)
	digest := "sha256:" + strings.Repeat("2", 64)
	bundle := "routes.p39713.r124"
	worker := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Annotations: map[string]string{"fugue.pro/source-commit": source}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "edge", Ready: true,
			ImageID: "ghcr.io/example/edge@" + digest}}}}
	workers := map[string]corev1.Pod{"node-a": worker}
	cohort := releaseguardian.CandidateWorkerCohort{WorkerSourceSHA: source, WorkerImageDigest: digest}
	fronts := map[string]observedFront{"node-a": {Generation: 79, ActiveSlot: "a", BundleGeneration: bundle,
		WorkerSourceCommit: source, WorkerImageDigest: digest}}
	state := edgegroupfront.ActivationState{Schema: edgegroupfront.ActivationStateSchemaV1, GroupID: group, Generation: 79,
		ActiveSlot: "a", BundleGeneration: bundle, WorkerSourceCommit: source, WorkerImageDigest: digest,
		Authority: edgegroupfront.ActivationAuthority, Operation: edgegroupfront.ActivationOperationRollback, RollbackOfGeneration: 78}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	activator := &frontAuthorityActivator{executor: activationFixtureExecutor{raw: raw}, config: frontAuthorityConfig{GroupID: group, Namespace: "fugue-system"}}
	if !activator.compensatedCurrentRuntimeMatches(context.Background(), workers, cohort, fronts, releaseguardian.AuthoritySlotA,
		source, digest, 29, bundle) {
		t.Fatal("production compensation chain 29 -> 79 was rejected")
	}

	for name, mutate := range map[string]func(*edgegroupfront.ActivationState){
		"odd generation": func(value *edgegroupfront.ActivationState) { value.Generation = 78; value.RollbackOfGeneration = 77 },
		"broken link":    func(value *edgegroupfront.ActivationState) { value.RollbackOfGeneration = 77 },
		"wrong operation": func(value *edgegroupfront.ActivationState) {
			value.Operation = edgegroupfront.ActivationOperationPromote
		},
		"wrong bundle": func(value *edgegroupfront.ActivationState) { value.BundleGeneration = "routes.p39714.r0" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := state
			mutate(&changed)
			changedRaw, marshalErr := json.Marshal(changed)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			candidate := *activator
			candidate.executor = activationFixtureExecutor{raw: changedRaw}
			if candidate.compensatedCurrentRuntimeMatches(context.Background(), workers, cohort, fronts, releaseguardian.AuthoritySlotA,
				source, digest, 29, bundle) {
				t.Fatal("invalid compensation chain was accepted")
			}
		})
	}
}

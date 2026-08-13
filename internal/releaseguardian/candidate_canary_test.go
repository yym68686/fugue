package releaseguardian

import (
	"strings"
	"testing"
	"time"
)

var candidateCanaryTestKey = []byte("candidate-canary-test-key-material-32-bytes-minimum")

func candidateCanaryFixture(t *testing.T) (CandidateAuthority, CurrentAuthority, time.Time) {
	t.Helper()
	now := time.Unix(1_000, 0).UTC()
	candidate := CandidateAuthority{
		APIVersion: APIVersion, Kind: CandidateAuthorityKind, GroupID: "edge-pool-a",
		RecordDigest: otherDigest, BundleGeneration: "candidate-bundle-7", WorkerSlot: AuthoritySlotB, ReleaseRecordDigest: testDigest,
		State: CandidateAuthorityLoaded, Generation: 7,
	}
	candidate = bindCandidatePromotionWitness(candidate)
	current := CurrentAuthority{
		APIVersion: APIVersion, Kind: CurrentAuthorityKind, GroupID: candidate.GroupID,
		CurrentRecordDigest: testDigest, CurrentWorkerSlot: AuthoritySlotA, AuthorityEpoch: 11,
	}
	return candidate, current, now
}

func candidateWorkerCohortFixture(t *testing.T, candidate CandidateAuthority) CandidateWorkerCohort {
	t.Helper()
	cohort, err := (CandidateWorkerCohort{
		GroupID: candidate.GroupID, WorkerSlot: candidate.WorkerSlot, BundleGeneration: candidate.BundleGeneration,
		WorkerSourceSHA: candidate.WorkerSourceSHA, WorkerImageDigest: candidate.WorkerImageDigest,
		Instances: []CandidateWorkerInstance{{NodeName: "edge-node-a", PodUID: "pod-uid-candidate-a"}},
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	return cohort
}

func routeSamples(candidate CandidateAuthority, now time.Time, healthy, attested bool) []CandidateRouteSample {
	samples := make([]CandidateRouteSample, CandidateCanaryRequiredSamples)
	for index := range samples {
		status, body := 200, testDigest
		if !healthy {
			status, body = 404, otherDigest
		}
		samples[index] = CandidateRouteSample{
			GroupID: candidate.GroupID, AuthorityRecordDigest: candidate.RecordDigest,
			WorkerSlot: candidate.WorkerSlot, ReleaseRecordDigest: candidate.ReleaseRecordDigest,
			Attested:   attested,
			StatusCode: status, BodyDigest: body, ExpectedBodyDigest: testDigest,
			OriginEvidenceDigest: testDigest, ObservedAt: now.Add(time.Duration(index-2) * time.Second).Format(time.RFC3339Nano),
		}
		if attested {
			samples[index].ObservedRecordDigest = candidate.RecordDigest
			samples[index].ObservedReleaseDigest = candidate.ReleaseRecordDigest
			samples[index].ObservedWorkerSlot = candidate.WorkerSlot
		}
	}
	return samples
}

func TestCandidateCanaryRequiresThreeCandidateBoundRouteSuccesses(t *testing.T) {
	candidate, current, now := candidateCanaryFixture(t)
	previous := candidate
	previous.RecordDigest, previous.WorkerSlot = current.CurrentRecordDigest, current.CurrentWorkerSlot
	result, err := EvaluateCandidateCanary(candidate, current, candidateWorkerCohortFixture(t, candidate), routeSamples(candidate, now, true, true), routeSamples(previous, now, true, false), now, 30*time.Second, "candidate-canary-v1", candidateCanaryTestKey)
	if err != nil {
		t.Fatal(err)
	}
	if result.RouteState != HealthHealthy || result.DependencyState != HealthHealthy || result.CandidateRecordDigest != candidate.RecordDigest || result.WorkerSlot != AuthoritySlotB {
		t.Fatalf("candidate canary result is not exact: %+v", result)
	}
	if err := result.Validate(now.Add(29 * time.Second)); err != nil {
		t.Fatal(err)
	}
	publicKey, err := CandidateCanaryPublicKey(candidateCanaryTestKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := result.VerifySignature(publicKey); err != nil {
		t.Fatal(err)
	}
	tampered := result
	tampered.RouteState = HealthDegraded
	if err := tampered.VerifySignature(publicKey); err == nil {
		t.Fatal("tampered candidate canary signature was accepted")
	}
}

func TestCandidateCanaryAcceptsAnExactAttestedPreviousControlAndRejectsReplay(t *testing.T) {
	candidate, current, now := candidateCanaryFixture(t)
	previous := candidate
	previous.RecordDigest, previous.WorkerSlot = current.CurrentRecordDigest, current.CurrentWorkerSlot
	previousSamples := routeSamples(previous, now, true, true)
	for index := range previousSamples {
		previousSamples[index].ObservedReleaseDigest = otherDigest
	}
	if _, err := EvaluateCandidateCanary(candidate, current, candidateWorkerCohortFixture(t, candidate), routeSamples(candidate, now, true, true), previousSamples, now, 30*time.Second, "candidate-canary-v1", candidateCanaryTestKey); err != nil {
		t.Fatalf("exact previous attestation rejected: %v", err)
	}
	for _, mutate := range []func(*CandidateRouteSample){
		func(sample *CandidateRouteSample) { sample.ObservedRecordDigest = candidate.RecordDigest },
		func(sample *CandidateRouteSample) { sample.ObservedWorkerSlot = candidate.WorkerSlot },
		func(sample *CandidateRouteSample) { sample.ObservedReleaseDigest = "bad" },
	} {
		drifted := append([]CandidateRouteSample(nil), previousSamples...)
		mutate(&drifted[0])
		if _, err := EvaluateCandidateCanary(candidate, current, candidateWorkerCohortFixture(t, candidate), routeSamples(candidate, now, true, true), drifted, now, 30*time.Second, "candidate-canary-v1", candidateCanaryTestKey); err == nil {
			t.Fatal("replayed previous attestation was accepted")
		}
	}
}

func TestCandidateCanaryAttributesMissingRouteToCandidateWhenPreviousIsHealthy(t *testing.T) {
	candidate, current, now := candidateCanaryFixture(t)
	previous := candidate
	previous.RecordDigest, previous.WorkerSlot = current.CurrentRecordDigest, current.CurrentWorkerSlot
	result, err := EvaluateCandidateCanary(candidate, current, candidateWorkerCohortFixture(t, candidate), routeSamples(candidate, now, false, true), routeSamples(previous, now, true, false), now, 30*time.Second, "candidate-canary-v1", candidateCanaryTestKey)
	if err != nil {
		t.Fatal(err)
	}
	if result.RouteState != HealthDegraded || result.DependencyState != HealthHealthy {
		t.Fatalf("missing candidate route was misattributed: %+v", result)
	}
}

func TestCandidateCanaryAttributesSharedOriginFailureToDependency(t *testing.T) {
	candidate, current, now := candidateCanaryFixture(t)
	previous := candidate
	previous.RecordDigest, previous.WorkerSlot = current.CurrentRecordDigest, current.CurrentWorkerSlot
	result, err := EvaluateCandidateCanary(candidate, current, candidateWorkerCohortFixture(t, candidate), routeSamples(candidate, now, false, true), routeSamples(previous, now, false, false), now, 30*time.Second, "candidate-canary-v1", candidateCanaryTestKey)
	if err != nil {
		t.Fatal(err)
	}
	if result.RouteState != HealthDegraded || result.DependencyState != HealthDegraded {
		t.Fatalf("shared route failure was misattributed: %+v", result)
	}
}

func TestCandidateCanaryRejectsCrossCandidateReplayAndStaleSamples(t *testing.T) {
	candidate, current, now := candidateCanaryFixture(t)
	previous := candidate
	previous.RecordDigest, previous.WorkerSlot = current.CurrentRecordDigest, current.CurrentWorkerSlot
	cross := routeSamples(candidate, now, true, true)
	cross[1].ObservedRecordDigest = testDigest
	if _, err := EvaluateCandidateCanary(candidate, current, candidateWorkerCohortFixture(t, candidate), cross, routeSamples(previous, now, true, false), now, 30*time.Second, "candidate-canary-v1", candidateCanaryTestKey); err == nil || !strings.Contains(err.Error(), "attestation") {
		t.Fatalf("cross-candidate result was accepted: %v", err)
	}
	stale := routeSamples(candidate, now, true, true)
	stale[0].ObservedAt = now.Add(-11 * time.Second).Format(time.RFC3339Nano)
	if _, err := EvaluateCandidateCanary(candidate, current, candidateWorkerCohortFixture(t, candidate), stale, routeSamples(previous, now, true, false), now, 30*time.Second, "candidate-canary-v1", candidateCanaryTestKey); err == nil || !strings.Contains(err.Error(), "window") {
		t.Fatalf("stale candidate sample was accepted: %v", err)
	}
}

func TestCandidateCanaryCannotEvaluateCurrentSlotOrUnloadedCandidate(t *testing.T) {
	candidate, current, now := candidateCanaryFixture(t)
	previous := candidate
	previous.RecordDigest, previous.WorkerSlot = current.CurrentRecordDigest, current.CurrentWorkerSlot
	candidate.WorkerSlot = current.CurrentWorkerSlot
	if _, err := EvaluateCandidateCanary(candidate, current, candidateWorkerCohortFixture(t, candidate), routeSamples(candidate, now, true, true), routeSamples(previous, now, true, false), now, 30*time.Second, "candidate-canary-v1", candidateCanaryTestKey); err == nil {
		t.Fatal("current slot was accepted as a candidate")
	}
	legacy, current, now := candidateCanaryFixture(t)
	legacy.AuthoritySequence, legacy.CandidateSequence = 0, 0
	previous = legacy
	previous.RecordDigest, previous.WorkerSlot = current.CurrentRecordDigest, current.CurrentWorkerSlot
	if _, err := EvaluateCandidateCanary(legacy, current, candidateWorkerCohortFixture(t, legacy), routeSamples(legacy, now, true, true), routeSamples(previous, now, true, false), now, 30*time.Second, "candidate-canary-v1", candidateCanaryTestKey); err == nil {
		t.Fatal("legacy candidate without promotion witness was accepted")
	}
}

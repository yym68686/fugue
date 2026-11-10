package platformproducer

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func testCaptureDeferral() (CaptureDeferralRequest, CaptureDeferralObservation) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	request := CaptureDeferralRequest{SchemaVersion: CaptureDeferralSchema, OperationID: "control-a", RequestedAt: at, ExpiresAt: at.Add(MaxCaptureDeferral)}
	observation := CaptureDeferralObservation{At: at, Authorities: map[string]model.PlatformPublicationPrecondition{}, InFlight: map[string]uint64{}}
	for i, scope := range []string{Scope, Scope + ":cell-alpha"} {
		policy := model.PlatformPublicationPrecondition{ArtifactID: "policy-" + scope, ContentHash: "sha256:" + strings.Repeat("a", 64), ReleaseID: "release-" + scope, FencingToken: int64(i + 1)}
		request.Authorities = append(request.Authorities, CaptureDeferralAuthority{Scope: scope, Policy: policy})
		observation.Authorities[scope], observation.InFlight[scope] = policy, 0
	}
	return request, observation
}

func restartCaptureDeferral(t *testing.T, record CaptureDeferralRecord) CaptureDeferralRecord {
	t.Helper()
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var restored CaptureDeferralRecord
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	return restored
}

func TestCaptureDeferralDrainRestartAndAbsoluteExpiry(t *testing.T) {
	request, observation := testCaptureDeferral()
	observation.InFlight[Scope] = 1
	record, err := PrepareCaptureDeferral(request, nil, observation)
	if err != nil || record.Phase != "draining" || record.ActivatedAt != nil {
		t.Fatal("failed to reserve bounded drain", record, err)
	}
	for scope, policy := range observation.Authorities {
		if deferNew, err := record.DefersNewInput(scope, policy, observation.At); err != nil || !deferNew {
			t.Fatal("drain admitted more new input", scope, err)
		}
	}
	// Existing admitted work is left alone. Completion is an observation from
	// the adapter; the state machine cannot cancel or claim its result.
	observation.At = request.ExpiresAt.Add(-time.Second)
	observation.InFlight[Scope] = 0
	record, err = AdvanceCaptureDeferral(restartCaptureDeferral(t, record), observation)
	if err != nil || record.Phase != "active" || !record.ActivatedAt.Equal(observation.At) || !record.Request.ExpiresAt.Equal(request.ExpiresAt) {
		t.Fatal("activation reset deadline or failed after restart", record, err)
	}
	if deferred, err := record.DefersNewInput(Scope, observation.Authorities[Scope], request.ExpiresAt); err != nil || deferred {
		t.Fatal("stale active record overran absolute expiry", err)
	}
	observation.At = request.ExpiresAt
	record, err = AdvanceCaptureDeferral(restartCaptureDeferral(t, record), observation)
	if err != nil || record.Phase != "expired" {
		t.Fatal("exact deadline was not terminal", record, err)
	}
	// Terminal tombstones survive restart and even a clock moving backward.
	observation.At = request.RequestedAt
	record, err = AdvanceCaptureDeferral(restartCaptureDeferral(t, record), observation)
	if err != nil || record.Phase != "expired" {
		t.Fatal("expired request reactivated", err)
	}
	if deferred, err := record.DefersNewInput(Scope, observation.Authorities[Scope], observation.At); err != nil || deferred {
		t.Fatal("expiry retained deferral", err)
	}
	if _, err := PrepareCaptureDeferral(request, &record, observation); err == nil {
		t.Fatal("same durable operation was replayed")
	}
}

func TestCaptureDeferralRejectsIncompleteOrUnboundAdmission(t *testing.T) {
	for _, scenario := range []string{"missing-authority", "changed-artifact", "changed-release", "changed-hash", "changed-fence", "missing-count", "duplicate-scope", "unknown-scope", "too-long", "zero-window", "not-yet-valid", "already-expired", "zero-time", "too-many-scopes"} {
		t.Run(scenario, func(t *testing.T) {
			request, observation := testCaptureDeferral()
			policy := observation.Authorities[Scope]
			switch scenario {
			case "missing-authority":
				delete(observation.Authorities, Scope)
			case "changed-artifact":
				policy.ArtifactID += "-other"
			case "changed-release":
				policy.ReleaseID += "-other"
			case "changed-hash":
				policy.ContentHash = "sha256:" + strings.Repeat("b", 64)
			case "changed-fence":
				policy.FencingToken++
			case "missing-count":
				delete(observation.InFlight, Scope)
			case "duplicate-scope":
				request.Authorities = append(request.Authorities, request.Authorities[0])
			case "unknown-scope":
				request.Authorities[0].Scope = "other-producer"
			case "too-long":
				request.ExpiresAt = request.ExpiresAt.Add(time.Nanosecond)
			case "zero-window":
				request.ExpiresAt = request.RequestedAt
			case "not-yet-valid":
				observation.At = request.RequestedAt.Add(-time.Nanosecond)
			case "already-expired":
				observation.At = request.ExpiresAt
			case "zero-time":
				request.RequestedAt = time.Time{}
			case "too-many-scopes":
				request.Authorities = make([]CaptureDeferralAuthority, maxDeferredScopes+1)
			}
			if strings.HasPrefix(scenario, "changed-") {
				observation.Authorities[Scope] = policy
			}
			if _, err := PrepareCaptureDeferral(request, nil, observation); err == nil {
				t.Fatal("accepted unsafe admission", scenario)
			}
		})
	}
}

func TestCaptureDeferralScopeIsolationReleaseAndDrift(t *testing.T) {
	for _, scenario := range []string{"release", "authority-drift", "clock-regression", "missing-observation", "drain-expiry"} {
		t.Run(scenario, func(t *testing.T) {
			request, observation := testCaptureDeferral()
			if scenario == "drain-expiry" {
				observation.InFlight[Scope] = 1
			}
			record, err := PrepareCaptureDeferral(request, nil, observation)
			if err != nil {
				t.Fatal(err)
			}
			if deferred, err := record.DefersNewInput(Scope+":cell-unrelated", observation.Authorities[Scope], observation.At); err != nil || deferred {
				t.Fatal("control leaked into another scope", err)
			}
			observation.At = observation.At.Add(time.Second)
			switch scenario {
			case "release":
				record, err = ReleaseCaptureDeferral(record, observation.At)
			case "authority-drift":
				changed := observation.Authorities[Scope]
				changed.FencingToken++
				observation.Authorities[Scope] = changed
				record, err = AdvanceCaptureDeferral(record, observation)
			case "clock-regression":
				observation.At = request.RequestedAt.Add(-time.Second)
				record, err = AdvanceCaptureDeferral(record, observation)
			case "missing-observation":
				delete(observation.InFlight, Scope)
				record, err = AdvanceCaptureDeferral(record, observation)
			case "drain-expiry":
				observation.At = request.ExpiresAt
				record, err = AdvanceCaptureDeferral(record, observation)
			}
			if err != nil || record.EndedAt == nil || !record.Request.ExpiresAt.Equal(request.ExpiresAt) {
				t.Fatal("control was renewed or did not exit", record, err)
			}
			if deferred, err := record.DefersNewInput(Scope, request.Authorities[0].Policy, observation.At); err != nil || deferred {
				t.Fatal("ended control still applies", err)
			}
			observation.At = request.RequestedAt.Add(2 * time.Second)
			if next, err := AdvanceCaptureDeferral(restartCaptureDeferral(t, record), observation); err != nil || next.Phase != record.Phase || !next.EndedAt.Equal(*record.EndedAt) {
				t.Fatal("terminal control changed after restart", next, err)
			}
		})
	}
}

func TestCaptureDeferralImmutableRequestAndFaultRefusal(t *testing.T) {
	request, observation := testCaptureDeferral()
	record, err := PrepareCaptureDeferral(request, nil, observation)
	if err != nil {
		t.Fatal(err)
	}
	request.Authorities[0].Policy.FencingToken++
	if deferred, err := record.DefersNewInput(Scope, observation.Authorities[Scope], observation.At); err != nil || !deferred {
		t.Fatal("caller changed stored binding through alias", err)
	}
	observation.At = observation.At.Add(time.Second)
	observation.InFlight[Scope] = 1
	if _, err := AdvanceCaptureDeferral(record, observation); err == nil {
		t.Fatal("admission barrier violation was hidden")
	}
	changed := restartCaptureDeferral(t, record)
	changed.Request.ExpiresAt = changed.Request.ExpiresAt.Add(time.Second)
	if _, err := AdvanceCaptureDeferral(changed, observation); err == nil {
		t.Fatal("deadline tampering was accepted")
	}
	changed = restartCaptureDeferral(t, record)
	changed.Phase = "unknown"
	if _, err := changed.DefersNewInput(Scope, observation.Authorities[Scope], observation.At); err == nil {
		t.Fatal("unknown phase accepted")
	}
	if _, err := ReleaseCaptureDeferral(record, record.ObservedAt.Add(-time.Second)); err == nil {
		t.Fatal("regressed release timestamp accepted")
	}
}

func TestCaptureDeferralCanonicalBindingAndAdmissionClock(t *testing.T) {
	request, observation := testCaptureDeferral()
	original, err := captureDeferralDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Authorities[0], request.Authorities[1] = request.Authorities[1], request.Authorities[0]
	request.RequestedAt = request.RequestedAt.In(time.FixedZone("offset", 3600))
	request.ExpiresAt = request.ExpiresAt.In(time.FixedZone("offset", 3600))
	if reordered, err := captureDeferralDigest(request); err != nil || reordered != original {
		t.Fatal("equivalent binding produced another request identity", err)
	}
	record, err := PrepareCaptureDeferral(request, nil, observation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := record.DefersNewInput(Scope, observation.Authorities[Scope], observation.At.Add(-time.Nanosecond)); err == nil {
		t.Fatal("admission clock regression was hidden")
	}
	changed := observation.Authorities[Scope]
	changed.FencingToken++
	if _, err := record.DefersNewInput(Scope, changed, observation.At); err == nil {
		t.Fatal("admission skipped the exact authority binding")
	}
}

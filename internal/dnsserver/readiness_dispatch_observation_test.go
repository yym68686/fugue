package dnsserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

func TestDNSProbeDispatchDistinguishesObservedWorkFromUndispatched(t *testing.T) {
	plan := &platformconfig.DNSReadinessPlan{}
	for i := 0; i < dnsProbeDispatchLimit+2; i++ {
		plan.Probes = append(plan.Probes, platformconfig.DNSReadinessProbe{ID: fmt.Sprintf("requirement-%d", i), Address: "192.0.2.1", Hostname: "private.example.test", Path: fmt.Sprintf("/private/%d", i)})
	}
	policy := &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, MaxConcurrency: 1}
	file := filepath.Join(t.TempDir(), "dns")
	var journal bytes.Buffer
	scheduler := (&dnsReadinessScheduler{}).bind(model.PlatformConsumerAssignment{FencingToken: 1}, log.New(&journal, "", 0), file)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var attempts int
	const confirmedExpiry = 45 * time.Second
	var observedUntil time.Time
	facts := scheduler.collectWithEvaluator(ctx, plan, policy, func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		attempts++
		if attempts == 2 {
			cancel()
			return routeprobe.Proof{}, errors.New("private error")
		}
		return routeprobe.Proof{}, nil
	}, func(i int, _ routeprobe.Proof, err error, now time.Time) dnsReadinessFact {
		fact := dnsReadinessFact{ProbeID: plan.Probes[i].ID}
		if err == nil {
			observedUntil = now.Add(confirmedExpiry)
			fact.Ready, fact.Proof.ValidUntil = true, observedUntil
		} else {
			fact.Reason = "probe_failed"
		}
		return fact
	})
	if attempts != 2 || !facts[0].Ready || facts[1].Reason != "probe_failed" || facts[2].Reason != "not_observed" {
		t.Fatalf("observation changed calls or returned facts: attempts=%d", attempts)
	}
	raw, err := os.ReadFile(file + ".probe-observations.json")
	if err != nil || bytes.Contains(raw, []byte("private")) || bytes.Contains(raw, []byte("requirement-")) || bytes.Contains(journal.Bytes(), []byte("probe_dispatch")) {
		t.Fatalf("invalid private observation or journal leakage: %v", err)
	}
	var rows []struct {
		ID       string                   `json:"observation_id"`
		Dispatch dnsProbeDispatchSnapshot `json:"probe_dispatch"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil || len(rows) != 1 {
		t.Fatalf("invalid sweep observation: %v", err)
	}
	row := rows[0]
	if row.ID != facts[0].ObservationID || row.Dispatch.StopState != "canceled" || row.Dispatch.Omitted != 2 || len(row.Dispatch.Samples) != dnsProbeDispatchLimit {
		t.Fatalf("unexpected dispatch sample coverage: %+v", row.Dispatch)
	}
	for _, sample := range row.Dispatch.Samples {
		if sample.Result != "not_dispatched" || !sample.DispatchedAt.IsZero() || !sample.FinishedAt.IsZero() || sample.QueuePosition <= 2 {
			t.Fatalf("sample claimed an unobserved probe ran: %+v", sample)
		}
	}

	// Leave both outcomes in the sample, preceding ordinary unobserved work.
	var now = time.Now().UTC()
	timing := make([]dnsProbeDispatchTiming, len(facts))
	for i := 0; i < 2; i++ {
		timing[i] = dnsProbeDispatchTiming{previousExpiry: now.Add(time.Duration(i) * time.Second), dispatched: now.Add(-3 * time.Second), started: now.Add(-2 * time.Second), finished: now.Add(-time.Second), priority: i + 1}
	}
	queue := dnsProbeTargetQueue{address: plan.Probes[0].Address, next: 2}
	for i := range facts {
		queue.indices = append(queue.indices, i)
	}
	scheduler.targets[queue.address] = dnsProbeTargetState{retryAt: now.Add(time.Minute)}
	details := scheduler.probeDispatchObservation(plan, facts, timing, []dnsProbeTargetQueue{queue}, now, context.DeadlineExceeded)
	if details.StopState != "deadline" || details.Samples[0].Result != "scheduler_proof_observed" || details.Samples[1].Result != "probe_rejected" || details.Samples[2].Result != "not_dispatched" {
		t.Fatalf("outcomes were mixed: %+v", details.Samples[:3])
	}
	sample := details.Samples[0]
	requirement := sha256.Sum256([]byte(plan.Probes[0].ID))
	physical := sha256.Sum256([]byte(dnsroutesource.ProbeKey(plan.Probes[0])))
	if sample.RequirementDigest != fmt.Sprintf("%x", requirement[:12]) || sample.PhysicalDigest != fmt.Sprintf("%x", physical[:12]) ||
		sample.QueuePosition != 1 || sample.CandidatePriority != 1 || !sample.ObservedProofUntil.Equal(observedUntil) ||
		!sample.DispatchedAt.Equal(timing[0].dispatched) || !sample.StartedAt.Equal(timing[0].started) || !sample.FinishedAt.Equal(timing[0].finished) ||
		!details.Samples[2].RetryAtStop.After(details.StoppedAt) || !details.Samples[2].ObservedProofUntil.IsZero() {
		t.Fatalf("dispatch, expiry or identity mismatch: %+v", sample)
	}
}

func TestDNSProbeObservationEvictsOldRowsBeforeDroppingNewest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "observations.json")
	scheduler := &dnsReadinessScheduler{path: path}
	for i := 0; i < 8; i++ {
		buf, err := json.Marshal(struct {
			Serial  int
			Padding string
		}{i, string(bytes.Repeat([]byte("a"), 32<<10))})
		if err != nil {
			t.Fatal(err)
		}
		scheduler.saveObservation(buf)
	}
	raw, err := os.ReadFile(path)
	var rows []struct{ Serial int }
	if err != nil || len(raw) > 128<<10 || json.Unmarshal(raw, &rows) != nil || len(rows) != 3 || rows[0].Serial != 5 || rows[2].Serial != 7 {
		t.Fatalf("newest bounded rows missing: %+v, %v", rows, err)
	}
}

func TestDNSProbeDispatchRecordsBackoffWithoutDeadline(t *testing.T) {
	plan := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{
		{ID: "first", Address: "192.0.2.1"}, {ID: "second", Address: "192.0.2.1"},
	}}
	policy := &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, MaxConcurrency: 1}
	file := filepath.Join(t.TempDir(), "dns")
	scheduler := (&dnsReadinessScheduler{}).bind(model.PlatformConsumerAssignment{FencingToken: 1}, nil, file)
	attempts := 0
	facts := scheduler.collect(t.Context(), plan, policy, func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
		attempts++
		return routeprobe.Proof{}, errors.Join(routeprobe.ErrConnectionUnavailable, routeprobe.ErrUnavailable)
	})
	if attempts != 1 || facts[0].Reason != "probe_unavailable" || facts[1].Reason != "not_observed" {
		t.Fatalf("backoff behavior changed: attempts=%d", attempts)
	}
	raw, err := os.ReadFile(file + ".probe-observations.json")
	var rows []struct {
		BudgetExhausted bool                     `json:"budget_exhausted"`
		Dispatch        dnsProbeDispatchSnapshot `json:"probe_dispatch"`
	}
	if err != nil || json.Unmarshal(raw, &rows) != nil || len(rows) != 1 || rows[0].BudgetExhausted || rows[0].Dispatch.StopState != "no_eligible_dispatch" || len(rows[0].Dispatch.Samples) != 2 {
		t.Fatalf("backoff attributed to another cause: %+v, %v", rows, err)
	}
	dispatch := rows[0].Dispatch
	miss, failed := dispatch.Samples[0], dispatch.Samples[1]
	if miss.Result != "not_dispatched" || !miss.DispatchedAt.IsZero() || miss.QueuePosition != 2 || !miss.RetryAtStop.After(dispatch.StoppedAt) ||
		failed.Result != "probe_unavailable" || failed.DispatchedAt.IsZero() || failed.FinishedAt.IsZero() || failed.QueuePosition != 1 {
		t.Fatalf("wrong backoff evidence: %+v", dispatch)
	}
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPreparedWorkerPublicationRequiresAppliedNonCandidate(t *testing.T) {
	target := releaseguardian.FrontAuthorityTarget{GroupID: "edge-group-fixture", FrontBundleGeneration: "config.p7.r4"}
	base := authorityWorkerHealth{Status: "ok", Healthy: true, EdgeGroupID: target.GroupID, BundleVersion: target.FrontBundleGeneration, ServingGeneration: "config", PublicationSequence: 7, CaddyAppliedVersion: target.FrontBundleGeneration}
	if !preparedWorkerPublicationReady(base, target) {
		t.Fatal("exact applied publication rejected")
	}
	for name, change := range map[string]func(*authorityWorkerHealth){
		"candidate":   func(h *authorityWorkerHealth) { h.CandidateBundleLoaded = true },
		"unapplied":   func(h *authorityWorkerHealth) { h.CaddyAppliedVersion = "" },
		"old Caddy":   func(h *authorityWorkerHealth) { h.CaddyAppliedVersion = "old.p6.r4" },
		"unhealthy":   func(h *authorityWorkerHealth) { h.Healthy = false },
		"other group": func(h *authorityWorkerHealth) { h.EdgeGroupID = "edge-group-other" },
		"recovery regression": func(h *authorityWorkerHealth) {
			h.BundleVersion = "config.p7.r3"
			h.CaddyAppliedVersion = h.BundleVersion
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := base
			change(&h)
			if preparedWorkerPublicationReady(h, target) {
				t.Fatal("unproven target may activate")
			}
		})
	}
}

type incompletePreparationExecutor struct {
	calls  int
	states map[string]edgegroupfront.ActivationState
	target releaseguardian.FrontAuthorityTarget
	now    time.Time
	t      *testing.T
}

func (e *incompletePreparationExecutor) Exec(_ context.Context, _, pod, _ string, args ...string) ([]byte, error) {
	e.calls++
	if !strings.Contains(strings.Join(args, " "), "--prepare-publication") {
		e.t.Fatal("preparation performed traffic activation")
	}
	if e.calls == 2 {
		return nil, errors.New("second target unavailable")
	}
	p := edgegroupfront.PublicationPreparation{Schema: edgegroupfront.PublicationPreparationSchema, Previous: e.states[pod], TargetSlot: "b", BundleGeneration: e.target.FrontBundleGeneration, WorkerSourceCommit: e.target.WorkerSourceSHA, WorkerImageDigest: e.target.WorkerImageDigest, CreatedAt: e.now, ExpiresAt: e.now.Add(edgegroupfront.PublicationPreparationLifetime)}
	return json.Marshal(p)
}

func TestPublicationPreparationIncompleteCohortNeverActivates(t *testing.T) {
	now := time.Now().UTC()
	source := strings.Repeat("a", 40)
	image := "sha256:" + strings.Repeat("b", 64)
	previous := edgegroupfront.ActivationState{Schema: edgegroupfront.ActivationStateSchemaV1, GroupID: "edge-group-fixture", Generation: 7, ActiveSlot: "a", BundleGeneration: "old.p7.r4", WorkerSourceCommit: source, WorkerImageDigest: image, Authority: edgegroupfront.ActivationAuthority, Operation: edgegroupfront.ActivationOperationPromote, Reason: "prior verified traffic publication", UpdatedAt: now}
	target := releaseguardian.FrontAuthorityTarget{GroupID: previous.GroupID, TargetSlot: releaseguardian.AuthoritySlotB, FrontBundleGeneration: "new.p8.r4", WorkerSourceSHA: strings.Repeat("c", 40), WorkerImageDigest: "sha256:" + strings.Repeat("d", 64)}
	e := &incompletePreparationExecutor{states: map[string]edgegroupfront.ActivationState{"worker-1": previous, "worker-2": previous}, target: target, now: now, t: t}
	a := &frontAuthorityActivator{executor: e, config: frontAuthorityConfig{Namespace: "fixture"}, now: func() time.Time { return now }}
	before := frontAuthorityPreflight{workers: map[string]corev1.Pod{"node-1": {ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}}, "node-2": {ObjectMeta: metav1.ObjectMeta{Name: "worker-2"}}}, states: map[string]edgegroupfront.ActivationState{"node-1": previous, "node-2": previous}}
	if err := a.prepareWorkerPublications(context.Background(), target, before); err == nil || e.calls != 2 {
		t.Fatalf("incomplete cohort accepted: calls=%d err=%v", e.calls, err)
	}
}

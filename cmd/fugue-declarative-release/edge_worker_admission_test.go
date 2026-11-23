package main

import (
	"errors"
	"testing"

	"fugue/internal/declarativerelease"
)

func TestInactiveWorkersCanStartFromMixedSlots(t *testing.T) {
	target := declarativerelease.TargetIdentity{ConfigSHA: "next", ImageRef: "image-next"}
	for _, tc := range []struct {
		name                  string
		second                edgeGroupPod
		secondError           error
		wantWait, wantFailure bool
		wantChecks            int
	}{
		{"only blocked replacements", edgeGroupPod{NodeName: "b", SourceCommit: "next", ImageRef: "image-next"}, nil, true, false, 1},
		{"independent rolled back member", edgeGroupPod{NodeName: "b"}, nil, false, false, 2},
		{"unknown is not a confirmed drain", edgeGroupPod{NodeName: "b"}, errors.New("identity changed"), false, true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			err := inactiveWorkersCanStart(map[string]edgeGroupPod{"a": {NodeName: "a"}, "b": tc.second}, target, func(p edgeGroupPod) error {
				calls++
				if p.NodeName == "a" {
					return declarativerelease.ErrInactiveWorkerDraining
				}
				return tc.secondError
			})
			if calls != tc.wantChecks || (err != nil) != (tc.wantFailure || tc.wantWait) ||
				errors.Is(err, declarativerelease.ErrInactiveWorkerDraining) != tc.wantWait {
				t.Fatalf("calls %d, error %v", calls, err)
			}
		})
	}
}

func TestWorkerWaitRepreparePreservesInputs(t *testing.T) {
	p := declarativerelease.ExecutionPlan{PlanDigest: "original", Prewrite: declarativerelease.Observation{UID: "ds", Generation: 2, ResourceVersion: "1", Resources: []declarativerelease.ResourceObservation{{UID: "worker", Generation: 2, ObjectDigest: "spec"}}}}
	fresh := p
	fresh.PlanDigest = "fresh"
	fresh.Prewrite.ResourceVersion = "5"
	if !sameWorkerWaitInputs(p, fresh) {
		t.Fatal("fresh prewrite rejected status-only movement")
	}
	for _, change := range []func(*declarativerelease.ExecutionPlan){
		func(next *declarativerelease.ExecutionPlan) {
			next.Prewrite.Resources = []declarativerelease.ResourceObservation{{UID: "replacement"}}
		},
		func(next *declarativerelease.ExecutionPlan) {
			next.Prewrite.Participation = &declarativerelease.NodeParticipation{WorkloadUID: "other"}
		},
		func(next *declarativerelease.ExecutionPlan) { next.LKG.ImageRef = "different" },
		func(next *declarativerelease.ExecutionPlan) { next.DegradedPredecessor = true },
	} {
		next := fresh
		change(&next)
		if sameWorkerWaitInputs(p, next) {
			t.Fatal("drifted scope accepted")
		}
	}
}

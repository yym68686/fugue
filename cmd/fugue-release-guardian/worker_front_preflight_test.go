package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	dr "fugue/internal/declarativerelease"
)

type observationTestReader struct{ calls int }

func (r *observationTestReader) Exec(context.Context, string, string, string, ...string) ([]byte, error) {
	r.calls++
	return []byte("record"), nil
}

func TestWorkerObservationOnlyReadsOneDeclaredFile(t *testing.T) {
	d := &observationTestReader{}
	r := workerObservationPodReader{namespace: "fixture-system", delegate: d}
	for _, tc := range []struct {
		ns, container string
		args          []string
	}{
		{"other", "edge", []string{"cat", frontActivationStatePath}},
		{"fixture-system", "caddy", []string{"cat", frontActivationStatePath}},
		{"fixture-system", "edge", []string{"cat", "/other-file"}},
		{"fixture-system", "edge", []string{"cat", frontActivationStatePath, "-"}},
		{"fixture-system", "edge", []string{frontActivationCASBinary, "--prepare-publication"}},
	} {
		if _, err := r.Exec(context.Background(), tc.ns, "fixture-worker", tc.container, tc.args...); err == nil {
			t.Fatal("observer allowed unrelated command")
		}
	}
	if d.calls != 0 {
		t.Fatal("observer passed a rejected command to Kubernetes")
	}
	if data, err := r.Exec(context.Background(), r.namespace, "fixture-worker", "edge", "cat", frontActivationStatePath); err != nil || string(data) != "record" || d.calls != 1 {
		t.Fatal("exact read failed", err)
	}
}

func TestWorkerFrontPreflightBindsCandidateWithoutGrantingTraffic(t *testing.T) {
	b, n, w := partialFrontRollbackFixture()
	p, err := planPartialFrontRollbackRepair(b, "authority-uid", "23", n, w)
	if err != nil {
		t.Fatal(err)
	}
	digest := func(s string) string { return "sha256:" + strings.Repeat(s, 64) }
	component := dr.Component{ID: "edge-worker-fixture", IntentPath: "worker/intent.json", ManifestPath: "worker/resources.json", Concurrency: "fugue-production-edge-worker-fixture",
		Workload: dr.Workload{Namespace: "fixture-system"}, Artifact: dr.Artifact{Repository: "ghcr.io/example/worker"},
		Transition: &dr.Transition{Type: "edge-group-ab", EdgeGroupAB: &dr.EdgeGroupABTransition{GroupID: b.GroupID}}, Delivery: &dr.Delivery{Writer: "guardian"}}
	sel := frontRepairSelection{worker: component, runtime: frontAuthorityConfig{GroupID: b.GroupID}}
	h := frontRepairPreview{Schema: frontRepairPreviewSchema, Plan: p, Release: frontRepairReleaseBinding{Component: component.ID, Namespace: component.Workload.Namespace, GroupID: b.GroupID, PlanDigest: digest("1"), MonitorDigest: digest("2"), ArtifactDigest: digest("3"), WorkerSourceSHA: b.CurrentWorkerSourceSHA, WorkerImageDigest: b.CurrentWorkerImageDigest}}
	h.Digest, err = h.validatedDigest()
	if err != nil {
		t.Fatal(err)
	}
	r := dr.PlanRelease{ComponentID: component.ID, IntentPath: component.IntentPath, ManifestPath: component.ManifestPath, Concurrency: component.Concurrency, Workload: component.Workload, Transition: component.Transition, Delivery: component.Delivery, Artifact: component.Artifact, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: b.CurrentWorkerSourceSHA, ExpectedPreviousManifestSHA: b.CurrentWorkerSourceSHA, ExpectedPreviousOCIRevision: b.CurrentWorkerSourceSHA, ExpectedPreviousImageDigest: b.CurrentWorkerImageDigest, IntentGeneration: 3, IntentDigest: digest("4")}
	base := dr.Plan{HeadSHA: strings.Repeat("9", 40), PlanDigest: digest("5")}
	a := dr.ArtifactReceipt{Component: r.ComponentID, ConfigSHA: base.HeadSHA, OCIRevision: base.HeadSHA, Repository: r.Artifact.Repository, TopDigest: digest("8"), ReceiptDigest: digest("6"), PlanDigest: base.PlanDigest, IntentDigest: r.IntentDigest}
	for _, mode := range []string{"exact", "absent", "config", "manifest", "source", "image", "intent", "receipt", "cohort", "group", "extra", "target", "retry", "delivery", "same-code"} {
		t.Run(mode, func(t *testing.T) {
			x, y, z, prev := base, a, r, h
			switch mode {
			case "absent":
				z.ExpectedPreviousPresent = false
			case "config":
				z.ExpectedPreviousConfigSHA = ""
			case "manifest":
				z.ExpectedPreviousManifestSHA = ""
			case "source":
				z.ExpectedPreviousOCIRevision = ""
			case "image":
				z.ExpectedPreviousImageDigest = digest("f")
			case "intent":
				z.IntentPath = "other"
			case "receipt":
				y.PlanDigest = digest("f")
			case "cohort":
				prev.Plan.TargetGeneration++
			case "group":
				prev.Release.GroupID = "other"
			case "target":
				y.ConfigSHA = strings.Repeat("2", 40)
			case "retry":
				z.RetrySameLKG = true
			case "delivery":
				z.Delivery = &dr.Delivery{Writer: "other"}
			case "same-code":
				y.TopDigest = b.CurrentWorkerImageDigest
			}
			x.Releases = []dr.PlanRelease{z}
			if mode == "extra" {
				x.Releases = append(x.Releases, z)
			}
			result, err := bindWorkerFrontPreflight(sel, x, y, prev)
			if mode != "exact" {
				if err == nil {
					t.Fatal("accepted", mode)
				}
				return
			}
			if err != nil || result.AuthorizesTraffic || result.RequirePreparation < 1 || result.Members != len(n) || result.ProposedGeneration != b.CurrentFrontGeneration+2 || result.TargetReceipt != y.ReceiptDigest {
				t.Fatalf("unexpected diagnostic: %+v %v", result, err)
			}
		})
	}
	for _, args := range [][]string{nil, {"--apply"}, {"--plan", "foo"}, {"--receipt", "bar"}} {
		var out bytes.Buffer
		if runWorkerFrontPreflight(args, &out) == nil || out.Len() != 0 {
			t.Fatalf("accepted incomplete/mutating flags %v", args)
		}
	}
}

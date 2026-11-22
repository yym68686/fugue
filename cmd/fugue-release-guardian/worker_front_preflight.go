package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"reflect"
	"time"

	"fugue/internal/declarativerelease"
)

// This is a diagnostic for a planned Worker upgrade, not a repair approval.
// In particular, it may describe a stale inactive slot. It must not select it.
type workerFrontPreflight struct {
	Schema             string `json:"schema"`
	Component          string `json:"component"`
	TargetCommit       string `json:"target_commit"`
	TargetImageDigest  string `json:"target_image_digest"`
	TargetPlan         string `json:"target_plan_digest"`
	TargetReceipt      string `json:"target_receipt_digest"`
	PredecessorMonitor string `json:"predecessor_monitor_digest"`
	CohortDigest       string `json:"cohort_digest"`
	Members            int    `json:"members"`
	RequirePreparation int    `json:"require_preparation"`
	ProposedGeneration uint64 `json:"proposed_generation"`
	AuthorizesTraffic  bool   `json:"authorizes_traffic"`
}

// Plan and receipt are decoded using the production release validators.
// Restrict this preflight to a *single* forward Worker intent whose exact
// predecessor is the immutable Worker release and whose cohort is known.
func bindWorkerFrontPreflight(selection frontRepairSelection, plan declarativerelease.Plan, receipt declarativerelease.ArtifactReceipt, preview frontRepairPreview) (workerFrontPreflight, error) {
	reject := errors.New("Worker plan, built artifact, predecessor and physical cohort do not form one forward upgrade")
	if len(plan.Releases) != 1 {
		return workerFrontPreflight{}, reject
	}
	r := plan.Releases[0]
	w := selection.worker
	p := preview.Release
	d, err := preview.validatedDigest()
	if err != nil || d != preview.Digest || p.Component != w.ID || p.Namespace != w.Workload.Namespace || p.GroupID != selection.runtime.GroupID ||
		w.Transition == nil || w.Transition.Type != "edge-group-ab" || w.Transition.EdgeGroupAB == nil || w.Transition.EdgeGroupAB.GroupID != p.GroupID ||
		w.Delivery == nil || w.Delivery.Writer != "guardian" ||
		!r.ExpectedPreviousPresent || r.ExpectedPreviousConfigSHA != p.WorkerSourceSHA || r.ExpectedPreviousManifestSHA != p.WorkerSourceSHA ||
		r.ExpectedPreviousOCIRevision != p.WorkerSourceSHA || r.ExpectedPreviousImageDigest != p.WorkerImageDigest ||
		r.ComponentID != w.ID || r.IntentPath != w.IntentPath || r.ManifestPath != w.ManifestPath || r.Concurrency != w.Concurrency ||
		r.Workload != w.Workload || r.Artifact != w.Artifact || !reflect.DeepEqual(r.Transition, w.Transition) || !reflect.DeepEqual(r.Delivery, w.Delivery) ||
		!reflect.DeepEqual(r.ManifestVariables, w.ManifestVariables) || !reflect.DeepEqual(r.ArtifactTargets, w.ArtifactTargets) ||
		r.RetrySameLKG || len(r.RuntimeResourcesFromForward) != 0 || r.IntentGeneration < 1 ||
		receipt.Component != w.ID || receipt.ConfigSHA != plan.HeadSHA || receipt.OCIRevision != plan.HeadSHA ||
		receipt.PlanDigest != plan.PlanDigest || receipt.IntentDigest != r.IntentDigest || receipt.Repository != w.Artifact.Repository ||
		receipt.TopDigest == p.WorkerImageDigest || plan.HeadSHA == p.WorkerSourceSHA {
		return workerFrontPreflight{}, reject
	}
	preps, err := frontRepairPublicationRequests(preview.Plan)
	if err != nil || len(preps) == 0 || len(preps) >= len(preview.Plan.Members) {
		return workerFrontPreflight{}, reject
	}
	return workerFrontPreflight{Schema: "worker-front-preflight/v1", Component: w.ID, TargetCommit: plan.HeadSHA,
		TargetImageDigest: receipt.TopDigest, TargetPlan: plan.PlanDigest, TargetReceipt: receipt.ReceiptDigest,
		PredecessorMonitor: p.MonitorDigest, CohortDigest: preview.Plan.Digest, Members: len(preview.Plan.Members),
		RequirePreparation: len(preps), ProposedGeneration: preview.Plan.TargetGeneration}, nil
}

func runWorkerFrontPreflight(args []string, out io.Writer) error {
	f := flag.NewFlagSet("worker-front-preflight", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	pp, ap := f.String("plan", "", "production release plan"), f.String("receipt", "", "Worker artifact receipt")
	if err := f.Parse(args); err != nil || f.NArg() != 0 || f.NFlag() != 2 || *pp == "" || *ap == "" || out == nil {
		return errors.New("worker-front-preflight needs a plan and artifact receipt; it cannot apply changes")
	}
	// Bound even local inputs before invoking the normal strict decoders.
	read := func(path string, decode func(io.Reader) error) error {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > 2<<20 {
			return errors.New("preflight input is not a bounded regular file")
		}
		fd, err := os.Open(path)
		if err != nil {
			return err
		}
		defer fd.Close()
		limited := &io.LimitedReader{R: fd, N: (2 << 20) + 1}
		err = decode(limited)
		if limited.N == 0 {
			return errors.New("oversized preflight input")
		}
		return err
	}
	var plan declarativerelease.Plan
	var receipt declarativerelease.ArtifactReceipt
	if err := read(*pp, func(r io.Reader) (err error) { plan, err = declarativerelease.DecodePlan(r); return }); err != nil {
		return err
	}
	if err := read(*ap, func(r io.Reader) (err error) { receipt, err = declarativerelease.DecodeArtifactReceipt(r); return }); err != nil {
		return err
	}
	if len(plan.Releases) != 1 || plan.Releases[0].ComponentID != receipt.Component || receipt.PlanDigest != plan.PlanDigest {
		return errors.New("not an exact single-component release")
	}
	file, err := os.Open(defaultFrontRepairRegistry)
	if err != nil {
		return err
	}
	defer file.Close()
	registry, err := declarativerelease.DecodeEdgeGroupRegistry(file)
	if err != nil {
		return err
	}
	sel, err := selectFrontRepairWorker(registry, receipt.Component)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	runtime, err := newWorkerFrontObservationRuntime(sel)
	if err != nil {
		return err
	}
	preview, err := runtime.preview(ctx, sel)
	if err != nil {
		return err
	}
	result, err := bindWorkerFrontPreflight(sel, plan, receipt, preview)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(result)
}

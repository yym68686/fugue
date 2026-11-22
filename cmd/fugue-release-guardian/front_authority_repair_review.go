package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	"k8s.io/client-go/kubernetes"
)

const (
	frontRepairPreviewSchema   = "edge-front-authority-repair-preview/v1"
	defaultFrontRepairRegistry = "deploy/releases/edge-groups.json"
)

type frontRepairSelection struct {
	target  releaseguardian.TargetConfig
	runtime frontAuthorityConfig
	worker  declarativerelease.Component
}

func selectFrontRepairWorker(registry declarativerelease.EdgeGroupRegistry, component string) (frontRepairSelection, error) {
	if component == "" {
		return frontRepairSelection{}, errors.New("Front repair requires one exact declared Worker component")
	}
	if err := registry.Validate(); err != nil {
		return frontRepairSelection{}, err
	}
	for _, group := range registry.Groups {
		worker := group.Worker
		if worker.ID != component {
			continue
		}
		t, delivery := worker.Transition.EdgeGroupAB, worker.Delivery
		if worker.Transition.Type != "edge-group-ab" || t.WorkerContainer != "edge" ||
			t.ActivationStatePath != frontActivationStatePath || t.CASBinary != frontActivationCASBinary ||
			delivery == nil || delivery.Writer != "guardian" || delivery.Group != group.ID {
			return frontRepairSelection{}, errors.New("Front repair registry entry does not select the supported Guardian A/B writer")
		}
		target := releaseguardian.TargetConfig{Key: releaseguardian.Key{Component: component, Group: delivery.Group},
			Namespace: worker.Workload.Namespace, MonitorComponent: component, DependencyService: delivery.DependencyService}
		if err := target.Validate(); err != nil {
			return frontRepairSelection{}, err
		}
		return frontRepairSelection{target: target, worker: worker, runtime: frontAuthorityConfig{
			GroupID: group.GroupID, Namespace: target.Namespace, ExpectedNodes: t.ExpectedNodes}}, nil
	}
	return frontRepairSelection{}, errors.New("Front repair component is not a declared Edge Worker")
}

type frontRepairReleaseBinding struct {
	Component         string `json:"component"`
	Namespace         string `json:"namespace"`
	GroupID           string `json:"groupId"`
	PlanDigest        string `json:"planDigest"`
	MonitorDigest     string `json:"monitorDigest"`
	ArtifactDigest    string `json:"artifactDigest"`
	WorkerSourceSHA   string `json:"workerSourceSha"`
	WorkerImageDigest string `json:"workerImageDigest"`
}

func (selection frontRepairSelection) bindRelease(proof releaseguardian.FrontRepairWorkerRelease) (frontRepairReleaseBinding, error) {
	release, worker := proof.Release, selection.worker
	if worker.Transition == nil || worker.Transition.EdgeGroupAB == nil || worker.Delivery == nil ||
		release.ComponentID != worker.ID || release.Concurrency != worker.Concurrency ||
		release.Workload != worker.Workload || release.Artifact.Repository != worker.Artifact.Repository ||
		release.Transition == nil || release.Transition.Type != worker.Transition.Type || release.Transition.EdgeGroupAB == nil ||
		*release.Transition.EdgeGroupAB != *worker.Transition.EdgeGroupAB ||
		release.Delivery == nil || *release.Delivery != *worker.Delivery {
		return frontRepairReleaseBinding{}, errors.New("current immutable Worker release disagrees with the declared Worker lane")
	}
	return frontRepairReleaseBinding{Component: release.ComponentID, Namespace: selection.target.Namespace, GroupID: selection.runtime.GroupID,
		PlanDigest: proof.PlanDigest, MonitorDigest: proof.MonitorDigest, ArtifactDigest: proof.ArtifactDigest,
		WorkerSourceSHA: proof.WorkerSourceSHA, WorkerImageDigest: proof.WorkerImageDigest}, nil
}

type frontRepairPreview struct {
	Schema  string                    `json:"schema"`
	Release frontRepairReleaseBinding `json:"release"`
	Plan    frontRepairPlan           `json:"plan"`
	Digest  string                    `json:"digest"`
}

func (preview frontRepairPreview) validatedDigest() (string, error) {
	plan, release := preview.Plan, preview.Release
	if preview.Schema != frontRepairPreviewSchema || release.GroupID != plan.Before.GroupID ||
		release.WorkerSourceSHA != plan.Before.CurrentWorkerSourceSHA || release.WorkerImageDigest != plan.Before.CurrentWorkerImageDigest ||
		!exactSHA256Digest(release.PlanDigest) || !exactSHA256Digest(release.MonitorDigest) || !exactSHA256Digest(release.ArtifactDigest) ||
		(releaseguardian.Key{Component: release.Component}).Validate() != nil ||
		(releaseguardian.Key{Component: release.Namespace}).Validate() != nil {
		return "", errors.New("Front repair preview does not bind one immutable Worker release")
	}
	nodes := frontRepairPlanNodes(plan)
	rederived, err := bindFrontRepairPredecessors(plan.Before, plan.CurrentUID, plan.CurrentResourceVersion, nodes, plan.Members)
	wireDigest, wireErr := frontRepairPlanDigest(plan)
	if err != nil || wireErr != nil || rederived.Digest != plan.Digest || wireDigest != plan.Digest {
		return "", errors.Join(errors.New("Front repair preview predecessor is invalid"), err, wireErr)
	}
	preview.Digest = ""
	raw, err := declarativerelease.CanonicalJSON(preview)
	if err != nil {
		return "", err
	}
	return shaDigest(raw), nil
}

func frontRepairPlanNodes(plan frontRepairPlan) []frontRepairNode {
	nodes := make([]frontRepairNode, len(plan.Members))
	for i, member := range plan.Members {
		nodes[i] = member.Node
	}
	return nodes
}

func (r frontRepairRuntime) preview(ctx context.Context, selection frontRepairSelection) (frontRepairPreview, error) {
	if r.activator == nil || r.activator.config != selection.runtime {
		return frontRepairPreview{}, errors.New("Front repair preview must use the selected Worker lane runtime")
	}
	client := r.activator.client
	authority, err := releaseguardian.NewAuthorityStore(client, selection.target.Namespace)
	if err != nil {
		return frontRepairPreview{}, err
	}
	releases, err := releaseguardian.NewKubeStore(client, []releaseguardian.TargetConfig{selection.target})
	if err != nil {
		return frontRepairPreview{}, err
	}
	group := selection.runtime.GroupID
	requireSettled := func() error {
		if _, exists, err := authority.LoadTransitionJournal(ctx, group); err != nil || exists {
			return errors.Join(errors.New("Front repair preview cannot replace an outstanding transaction"), err)
		}
		return nil
	}
	if err := requireSettled(); err != nil {
		return frontRepairPreview{}, err
	}
	before, uid, rv, err := authority.LoadCurrent(ctx, group)
	if err != nil {
		return frontRepairPreview{}, err
	}
	proof, err := releases.LoadFrontRepairWorkerRelease(ctx, selection.target.Key, before)
	if err != nil {
		return frontRepairPreview{}, err
	}
	release, err := selection.bindRelease(proof)
	if err != nil {
		return frontRepairPreview{}, err
	}
	roster, _, err := r.roster(ctx)
	if err != nil {
		return frontRepairPreview{}, err
	}
	nodes := make([]frontRepairNode, 0, len(roster))
	for _, node := range roster {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	observed, err := r.observe(ctx, before, nodes)
	if err != nil {
		return frontRepairPreview{}, err
	}
	if err := requireSettled(); err != nil {
		return frontRepairPreview{}, err
	}
	actual, actualUID, actualRV, err := authority.LoadCurrent(ctx, group)
	if err != nil || actual != before || actualUID != uid || actualRV != rv {
		return frontRepairPreview{}, errors.Join(errors.New("Front repair preview authority changed during observation"), err)
	}
	currentProof, err := releases.LoadFrontRepairWorkerRelease(ctx, selection.target.Key, actual)
	if err != nil {
		return frontRepairPreview{}, err
	}
	currentRelease, err := selection.bindRelease(currentProof)
	if err != nil || currentRelease != release {
		return frontRepairPreview{}, errors.Join(errors.New("Front repair Worker release changed during observation"), err)
	}
	now := r.activator.now().UTC()
	for _, member := range observed {
		if member.ObservedAt.IsZero() || member.ObservedAt.After(now) || now.Sub(member.ObservedAt) >= 20*time.Second {
			return frontRepairPreview{}, fmt.Errorf("Front repair preview observation on %s expired", member.Node.Name)
		}
	}
	plan, err := planPartialFrontRollbackRepair(before, string(uid), rv, nodes, frontRepairWitnesses(observed))
	if err != nil {
		return frontRepairPreview{}, err
	}
	preview := frontRepairPreview{Schema: frontRepairPreviewSchema, Release: release, Plan: plan}
	preview.Digest, err = preview.validatedDigest()
	return preview, err
}

func newWorkerFrontObservationRuntime(selection frontRepairSelection) (frontRepairRuntime, error) {
	config, err := loadKubeConfig()
	if err != nil {
		return frontRepairRuntime{}, err
	}
	config.QPS, config.Burst = guardianKubeClientQPS, guardianKubeClientBurst
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return frontRepairRuntime{}, err
	}
	reader := workerObservationPodReader{namespace: selection.target.Namespace, delegate: &kubePodExecutor{config: config, client: client}}
	a, err := newFrontAuthorityActivator(client, reader, selection.runtime, "front-repair-preview")
	if err != nil {
		return frontRepairRuntime{}, err
	}
	return newFrontRepairRuntime(a), nil
}

// The release runner only needs the durable activation record. Do not expose
// the general native writer via the observer, even if its call graph changes.
type workerObservationPodReader struct {
	namespace string
	delegate  podCommandExecutor
}

func (r workerObservationPodReader) Exec(ctx context.Context, ns, pod, container string, args ...string) ([]byte, error) {
	if r.delegate == nil || ns == "" || ns != r.namespace || pod == "" || container != "edge" ||
		len(args) != 2 || args[0] != "cat" || args[1] != frontActivationStatePath {
		return nil, errors.New("Worker observation may only read the declared activation record")
	}
	return r.delegate.Exec(ctx, ns, pod, container, args...)
}

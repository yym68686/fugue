package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	"k8s.io/client-go/kubernetes"
)

const (
	frontRepairPreviewSchema   = "edge-front-authority-repair-preview/v1"
	frontRepairApprovalSchema  = "edge-front-authority-repair-approval/v1"
	frontRepairApprovalMaxAge  = 24 * time.Hour
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

type frontRepairApproval struct {
	Schema             string    `json:"schema"`
	PreviewDigest      string    `json:"previewDigest"`
	ExecutorCommit     string    `json:"executorCommit"`
	ToolManifestDigest string    `json:"toolManifestDigest"`
	ApprovedAt         time.Time `json:"approvedAt"`
	ExpiresAt          time.Time `json:"expiresAt"`
}

// bindPreview is structural validation only; it cannot authorize a journal.
func (approval frontRepairApproval) bindPreview(preview frontRepairPreview, verifiedExecutorCommit string, now time.Time) (frontRepairReviewedIntent, error) {
	previewDigest, err := preview.validatedDigest()
	if err != nil || preview.Digest != previewDigest || approval.PreviewDigest != previewDigest ||
		approval.Schema != frontRepairApprovalSchema || !exactSourceSHA(verifiedExecutorCommit) || approval.ExecutorCommit != verifiedExecutorCommit ||
		!exactSHA256Digest(approval.ToolManifestDigest) ||
		now.IsZero() || approval.ApprovedAt.IsZero() || approval.ApprovedAt.After(now) || !now.Before(approval.ExpiresAt) ||
		approval.ExpiresAt.Sub(approval.ApprovedAt) > frontRepairApprovalMaxAge {
		return frontRepairReviewedIntent{}, errors.Join(errors.New("Front repair approval does not name the exact preview, executor and live approval window"), err)
	}
	return frontRepairReviewedIntent{Group: preview.Release.GroupID, PlanDigest: preview.Plan.Digest,
		Nodes: frontRepairPlanNodes(preview.Plan)}, nil
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

// runApproved is for the protected launcher. It must obtain the trusted key
// and artifact evidence independently of the approval.
func (r frontRepairRuntime) runApproved(ctx context.Context, selection frontRepairSelection,
	preview frontRepairPreview, envelope frontRepairApprovalEnvelope, trust frontRepairApprovalTrust, artifact frontRepairVerifiedArtifact) (frontRepairOpened, error) {
	if r.activator == nil || r.activator.config != selection.runtime ||
		preview.Release.Component != selection.target.Key.Component || preview.Release.Namespace != selection.target.Namespace ||
		preview.Release.GroupID != selection.runtime.GroupID {
		return frontRepairOpened{}, errors.New("Front repair approval does not belong to this declared Worker lane")
	}
	if _, supported := r.activator.executor.(frontRepairToolExecutor); !supported {
		return frontRepairOpened{}, errors.New("Front repair has no protected temporary tool transport")
	}
	if r.requireControlPlaneFences == nil {
		return frontRepairOpened{}, errors.New("Front repair live control-plane fence checker is missing")
	}
	if !artifact.identity.valid() || artifact.identity != trust.Artifact {
		return frontRepairOpened{}, errors.New("Front repair requires authenticated Actions artifact intake")
	}
	tools := artifact.tools
	intent, err := envelope.verify(preview, trust, tools, r.activator.now().UTC())
	if err != nil {
		return frontRepairOpened{}, err
	}
	r.nodeTools = &tools
	// Even a detached node command with the maximum allowed clock skew
	// must expire before the approval. Never extend the caller deadline.
	ctx, cancel := context.WithDeadline(ctx, envelope.Statement.ExpiresAt.Add(-frontRepairNodeClockBound))
	defer cancel()
	if err := r.requireControlPlaneFences(ctx); err != nil {
		return frontRepairOpened{}, err
	}
	client := r.activator.client
	store, err := releaseguardian.NewKubeStore(client, []releaseguardian.TargetConfig{selection.target})
	if err != nil {
		return frontRepairOpened{}, err
	}
	authority, err := releaseguardian.NewAuthorityStore(client, selection.target.Namespace)
	if err != nil {
		return frontRepairOpened{}, err
	}
	readBoundRelease := func(ctx context.Context, authority *releaseguardian.AuthorityStore) (declarativerelease.PlanRelease, error) {
		before, uid, _, err := authority.LoadCurrent(ctx, intent.Group)
		if err != nil || string(uid) != preview.Plan.CurrentUID {
			return declarativerelease.PlanRelease{}, errors.Join(errors.New("reviewed group authority no longer exists"), err)
		}
		proof, err := store.LoadFrontRepairWorkerRelease(ctx, selection.target.Key, before)
		if err != nil {
			return declarativerelease.PlanRelease{}, err
		}
		binding, err := selection.bindRelease(proof)
		if err != nil || binding != preview.Release {
			return declarativerelease.PlanRelease{}, errors.Join(errors.New("reviewed immutable Worker release changed"), err)
		}
		return proof.Release, nil
	}
	release, err := readBoundRelease(ctx, authority)
	if err != nil {
		return frontRepairOpened{}, err
	}
	return r.runReviewedWithReleaseCheck(ctx, release, intent, func(ctx context.Context, locked frontRepairStore) error {
		if err := locked.requireHeld(ctx, intent.Group); err != nil {
			return err
		}
		if err := r.requireControlPlaneFences(ctx); err != nil {
			return err
		}
		if _, err := readBoundRelease(ctx, locked.authority); err != nil {
			return err
		}
		// Verify every recipient before opening a mutating transaction. An
		// unsupported peer must never strand a partly applied repair.
		members, err := r.observe(ctx, preview.Plan.Before, intent.Nodes)
		if err != nil {
			return err
		}
		for _, member := range members {
			if _, err := r.stagedToolForMember(ctx, member); err != nil {
				return err
			}
		}
		return nil
	})
}

// This is a read-only entrypoint: its only pod exec is the activation-file
// read already used by the runtime observer. There is no apply option.
func runFrontRepairPreview(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("front-repair-preview", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	registryPath := flags.String("registry", defaultFrontRepairRegistry, "checked declarative Edge group registry")
	component := flags.String("component", "", "exact Worker component from the registry")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *component == "" || stdout == nil {
		return errors.New("front-repair-preview requires --component and accepts only an optional --registry")
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, 45*time.Second)
	defer cancel()
	preview, err := captureFrontRepairPreview(ctx, *registryPath, *component)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(preview)
}

func captureFrontRepairPreview(ctx context.Context, registryPath, component string) (frontRepairPreview, error) {
	file, err := os.Open(registryPath)
	if err != nil {
		return frontRepairPreview{}, err
	}
	defer file.Close()
	registry, err := declarativerelease.DecodeEdgeGroupRegistry(file)
	if err != nil {
		return frontRepairPreview{}, err
	}
	selection, err := selectFrontRepairWorker(registry, component)
	if err != nil {
		return frontRepairPreview{}, err
	}
	config, err := loadKubeConfig()
	if err != nil {
		return frontRepairPreview{}, err
	}
	config.QPS, config.Burst = guardianKubeClientQPS, guardianKubeClientBurst
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return frontRepairPreview{}, err
	}
	a, err := newFrontAuthorityActivator(client, &kubePodExecutor{config: config, client: client}, selection.runtime, "front-repair-preview")
	if err != nil {
		return frontRepairPreview{}, err
	}
	return newFrontRepairRuntime(a).preview(ctx, selection)
}

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

func writeWorkerDrainWait(output io.Writer, prepared declarativerelease.ExecutionPlan, reason error) error {
	result := declarativerelease.WorkerDrainWait(prepared, reason)
	encoded, err := declarativerelease.CanonicalJSON(result)
	if err != nil {
		return err
	}
	_, err = output.Write(append(encoded, '\n'))
	return err
}

// An inactive Pod can already serve the target image after a node-local
// rollback. Check only Pods the candidate actually needs to replace.
func inactiveWorkersCanStart(pods map[string]edgeGroupPod, target declarativerelease.TargetIdentity, check func(edgeGroupPod) error) error {
	if len(pods) == 0 {
		return errors.New("inactive Worker cohort is empty")
	}
	blocked := make([]error, 0, len(pods))
	canStart := false
	for _, node := range sortedEdgeNodes(pods) {
		pod := pods[node]
		if pod.SourceCommit == target.ConfigSHA && pod.ImageRef == target.ImageRef {
			continue
		}
		if err := check(pod); err != nil {
			if !errors.Is(err, declarativerelease.ErrInactiveWorkerDraining) {
				return err
			}
			blocked = append(blocked, fmt.Errorf("node %s: %w", node, err))
			continue
		}
		canStart = true
	}
	if canStart {
		return nil
	}
	return errors.Join(blocked...)
}

func requireCurrentWorkerWait(ctx context.Context, release declarativerelease.PlanRelease, original declarativerelease.ExecutionPlan) error {
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	key := releaseguardian.Key{Component: release.ComponentID, Group: release.Delivery.Group}
	store, err := releaseguardian.NewKubeStore(client, []releaseguardian.TargetConfig{{Key: key, Namespace: release.Workload.Namespace,
		MonitorComponent: key.Component, DependencyService: release.Delivery.DependencyService}})
	if err != nil {
		return err
	}
	snapshot, err := store.Load(ctx, key)
	if err != nil {
		return err
	}
	if snapshot.Validate(time.Now().UTC()) != nil || !releaseguardian.WaitingForWorkerDrain(snapshot) ||
		snapshot.Bundle.Prepared.PlanDigest != original.PlanDigest ||
		snapshot.Record.RecordDigest != os.Getenv("FUGUE_RELEASE_GUARDIAN_RECORD_DIGEST") ||
		snapshot.Health.Dependency.State != releaseguardian.HealthHealthy || snapshot.Health.Route.State != releaseguardian.HealthHealthy {
		return errors.New("Worker drain no longer has its admitted desired release and serving LKG")
	}
	return nil
}

func prepareAfterWorkerDrain(ctx context.Context, cluster declarativerelease.Cluster, plan declarativerelease.Plan, artifact declarativerelease.ArtifactReceipt, original declarativerelease.ExecutionPlan, files map[string][]byte) (declarativerelease.ExecutionPlan, error) {
	rendered := declarativerelease.RenderedManifests{Forward: files["forward.json"], LKG: files["lkg.json"], ForwardDigest: original.Forward.ManifestDigest, LKGDigest: original.LKG.ManifestDigest}
	fresh, err := declarativerelease.PrepareExecution(ctx, cluster, plan, original.Component, artifact, rendered, time.Now().UTC())
	if err != nil {
		return declarativerelease.ExecutionPlan{}, err
	}
	if !sameWorkerWaitInputs(original, fresh) {
		return declarativerelease.ExecutionPlan{}, errors.New("Worker drain prewrite identity or participation changed")
	}
	return fresh, nil
}

func sameWorkerWaitInputs(original, fresh declarativerelease.ExecutionPlan) bool {
	return fresh.Component == original.Component && fresh.ConfigSHA == original.ConfigSHA &&
		fresh.ReleasePlanDigest == original.ReleasePlanDigest && fresh.IntentDigest == original.IntentDigest && fresh.ArtifactDigest == original.ArtifactDigest &&
		fresh.Forward == original.Forward && fresh.LKG == original.LKG && fresh.DegradedRoute == original.DegradedRoute &&
		fresh.DegradedPredecessor == original.DegradedPredecessor && !fresh.AlreadyConverged && !original.AlreadyConverged &&
		fresh.Prewrite.SameSpecIdentity(original.Prewrite) && reflect.DeepEqual(fresh.Prewrite.Participation, original.Prewrite.Participation)
}

func (cluster *kubectlCluster) inspectInactiveWorkers(ctx context.Context, release declarativerelease.PlanRelease, target declarativerelease.TargetIdentity, forward []byte) error {
	transition, err := cluster.managedEdgeTransition(ctx, *release.Transition.EdgeGroupAB)
	if err != nil {
		return err
	}
	state, err := cluster.readEdgeGroupState(ctx, release, transition)
	if err != nil {
		return err
	}
	if state.ActiveSlot != "a" && state.ActiveSlot != "b" {
		return errors.New("inactive Worker slot is unproven")
	}
	slot := otherEdgeSlot(state.ActiveSlot)
	// CI bound the rendered forward manifest to the active slot. A later
	// promotion must not repurpose the old candidate as the serving Worker.
	staged, err := declaredEdgeDaemonSetTarget(forward, release, edgeWorkerName(transition, slot), transition.WorkerContainer)
	if err != nil || staged.ConfigSHA != target.ConfigSHA || staged.ImageRef != target.ImageRef {
		return errors.New("inactive slot no longer matches the rendered Worker candidate")
	}
	active, err := declaredEdgeDaemonSetTarget(forward, release, edgeWorkerName(transition, state.ActiveSlot), transition.WorkerContainer)
	if err != nil || !edgePodsMatchTarget(edgeWorkerPods(state, state.ActiveSlot), active) {
		return errors.New("current serving Worker no longer matches the retained declaration")
	}
	inactive := edgeWorkerPods(state, slot)
	if len(inactive) != transition.ExpectedNodes {
		return errors.New("inactive Worker cohort is incomplete")
	}
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return err
	}
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return err
	}
	return inactiveWorkersCanStart(inactive, target, func(pod edgeGroupPod) error {
		return cluster.observeEdgeWorkerDrain(ctx, client, release, transition, edgeWorkerName(transition, slot), pod)
	})
}

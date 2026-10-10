package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

func (cluster *kubectlCluster) bindServingEdgeDeclaration(plan declarativerelease.Plan, release declarativerelease.PlanRelease, state edgeGroupState, rendered declarativerelease.RenderedManifests) (declarativerelease.RenderedManifests, error) {
	bound, err := declarativerelease.BindEdgeCandidateForward(plan, release.ComponentID, state.ActiveSlot, rendered)
	if err != nil {
		return bound, err
	}
	transition := *release.Transition.EdgeGroupAB
	name := edgeWorkerName(transition, state.ActiveSlot)
	declared, err := declaredEdgeDaemonSetTarget(bound.Forward, release, name, transition.WorkerContainer)
	if err != nil || edgePodsMatchTarget(edgeWorkerPods(state, state.ActiveSlot), declared) {
		return bound, err
	}
	if release.SupersedesFailedConfigSHA == "" {
		return bound, errors.New("retained active Worker differs from the verified LKG declaration")
	}
	config, err := loadComponentLeaseClientConfig()
	if err != nil {
		return bound, err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return bound, err
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return bound, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	runtime := &kubectlEdgeGroupRuntime{cluster: cluster, client: dynamicClient, release: release, transition: transition}
	current, authority, err := runtime.readCurrentAuthority(ctx)
	if err != nil || current.Validate() != nil || current.CurrentWorkerSourceSHA != release.SupersedesFailedConfigSHA || string(current.CurrentWorkerSlot) != state.ActiveSlot {
		return bound, errors.New("retained Worker recovery lacks the exact committed superseded authority")
	}
	target := declarativerelease.TargetIdentity{Present: true, ConfigSHA: current.CurrentWorkerSourceSHA, ManifestSHA: current.CurrentWorkerSourceSHA,
		OCIRevision: current.CurrentWorkerSourceSHA, ImageRef: release.Artifact.Repository + "@" + current.CurrentWorkerImageDigest}
	if !edgePodsMatchTarget(edgeWorkerPods(state, state.ActiveSlot), target) {
		return bound, errors.New("retained Worker runtime differs from committed authority")
	}
	for _, front := range state.FrontHealth {
		if !edgeFrontHealthMatchesServingAuthority(front, current) {
			return bound, errors.New("retained Worker is not selected by the exact Front authority")
		}
	}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: release.Workload.Namespace, Name: name}
	liveRaw, err := cluster.getResource(ctx, identity)
	if err != nil {
		return bound, err
	}
	live, err := decodeJSONObject(liveRaw)
	if err != nil {
		return bound, err
	}
	key := releaseguardian.Key{Component: release.ComponentID, Group: release.Delivery.Group}
	selector := labels.Set{"app.kubernetes.io/managed-by": "fugue-release-guardian", "fugue.pro/component": key.Component, "fugue.pro/group": key.Group}.AsSelector().String()
	records, err := client.CoreV1().ConfigMaps(release.Workload.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 128})
	if err != nil || records.Continue != "" {
		return bound, errors.New("retained Worker immutable declarations unavailable")
	}
	var retained map[string]any
	for _, object := range records.Items {
		if object.Immutable == nil || !*object.Immutable || object.Data["guardian-record.json"] == "" {
			continue
		}
		var record releaseguardian.ReleaseRecord
		if decodeStrictJSON([]byte(object.Data["guardian-record.json"]), &record) != nil || record.Validate() != nil || record.Key() != key || record.ConfigSHA != target.ConfigSHA || record.ImageDigest != current.CurrentWorkerImageDigest {
			continue
		}
		files := map[string][]byte{}
		for _, filename := range []string{"artifact-receipt.json", "execution-plan.json", "forward.json", "lkg.json", "release-plan.json"} {
			files[filename] = []byte(object.Data[filename])
		}
		bundle, decodeErr := releaseguardian.DecodeExecutionBundle(files, key)
		if decodeErr != nil {
			continue
		}
		expected, recordErr := bundle.ReleaseRecord(key, record.LKGRecordDigest)
		candidate, itemErr := declarativerelease.ResourceSetItem(bundle.Forward, identity)
		candidateTarget, targetErr := declaredEdgeDaemonSetTarget(bundle.Forward, release, name, transition.WorkerContainer)
		if recordErr != nil || expected != record || itemErr != nil || targetErr != nil || !retainedWorkerDeclarationMatches(candidateTarget, target, candidate, live) {
			continue
		}
		if verifyNoEmergencyOwnership(release, identity, candidate, live) != nil {
			continue
		}
		if retained != nil && digestJSON(retained) != digestJSON(candidate) {
			return bound, errors.New("retained Worker has ambiguous immutable declarations")
		}
		retained = candidate
	}
	if retained == nil {
		return bound, errors.New("retained Worker lacks an exact immutable release declaration")
	}
	latest, latestAuthority, err := runtime.readCurrentAuthority(ctx)
	if err != nil || latest != current || latestAuthority.GetUID() != authority.GetUID() || latestAuthority.GetResourceVersion() != authority.GetResourceVersion() {
		return bound, errors.New("retained Worker authority changed during declaration binding")
	}
	forward, err := declarativerelease.DecodeResourceSet(bytes.NewReader(bound.Forward))
	if err != nil {
		return bound, err
	}
	for index, item := range forward.Items {
		if strings.TrimSpace(stringValue(mapField(item, "metadata")["name"])) == name && item["kind"] == "DaemonSet" {
			forward.Items[index] = retained
		}
	}
	bound.Forward, err = declarativerelease.CanonicalJSON(forward)
	bound.ForwardDigest = digestBytesLocal(bound.Forward)
	return bound, err
}

func retainedWorkerDeclarationMatches(candidate, target declarativerelease.TargetIdentity, declared, live map[string]any) bool {
	return candidate.Present && target.Present && candidate.ConfigSHA == target.ConfigSHA && candidate.ManifestSHA == target.ManifestSHA &&
		candidate.OCIRevision == target.OCIRevision && candidate.ImageRef == target.ImageRef && declarativerelease.ResourceDesiredSubset(declared, live)
}

package releaseguardian

import (
	"context"
	"errors"

	"fugue/internal/edgegroupfront"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VerifyNodeWorkerArtifact binds the independent candidate to the ordinary
// release monitor, not a mutable image tag or a caller's assertion that a
// specified build was used. Only the one live replica in that verified
// Deployment can obtain this positive check. This does not authorize
// allocating the replica; the independent release must already own it.
func (store *AuthorityStore) VerifyNodeWorkerArtifact(ctx context.Context, j edgegroupfront.NodeWorkerJournal, pod corev1.Pod) error {
	reject := errors.New("candidate Worker is not the independently released, monitored replica")
	if !componentPattern.MatchString(j.CandidateComponent) || pod.Namespace != store.namespace ||
		pod.Spec.NodeName != j.Recipient.Node || string(pod.UID) != j.CandidateWorkerUID ||
		pod.Annotations["fugue.pro/source-commit"] != j.Candidate.WorkerSourceCommit ||
		pod.Labels["fugue.io/edge-node"] != j.Recipient.Node {
		return reject
	}
	read := &KubeStore{client: store.client}
	target := TargetConfig{Namespace: store.namespace, MonitorComponent: j.CandidateComponent}
	record, err := read.loadStableMonitorMap(ctx, target)
	if err != nil || record.UID == "" || record.ResourceVersion == "" || record.DeletionTimestamp != nil {
		return errors.Join(reject, err)
	}
	plan, artifact, prepared, monitor, _, _, err := decodeStableRecord(record.Data)
	if err != nil {
		return errors.Join(reject, err)
	}
	release, found := releaseForComponent(plan, j.CandidateComponent)
	workload := release.Workload
	if !found || workload.Kind != "Deployment" || workload.Namespace != store.namespace || workload.Container != "edge" ||
		workload.Replicas != 1 || workload.RolloutMode != "recreate" || monitor.Component != j.CandidateComponent ||
		artifact.Component != j.CandidateComponent || artifact.ReceiptDigest != j.ReleaseDigest ||
		artifact.OCIRevision != j.Candidate.WorkerSourceCommit || artifact.TopDigest != j.Candidate.WorkerImageDigest ||
		artifact.ImmutableRef != release.Artifact.Repository+"@"+j.Candidate.WorkerImageDigest || prepared.Forward.ImageRef != artifact.ImmutableRef {
		return reject
	}
	rsOwner := metav1.GetControllerOf(&pod)
	if rsOwner == nil || rsOwner.Kind != "ReplicaSet" || rsOwner.UID == "" {
		return reject
	}
	apps := store.client.AppsV1()
	rs, err := apps.ReplicaSets(store.namespace).Get(ctx, rsOwner.Name, metav1.GetOptions{})
	if err != nil || rs.UID != rsOwner.UID || rs.DeletionTimestamp != nil || rs.Spec.Replicas == nil || *rs.Spec.Replicas != 1 {
		return errors.Join(reject, err)
	}
	deploymentOwner := metav1.GetControllerOf(rs)
	if deploymentOwner == nil || deploymentOwner.Kind != workload.Kind || deploymentOwner.Name != workload.Name || deploymentOwner.UID == "" {
		return reject
	}
	d, err := apps.Deployments(store.namespace).Get(ctx, workload.Name, metav1.GetOptions{})
	if err != nil || d.UID != deploymentOwner.UID || d.DeletionTimestamp != nil || d.Spec.Replicas == nil ||
		*d.Spec.Replicas != 1 || d.Status.ObservedGeneration != d.Generation || d.Status.Replicas != 1 ||
		d.Status.UpdatedReplicas != 1 || d.Status.AvailableReplicas != 1 || d.Spec.Strategy.Type != "Recreate" ||
		d.Spec.Template.Spec.NodeSelector["fugue.io/edge-node"] != j.Recipient.Node && d.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"] != j.Recipient.Node ||
		workloadIdentityMatchesDeployment(d, release, prepared.Forward) != nil {
		return errors.Join(reject, err)
	}
	fresh, err := read.loadStableMonitorMap(ctx, target)
	if err != nil || fresh.Name != record.Name || fresh.UID != record.UID || fresh.ResourceVersion != record.ResourceVersion || fresh.DeletionTimestamp != nil {
		return errors.Join(errors.New("candidate Worker stable monitor changed during verification"), err)
	}
	return nil
}

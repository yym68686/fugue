package releaseguardian

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"fugue/internal/declarativerelease"
	"fugue/internal/edgegroupfront"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"
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
		pod.Annotations["fugue.pro/source-commit"] != j.Candidate.WorkerSourceCommit {
		return reject
	}
	if j.FixedTransport {
		return store.verifyFixedNodeWorkerArtifact(ctx, j, pod)
	}
	if pod.Labels["fugue.io/edge-node"] != j.Recipient.Node {
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

func (store *AuthorityStore) verifyFixedNodeWorkerArtifact(ctx context.Context, j edgegroupfront.NodeWorkerJournal, pod corev1.Pod) error {
	reject := errors.New("fixed Worker is not the exact retained declarative workload revision")
	owner := metav1.GetControllerOf(&pod)
	if owner == nil || owner.Kind != "DaemonSet" || owner.UID == "" || pod.Labels["fugue.io/edge-slot"] != j.Candidate.Slot {
		return reject
	}
	configSHA := pod.Annotations["fugue.pro/production-config-sha"]
	if !shaPattern.MatchString(configSHA) {
		return reject
	}
	matches, err := store.client.CoreV1().ConfigMaps(store.namespace).List(ctx, metav1.ListOptions{
		Limit: 16, LabelSelector: labels.Set{
			"app.kubernetes.io/managed-by": "fugue-declarative-release",
			"fugue.pro/component":          j.CandidateComponent, "fugue.pro/config-sha": configSHA,
		}.String(),
	})
	if err != nil || matches.Continue != "" || len(matches.Items) > 16 {
		return errors.Join(reject, err)
	}
	var rendered appsv1.DaemonSet
	var isolated bool
	for _, object := range matches.Items {
		if object.Immutable == nil || !*object.Immutable || object.UID == "" || object.ResourceVersion == "" || object.DeletionTimestamp != nil ||
			totalConfigMapBytes(object.Data) > maxRecordBytes {
			continue
		}
		plan, artifact, prepared, monitor, forward, _, err := decodeStableRecord(object.Data)
		release, found := releaseForComponent(plan, j.CandidateComponent)
		if err != nil || !found ||
			!fixedWorkerReleaseScope(release, j, owner.Name, pod.Namespace) ||
			monitor.Component != j.CandidateComponent || artifact.Component != j.CandidateComponent ||
			object.Name != monitorRecordNameFromDigest(j.CandidateComponent, monitor.RecordDigest) ||
			prepared.Forward.ConfigSHA != configSHA || artifact.ReceiptDigest != j.ReleaseDigest ||
			artifact.OCIRevision != j.Candidate.WorkerSourceCommit || artifact.TopDigest != j.Candidate.WorkerImageDigest ||
			artifact.ImmutableRef != prepared.Forward.ImageRef {
			continue
		}
		resources, err := declarativerelease.DecodeResourceSet(bytes.NewReader(forward))
		if err != nil {
			return errors.Join(reject, err)
		}
		nodeOnly := release.Transition == nil
		if nodeOnly && len(resources.Items) != 1 {
			return reject
		}
		for _, object := range resources.Items {
			meta, _ := object["metadata"].(map[string]any)
			if object["kind"] != "DaemonSet" || meta["name"] != owner.Name || meta["namespace"] != pod.Namespace {
				continue
			}
			raw, err := json.Marshal(object)
			if err != nil || rendered.Name != "" || json.Unmarshal(raw, &rendered) != nil ||
				rendered.Spec.Template.Labels["fugue.io/edge-slot"] != j.Candidate.Slot ||
				rendered.Spec.Template.Labels["fugue.io/edge-group-id"] != j.Recipient.Group ||
				rendered.Spec.Template.Annotations["fugue.pro/oci-revision"] != j.Candidate.WorkerSourceCommit ||
				containerImageMatches(rendered.Spec.Template.Spec.Containers, "edge", artifact.ImmutableRef) != nil {
				return reject
			}
			if nodeOnly && !fixedWorkerNodeTemplate(rendered.Spec.Template, j) {
				return reject
			}
			isolated = nodeOnly
		}
	}
	if rendered.Name == "" || rendered.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType {
		return errors.Join(reject, errors.New("no matching immutable OnDelete Worker release record"))
	}
	apps := store.client.AppsV1()
	ds, err := apps.DaemonSets(store.namespace).Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil || ds.UID != owner.UID || ds.DeletionTimestamp != nil || ds.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType {
		return errors.Join(reject, err)
	}
	if isolated && (!fixedWorkerNodeTemplate(ds.Spec.Template, j) || pod.Labels["fugue.io/edge-node"] != j.Recipient.Node) {
		return reject
	}
	revision, err := store.readFixedNodeWorkerRevision(ctx, pod.Labels[appsv1.ControllerRevisionHashLabelKey], owner)
	if err != nil {
		return errors.Join(reject, err)
	}
	var rev struct {
		Spec struct {
			Template corev1.PodTemplateSpec `json:"template"`
		} `json:"spec"`
	}
	if json.Unmarshal(revision.Data.Raw, &rev) != nil || rev.Spec.Template.Labels["fugue.io/edge-slot"] != j.Candidate.Slot ||
		rev.Spec.Template.Annotations["fugue.pro/production-config-sha"] != configSHA {
		return errors.Join(reject, errors.New("ControllerRevision slot or production config differs from the Worker"))
	}
	if isolated && !fixedWorkerNodeTemplate(rev.Spec.Template, j) {
		return reject
	}
	for _, c := range rendered.Spec.Template.Spec.Containers {
		if containerImageMatches(rev.Spec.Template.Spec.Containers, c.Name, c.Image) != nil ||
			containerImageMatches(pod.Spec.Containers, c.Name, c.Image) != nil {
			return errors.Join(reject, errors.New("ControllerRevision or live container image differs from the release record"))
		}
	}
	if isolated {
		for _, c := range rendered.Spec.Template.Spec.InitContainers {
			if containerImageMatches(rev.Spec.Template.Spec.InitContainers, c.Name, c.Image) != nil ||
				containerImageMatches(pod.Spec.InitContainers, c.Name, c.Image) != nil {
				return reject
			}
		}
	}
	return nil
}

// A fixed slot can be individually released after its shared template has
// been excluded from this node. The immutable ordinary release must itself
// name the replacement controller; a cloned live workload is not a receipt.
func fixedWorkerReleaseScope(r declarativerelease.PlanRelease, j edgegroupfront.NodeWorkerJournal, owner, namespace string) bool {
	if t := r.Transition; t != nil {
		return t.Type == "edge-group-ab" && t.EdgeGroupAB != nil && t.EdgeGroupAB.GroupID == j.Recipient.Group
	}
	w := r.Workload
	return j.FixedRotation && strings.HasPrefix(r.ComponentID, "edge-worker-") &&
		w.APIVersion == "apps/v1" && w.Kind == "DaemonSet" && w.Name == owner && w.Namespace == namespace &&
		w.Container == "edge" && w.RolloutMode == "on-delete"
}

func fixedWorkerNodeTemplate(t corev1.PodTemplateSpec, j edgegroupfront.NodeWorkerJournal) bool {
	if t.Spec.HostNetwork || t.Spec.NodeName != "" || t.Spec.NodeSelector["kubernetes.io/hostname"] != j.Recipient.Node ||
		t.Labels["fugue.io/edge-node"] != j.Recipient.Node || t.Labels["fugue.io/edge-group-id"] != j.Recipient.Group ||
		t.Labels["fugue.io/edge-slot"] != j.Candidate.Slot || t.Spec.Affinity == nil || t.Spec.Affinity.NodeAffinity == nil ||
		t.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	terms := t.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchFields) != 1 {
		return false
	}
	f := terms[0].MatchFields[0]
	return f.Key == "metadata.name" && f.Operator == corev1.NodeSelectorOpIn && len(f.Values) == 1 && f.Values[0] == j.Recipient.Node
}

func (store *AuthorityStore) readFixedNodeWorkerRevision(ctx context.Context, hash string, owner *metav1.OwnerReference) (*appsv1.ControllerRevision, error) {
	// DaemonSet revision hashes can begin with a digit. Validate the hash as
	// a DNS label before using it as a suffix in the exact object lookup.
	if len(validation.IsDNS1123Label(hash)) != 0 {
		return nil, errors.New("Worker ControllerRevision hash label is invalid")
	}
	revision, err := store.client.AppsV1().ControllerRevisions(store.namespace).Get(ctx, owner.Name+"-"+hash, metav1.GetOptions{})
	if err != nil || revision.UID == "" || revision.ResourceVersion == "" || revision.DeletionTimestamp != nil {
		return nil, errors.Join(errors.New("exact live Worker ControllerRevision is unavailable"), err)
	}
	revisionOwner := metav1.GetControllerOf(revision)
	if revisionOwner == nil || revisionOwner.Kind != owner.Kind || revisionOwner.Name != owner.Name || revisionOwner.UID != owner.UID {
		return nil, errors.New("Worker ControllerRevision does not belong to the exact DaemonSet")
	}
	return revision, nil
}

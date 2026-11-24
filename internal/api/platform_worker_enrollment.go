package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

type workerEnrollmentBinding struct {
	id         string
	serviceUID string
}

// Physical identity still comes from Kubernetes, not caller input, an Edge
// heartbeat, or an old serving receipt. Only the current signed global source
// can authorize a replacement registration.
func (s *Server) bindGlobalWorkerEnrollment(ctx context.Context, client *clusterNodeClient, pod corev1.Pod) (workerEnrollmentBinding, error) {
	var empty workerEnrollmentBinding
	authority, release, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil || !found {
		return empty, err
	}
	policy, err := platformproducer.Decode(authority)
	if err != nil {
		return empty, err
	}
	if policy.TargetScope != "global" || policy.Mode != "serving" || policy.PublicationRole != "" || policy.DNSPolicyArtifactID == "" {
		return empty, nil
	}
	source, err := s.store.GetPlatformArtifact(policy.DNSPolicyArtifactID)
	if err != nil {
		return empty, err
	}
	if source.Content["offline_edges"] == nil {
		return empty, nil
	}
	raw, err := json.Marshal(source.Content["offline_edges"])
	var holds []platformproducer.OfflineEdge
	if err != nil || len(raw) > 128<<10 || json.Unmarshal(raw, &holds) != nil || platformproducer.ValidateOfflineEdges(holds) != nil {
		return empty, errors.New("invalid signed Worker enrollment input")
	}
	for _, hold := range holds {
		if hold.EdgeID != pod.Spec.NodeName || hold.WorkerEnrollment == nil {
			continue
		}
		trusted := func(a model.PlatformArtifact) bool {
			return a.Status == model.PlatformArtifactStatusValidated && s.store.VerifyPlatformArtifactIntegrity(a) == nil &&
				platformsafety.EvaluateArtifactIntegrity(a, s.bundleKeyring()).Pass
		}
		if release.ArtifactID != authority.ID || release.Generation != authority.Generation || release.Status != model.PlatformArtifactReleaseStatusActive || release.FencingToken <= 0 ||
			source.ContentHash != policy.DNSPolicyDigest || !trusted(authority) || !trusted(source) {
			return empty, errors.New("Worker enrollment has no active signed authority")
		}
		var node corev1.Node
		if err := client.getKubernetesNode(ctx, "/api/v1/nodes/"+url.PathEscape(hold.EdgeID), &node); err != nil {
			return empty, err
		}
		var serviceUID string
		if !matchesReplacementWorker(hold, pod, node) {
			owner, exact := workerController(pod.OwnerReferences)
			if exact && owner.Kind == "DaemonSet" {
				// Existing DS Pods may keep their original identity, but the
				// permanent ledger forbids it from being re-enrolled.
				return empty, nil
			}
			// Never fall back to the retired consumer ID for a new owner.
			// The pin is release configuration, not a Pod-supplied input.
			serviceUID = s.workerFrontBindings[hold.EdgeID].uid
			if serviceUID == "" {
				return empty, errors.New("independent Worker has no pinned public Front")
			}
			if err := verifyIndependentWorkerDeployment(ctx, client, hold, pod, node); err != nil {
				return empty, err
			}
		}
		// A replacement is not an alternative name for an active consumer.
		// Both abandoned route slots must exist, and the proposed new ID
		// must not itself have been retired.
		legacy := hold
		legacy.WorkerEnrollment = nil
		oldTargets, err := offlineEdgeRouteEnrollments(legacy)
		if err != nil {
			return empty, err
		}
		newTargets, err := offlineEdgeRouteEnrollments(hold)
		if err != nil {
			return empty, err
		}
		retired, err := s.store.RetiredPlatformConsumerTargets(ctx, append(oldTargets[:], newTargets[:]...))
		if err != nil {
			return empty, err
		}
		if !retired[oldTargets[0]] || !retired[oldTargets[1]] || retired[newTargets[0]] || retired[newTargets[1]] {
			return empty, errors.New("Worker replacement enrollment is not authorized by retirement state")
		}
		return workerEnrollmentBinding{id: hold.WorkerEnrollment.ID, serviceUID: serviceUID}, nil
	}
	return empty, nil
}

func workerEnrollmentPhysicalMatch(hold platformproducer.OfflineEdge, pod corev1.Pod, node corev1.Node) bool {
	enrollment := hold.WorkerEnrollment
	if enrollment == nil || pod.Spec.NodeName != hold.EdgeID || node.Name != hold.EdgeID || string(node.UID) != enrollment.NodeUID || node.DeletionTimestamp != nil ||
		node.Labels["fugue.io/edge-group-id"] != hold.EdgeGroupID || node.Labels["fugue.io/role.edge"] != "true" ||
		pod.Labels["fugue.io/edge-group-id"] != hold.EdgeGroupID || pod.Spec.ServiceAccountName != enrollment.ServiceAccount {
		return false
	}
	cutover, err := time.Parse(time.RFC3339, enrollment.PodCreatedAfter)
	return err == nil && pod.CreationTimestamp.After(cutover)
}

func matchesReplacementWorker(hold platformproducer.OfflineEdge, pod corev1.Pod, node corev1.Node) bool {
	owner, exact := workerController(pod.OwnerReferences)
	return workerEnrollmentPhysicalMatch(hold, pod, node) && exact && owner.Kind == "DaemonSet" &&
		slices.Contains(hold.WorkerEnrollment.DaemonSetUIDs, string(owner.UID))
}

func workerController(owners []metav1.OwnerReference) (metav1.OwnerReference, bool) {
	var controller metav1.OwnerReference
	for _, owner := range owners {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if controller.UID != "" || owner.APIVersion != "apps/v1" || owner.UID == "" {
			return metav1.OwnerReference{}, false
		}
		controller = owner
	}
	return controller, controller.UID != ""
}

func verifyIndependentWorkerDeployment(ctx context.Context, client *clusterNodeClient, hold platformproducer.OfflineEdge, pod corev1.Pod, node corev1.Node) error {
	deny := errors.New("independent Worker has no matching signed Deployment and immutable Pod identity")
	enrollment := hold.WorkerEnrollment
	owner, exact := workerController(pod.OwnerReferences)
	if enrollment == nil || enrollment.Validate() != nil || enrollment.IndependentDeployment == nil ||
		!workerEnrollmentPhysicalMatch(hold, pod, node) || pod.UID == "" || pod.ResourceVersion == "" ||
		pod.DeletionTimestamp != nil || (pod.Status.Phase != corev1.PodPending && pod.Status.Phase != corev1.PodRunning) ||
		len(validation.IsDNS1123Label(pod.Namespace)) != 0 || !exact ||
		owner.Kind != "ReplicaSet" || len(validation.IsDNS1123Subdomain(owner.Name)) != 0 {
		return deny
	}
	grant := *enrollment.IndependentDeployment
	if !independentWorkerPodShape(pod.Spec, pod.Labels, hold, grant) {
		return deny
	}
	// Verify the real controller chain under the API's existing read rights.
	// Do not require AvailableReplicas or a stable Monitor here: the candidate
	// needs this credential to prepare before it can become ready.
	base := "/apis/apps/v1/namespaces/" + url.PathEscape(pod.Namespace)
	var rs appsv1.ReplicaSet
	if err := client.doJSON(ctx, http.MethodGet, base+"/replicasets/"+url.PathEscape(owner.Name), &rs); err != nil {
		return errors.Join(deny, err)
	}
	parent, owned := workerController(rs.OwnerReferences)
	hash := pod.Labels[appsv1.DefaultDeploymentUniqueLabelKey]
	if rs.Namespace != pod.Namespace || rs.Name != owner.Name || rs.UID != owner.UID || rs.ResourceVersion == "" ||
		rs.DeletionTimestamp != nil || rs.Spec.Replicas == nil || *rs.Spec.Replicas != 1 || !owned ||
		parent.Kind != "Deployment" || parent.Name != grant.Name || string(parent.UID) != grant.UID ||
		hash == "" || rs.Spec.Selector == nil || rs.Spec.Selector.MatchLabels[appsv1.DefaultDeploymentUniqueLabelKey] != hash ||
		rs.Spec.Template.Labels[appsv1.DefaultDeploymentUniqueLabelKey] != hash ||
		!workerSelectorMatches(rs.Spec.Selector, pod.Labels, rs.Spec.Template.Labels) ||
		!independentWorkerPodShape(rs.Spec.Template.Spec, rs.Spec.Template.Labels, hold, grant) {
		return deny
	}
	var d appsv1.Deployment
	if err := client.doJSON(ctx, http.MethodGet, base+"/deployments/"+url.PathEscape(grant.Name), &d); err != nil {
		return errors.Join(deny, err)
	}
	if d.Namespace != pod.Namespace || d.Name != grant.Name || string(d.UID) != grant.UID || d.ResourceVersion == "" ||
		d.DeletionTimestamp != nil || d.Spec.Replicas == nil || *d.Spec.Replicas != 1 || d.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType ||
		!workerSelectorMatches(d.Spec.Selector, pod.Labels, d.Spec.Template.Labels) ||
		!independentWorkerPodShape(d.Spec.Template.Spec, d.Spec.Template.Labels, hold, grant) {
		return deny
	}
	return nil
}

func workerSelectorMatches(selector *metav1.LabelSelector, podLabels, templateLabels map[string]string) bool {
	compiled, err := metav1.LabelSelectorAsSelector(selector)
	return err == nil && !compiled.Empty() && compiled.Matches(labels.Set(podLabels)) && compiled.Matches(labels.Set(templateLabels))
}

func independentWorkerPodShape(spec corev1.PodSpec, podLabels map[string]string, hold platformproducer.OfflineEdge, grant platformproducer.OfflineWorkerDeployment) bool {
	if podLabels["fugue.io/edge-group-id"] != hold.EdgeGroupID || podLabels["fugue.io/edge-node"] != hold.EdgeID ||
		spec.ServiceAccountName != hold.WorkerEnrollment.ServiceAccount || len(spec.Containers) != 2 {
		return false
	}
	hasNodePin := false
	for _, key := range []string{"kubernetes.io/hostname", "fugue.io/edge-node"} {
		if node, set := spec.NodeSelector[key]; set {
			if node != hold.EdgeID {
				return false
			}
			hasNodePin = true
		}
	}
	images := map[string]string{spec.Containers[0].Name: spec.Containers[0].Image, spec.Containers[1].Name: spec.Containers[1].Image}
	return hasNodePin && images["edge"] == grant.EdgeImage && images["caddy"] == grant.CaddyImage
}

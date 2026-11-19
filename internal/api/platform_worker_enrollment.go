package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// Physical identity still comes from Kubernetes, not caller input, an Edge
// heartbeat, or an old serving receipt. Only the current signed global source
// can authorize a replacement registration.
func (s *Server) bindGlobalWorkerEnrollment(ctx context.Context, client *clusterNodeClient, pod corev1.Pod) (string, error) {
	authority, release, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil || !found {
		return "", err
	}
	policy, err := platformproducer.Decode(authority)
	if err != nil {
		return "", err
	}
	if policy.TargetScope != "global" || policy.Mode != "serving" || policy.PublicationRole != "" || policy.DNSPolicyArtifactID == "" {
		return "", nil
	}
	source, err := s.store.GetPlatformArtifact(policy.DNSPolicyArtifactID)
	if err != nil {
		return "", err
	}
	if source.Content["offline_edges"] == nil {
		return "", nil
	}
	raw, err := json.Marshal(source.Content["offline_edges"])
	var holds []platformproducer.OfflineEdge
	if err != nil || len(raw) > 128<<10 || json.Unmarshal(raw, &holds) != nil || platformproducer.ValidateOfflineEdges(holds) != nil {
		return "", errors.New("invalid signed Worker enrollment input")
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
			return "", errors.New("Worker enrollment has no active signed authority")
		}
		var node corev1.Node
		if err := client.getKubernetesNode(ctx, "/api/v1/nodes/"+url.PathEscape(hold.EdgeID), &node); err != nil {
			return "", err
		}
		if !matchesReplacementWorker(hold, pod, node) {
			// Existing Pods may keep observing their original identity, but
			// the permanent ledger still forbids it from being re-enrolled.
			return "", nil
		}
		// A replacement is not an alternative name for an active consumer.
		// Both abandoned route slots must exist, and the proposed new ID
		// must not itself have been retired.
		legacy := hold
		legacy.WorkerEnrollment = nil
		oldTargets, err := offlineEdgeRouteEnrollments(legacy)
		if err != nil {
			return "", err
		}
		newTargets, err := offlineEdgeRouteEnrollments(hold)
		if err != nil {
			return "", err
		}
		retired, err := s.store.RetiredPlatformConsumerTargets(ctx, append(oldTargets[:], newTargets[:]...))
		if err != nil {
			return "", err
		}
		if !retired[oldTargets[0]] || !retired[oldTargets[1]] || retired[newTargets[0]] || retired[newTargets[1]] {
			return "", errors.New("Worker replacement enrollment is not authorized by retirement state")
		}
		return hold.WorkerEnrollment.ID, nil
	}
	return "", nil
}

func matchesReplacementWorker(hold platformproducer.OfflineEdge, pod corev1.Pod, node corev1.Node) bool {
	enrollment := hold.WorkerEnrollment
	if enrollment == nil || pod.Spec.NodeName != hold.EdgeID || node.Name != hold.EdgeID || string(node.UID) != enrollment.NodeUID || node.DeletionTimestamp != nil ||
		node.Labels["fugue.io/edge-group-id"] != hold.EdgeGroupID || node.Labels["fugue.io/role.edge"] != "true" ||
		pod.Labels["fugue.io/edge-group-id"] != hold.EdgeGroupID || pod.Spec.ServiceAccountName != enrollment.ServiceAccount {
		return false
	}
	cutover, err := time.Parse(time.RFC3339, enrollment.PodCreatedAfter)
	if err != nil || !pod.CreationTimestamp.After(cutover) {
		return false
	}
	owners := 0
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller {
			if owner.APIVersion != "apps/v1" || owner.Kind != "DaemonSet" || !slices.Contains(enrollment.DaemonSetUIDs, string(owner.UID)) {
				return false
			}
			owners++
		}
	}
	return owners == 1
}

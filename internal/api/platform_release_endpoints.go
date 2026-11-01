package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"

	"fugue/internal/releaseproof"

	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/util/validation"
)

type kubeOwnerReferenceEvidence = releaseproof.OwnerReference
type kubeReleaseAnnotations = releaseproof.ReleaseAnnotations
type kubeReleaseEndpointEvidence = releaseproof.Endpoint
type kubeReleaseReplicaSetEvidence = releaseproof.ReplicaSet
type kubeReleasePodEvidence = releaseproof.Pod
type releaseEndpointPodIdentity = releaseproof.EndpointPodIdentity

func (c *managedAppStatusClient) captureReleaseEndpointOwners(ctx context.Context, snapshot *managedAppKubeSnapshot) error {
	pods, err := listTypedKubeResources[kubeReleasePodEvidence](ctx, c, "/api/v1/pods")
	if err != nil {
		return err
	}
	byPod := make(map[string]kubeReleasePodEvidence, len(pods))
	for _, pod := range pods {
		key := kubeNamespacedKey(pod.Metadata.Namespace, pod.Metadata.Name)
		if _, exists := byPod[key]; exists || pod.Metadata.Namespace == "" || pod.Metadata.Name == "" {
			return fmt.Errorf("ambiguous Pod inventory")
		}
		byPod[key] = pod
	}
	// Only ReplicaSets named by observed endpoint Pods can participate in the
	// ownership proof. Historical ReplicaSets are not part of that authority.
	// Keep negative endpoints too: the evaluator validates their ownership before
	// deciding readiness and must not silently drop an invalid member.
	type reference struct{ namespace, name string }
	references := map[string]reference{}
	for _, slices := range snapshot.releaseEndpointSlices {
		for _, slice := range slices {
			for _, endpoint := range slice.Endpoints {
				target := endpoint.TargetRef
				pod, found := byPod[kubeNamespacedKey(target.Namespace, target.Name)]
				if !found || target.Kind != "Pod" || target.APIVersion != "" && target.APIVersion != "v1" || target.Namespace != slice.Metadata.Namespace || target.UID == "" || pod.Metadata.UID != target.UID {
					continue // The unchanged evaluator rejects this endpoint.
				}
				owner, valid := singleKubeControllerOwner(pod.Metadata.OwnerReferences, "apps/v1", "ReplicaSet")
				if !valid {
					continue
				}
				if len(validation.IsDNS1123Label(pod.Metadata.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(owner.Name)) != 0 {
					return fmt.Errorf("invalid ReplicaSet owner reference")
				}
				references[kubeNamespacedKey(pod.Metadata.Namespace, owner.Name)] = reference{pod.Metadata.Namespace, owner.Name}
			}
		}
	}
	keys := make([]string, 0, len(references))
	for key := range references {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	replicas := make([]kubeReleaseReplicaSetEvidence, len(keys))
	group, readCtx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	for index, key := range keys {
		ref := references[key]
		group.Go(func() error {
			if err := readCtx.Err(); err != nil {
				return err
			}
			path := "/apis/apps/v1/namespaces/" + url.PathEscape(ref.namespace) + "/replicasets/" + url.PathEscape(ref.name)
			var rs kubeReleaseReplicaSetEvidence
			if err := c.doJSON(readCtx, path, &rs); err != nil {
				var status *kubeStatusError
				if errors.As(err, &status) && status.StatusCode == http.StatusNotFound {
					return nil // Missing owner remains absent and cannot make a release ready.
				}
				return err
			}
			if rs.Metadata.Namespace != ref.namespace || rs.Metadata.Name != ref.name {
				return fmt.Errorf("ReplicaSet response identity differs from request")
			}
			replicas[index] = rs
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	byReplicaSet := make(map[string]kubeReleaseReplicaSetEvidence, len(replicas))
	for _, rs := range replicas {
		if rs.Metadata.Name != "" {
			byReplicaSet[kubeNamespacedKey(rs.Metadata.Namespace, rs.Metadata.Name)] = rs
		}
	}
	// No partially assembled ownership evidence escapes on a failed read. UID,
	// labels, release key, image and controller checks remain in the evaluator.
	snapshot.releasePods, snapshot.releaseReplicaSets = byPod, byReplicaSet
	return nil
}

func singleKubeControllerOwner(refs []kubeOwnerReferenceEvidence, apiVersion, kind string) (kubeOwnerReferenceEvidence, bool) {
	return releaseproof.SingleControllerOwner(refs, apiVersion, kind)
}

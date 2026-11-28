package releaseguardian

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"fugue/internal/declarativerelease"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// rehomePlacement is read-only deployment and image evidence. A successful
// observation does not prove uncached pulls, route/dependency health, the
// immutable recovery baseline, or closure of the provider write boundary.
type rehomePlacement struct {
	Deployment RecoveryRecordRef   `json:"deployment"`
	Generation int64               `json:"generation"`
	Pods       []NativeServingPod  `json:"pods"`
	Services   []RecoveryRecordRef `json:"services"`
}

// verifyRegistryRehomePlacement reconstructs the preimage of a Deployment
// after a claimed write, without depending on a surviving worker or a
// successful PATCH response. Only all reviewed images and the existing
// pod-template stamp may differ from the signed source spec.
func verifyRegistryRehomePlacement(review RegistryRehomeReview, live *appsv1.Deployment) error {
	source := review.Source.Deployment
	if live == nil || live.Generation < 2 || live.Generation-1 != source.Generation {
		return errors.New("registry rehome placement is not the next reviewed Deployment generation")
	}
	before := live.DeepCopy()
	before.Generation--
	pod := &before.Spec.Template.Spec
	projection := review.Draft.Current
	for _, containers := range [][]corev1.Container{pod.Containers, pod.InitContainers} {
		for i := range containers {
			if containers[i].Image != projection.IndependentImage {
				return errors.New("registry rehome placement does not use the independent image in every container")
			}
			containers[i].Image = projection.SourceImage
		}
	}
	if value, exists := before.Spec.Template.Annotations[podArtifactImageAnnotation]; exists {
		if value != projection.IndependentImage {
			return errors.New("registry rehome placement does not use the independent image in the pod stamp")
		}
		before.Spec.Template.Annotations[podArtifactImageAnnotation] = projection.SourceImage
	}
	cas, err := prepareRegistryRehomeDeploymentCAS(review, before)
	if err != nil || !apiequality.Semantic.DeepEqual(cas.expected.Spec, live.Spec) {
		return errors.Join(errors.New("registry rehome placement cannot reconstruct the signed source spec"), err)
	}
	return verifyRegistryRehomeReady(live)
}

func verifyRegistryRehomeReady(live *appsv1.Deployment) error {
	count := derefReplicas(live.Spec.Replicas)
	status := live.Status
	if count < 1 || status.ObservedGeneration != live.Generation || status.Replicas != count || status.UpdatedReplicas != count ||
		status.ReadyReplicas != count || status.AvailableReplicas != count || status.UnavailableReplicas != 0 {
		return errors.New("registry rehome placement has not converged")
	}
	return nil
}

// observeRegistryRehomePlacement is deliberately separate from ordinary
// serving observation. It is safe to repeat after a lost reply, but cannot
// reserve a claim, infer that a write did not happen, or create a new seal.
func observeRegistryRehomePlacement(ctx context.Context, client kubernetes.Interface, review RegistryRehomeReview) (rehomePlacement, error) {
	return observeRegistryRehomePlacementAt(ctx, client, review, false)
}

func observeRegistryRehomePlacementAt(ctx context.Context, client kubernetes.Interface, review RegistryRehomeReview, original bool) (rehomePlacement, error) {
	id := review.Draft.Workload
	if client == nil || id != review.Source.Deployment.Identity ||
		id.APIVersion != "apps/v1" || id.Kind != "Deployment" || id.Namespace == "" || id.Name == "" {
		return rehomePlacement{}, errors.New("registry rehome placement requires a reader for the reviewed Kubernetes Deployment")
	}
	deployments := client.AppsV1().Deployments(id.Namespace)
	live, err := deployments.Get(ctx, id.Name, metav1.GetOptions{})
	if err != nil {
		return rehomePlacement{}, err
	}
	store := KubeStore{client: client}
	mirror := review.Draft.Mirror.Forward.RecoveryImage
	// Only image matching uses this transient view; it is not a new release
	// receipt and must never be marshalled into the historical archive.
	image := declarativerelease.ArtifactReceipt{ImmutableRef: mirror.Image,
		TopDigest: mirror.IndexDigest, PlatformManifestDigest: mirror.ManifestDigest}
	if original {
		if _, err := prepareRegistryRehomeDeploymentCAS(review, live); err != nil {
			return rehomePlacement{}, err
		}
		if err := verifyRegistryRehomeReady(live); err != nil {
			return rehomePlacement{}, err
		}
		image.ImmutableRef = review.Draft.Current.SourceImage
	} else if err := verifyRegistryRehomePlacement(review, live); err != nil {
		return rehomePlacement{}, err
	}
	pods, err := store.nativeServingPods(ctx, live, image)
	if err != nil {
		return rehomePlacement{}, err
	}
	readServices := func() ([]RecoveryRecordRef, error) {
		out := make([]RecoveryRecordRef, 0, len(review.Source.Services))
		for _, s := range review.Source.Services {
			identity := s.Live.Identity
			if identity.Kind != "Service" || identity.APIVersion != "v1" || identity.Namespace != id.Namespace {
				return nil, errors.New("registry rehome placement contains an unscoped companion")
			}
			svc, err := client.CoreV1().Services(identity.Namespace).Get(ctx, identity.Name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			spec, err := declarativerelease.CanonicalJSON(svc.Spec)
			if err != nil || svc.UID == "" || string(svc.UID) != s.Live.UID || svc.Generation != s.Live.Generation ||
				svc.ResourceVersion == "" || svc.DeletionTimestamp != nil || digest(spec) != s.Live.SpecDigest {
				return nil, fmt.Errorf("registry rehome companion Service %s drifted from its reviewed identity or spec", identity.Name)
			}
			out = append(out, recoveryRef(svc))
		}
		return out, nil
	}
	services, err := readServices()
	if err != nil {
		return rehomePlacement{}, err
	}
	freshPods, err := store.nativeServingPods(ctx, live, image)
	if err != nil || !slices.Equal(pods, freshPods) {
		return rehomePlacement{}, errors.Join(errors.New("registry rehome serving Pods changed during readback"), err)
	}
	freshServices, err := readServices()
	if err != nil || !slices.Equal(services, freshServices) {
		return rehomePlacement{}, errors.Join(errors.New("registry rehome companion Services changed during readback"), err)
	}
	fresh, err := deployments.Get(ctx, id.Name, metav1.GetOptions{})
	if err != nil || recoveryRef(fresh) != recoveryRef(live) {
		return rehomePlacement{}, errors.Join(errors.New("registry rehome Deployment changed during readback"), err)
	}
	return rehomePlacement{recoveryRef(live), live.Generation, pods, services}, nil
}

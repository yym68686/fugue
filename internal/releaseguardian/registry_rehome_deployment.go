package releaseguardian

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"fugue/internal/declarativerelease"
	appsv1 "k8s.io/api/apps/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

const podArtifactImageAnnotation = "fugue.pro/artifact-image"

type rehomePatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

type rehomeDeploymentCAS struct {
	source   *appsv1.Deployment
	expected *appsv1.Deployment
	patch    []byte
}

// prepareRegistryRehomeDeploymentCAS derives a provider mutation from an
// already inspected review and a fresh exact Deployment read. The provider
// version may have advanced because of status writes; the signed UID,
// generation, and complete spec may not change. This does not grant authority
// to send the patch or construct a new verified monitor record.
func prepareRegistryRehomeDeploymentCAS(review RegistryRehomeReview, live *appsv1.Deployment) (rehomeDeploymentCAS, error) {
	draft, bound := review.Draft, review.Source.Deployment
	image := draft.Current
	reject := func(reason string) (rehomeDeploymentCAS, error) {
		return rehomeDeploymentCAS{}, fmt.Errorf("%w: %s", declarativerelease.ErrWriteRejected, reason)
	}
	if !validRegistryRehomeProjection(image, draft.Mirror.Forward) || draft.Workload != bound.Identity ||
		bound.Identity.APIVersion != "apps/v1" || bound.Identity.Kind != "Deployment" || bound.Identity.Namespace == "" || bound.Identity.Name == "" ||
		live == nil || live.Name != bound.Identity.Name || live.Namespace != bound.Identity.Namespace ||
		live.UID == "" || string(live.UID) != bound.UID || live.Generation < 1 || live.Generation == math.MaxInt64 || live.Generation != bound.Generation ||
		live.ResourceVersion == "" || live.DeletionTimestamp != nil || live.Spec.Paused {
		return reject("registry rehome Deployment no longer matches the reviewed object")
	}
	spec, err := declarativerelease.CanonicalJSON(live.Spec)
	if err != nil || digest(spec) != bound.SpecDigest {
		return reject("registry rehome Deployment spec drifted from the signed review")
	}
	pod := live.Spec.Template.Spec
	podMap, err := rehomeJSONMap(pod)
	if err != nil {
		return reject("registry rehome Deployment PodSpec is invalid")
	}
	containers, err := singleArtifactDeploymentContainers(podMap, image.SourceImage)
	if err != nil || len(containers) != len(image.Containers) {
		return reject("registry rehome Deployment has a different container inventory")
	}
	expected := live.DeepCopy()
	tests := []rehomePatchOperation{
		{Op: "test", Path: "/metadata/uid", Value: string(live.UID)},
		{Op: "test", Path: "/metadata/resourceVersion", Value: live.ResourceVersion},
	}
	var writes []rehomePatchOperation
	for i, observed := range containers {
		reviewed := image.Containers[i]
		if reviewed.Field != observed.field || reviewed.Name != observed.name {
			return reject("registry rehome Deployment container order differs from the reviewed image projection")
		}
		index := i
		containerList := expected.Spec.Template.Spec.Containers
		if observed.field == "initContainers" {
			index -= len(expected.Spec.Template.Spec.Containers)
			containerList = expected.Spec.Template.Spec.InitContainers
		}
		if index < 0 || index >= len(containerList) || containerList[index].Name != observed.name || containerList[index].Image != image.SourceImage {
			return reject("registry rehome Deployment lacks the reviewed image location")
		}
		path := "/spec/template/spec/" + observed.field + "/" + strconv.Itoa(index)
		tests = append(tests, rehomePatchOperation{Op: "test", Path: path + "/name", Value: observed.name},
			rehomePatchOperation{Op: "test", Path: path + "/image", Value: image.SourceImage})
		writes = append(writes, rehomePatchOperation{Op: "replace", Path: path + "/image", Value: image.IndependentImage})
		containerList[index].Image = image.IndependentImage
	}
	if value, exists := live.Spec.Template.Annotations[podArtifactImageAnnotation]; exists {
		if value != image.SourceImage {
			return reject("registry rehome Deployment has a different pod artifact-image annotation")
		}
		const path = "/spec/template/metadata/annotations/fugue.pro~1artifact-image"
		tests = append(tests, rehomePatchOperation{Op: "test", Path: path, Value: value})
		writes = append(writes, rehomePatchOperation{Op: "replace", Path: path, Value: image.IndependentImage})
		expected.Spec.Template.Annotations[podArtifactImageAnnotation] = image.IndependentImage
	}
	patch, err := json.Marshal(append(tests, writes...))
	if err != nil {
		return reject("could not encode registry rehome compare-and-swap")
	}
	return rehomeDeploymentCAS{source: live.DeepCopy(), expected: expected, patch: patch}, nil
}

// writeRegistryRehomeDeploymentOnce is intentionally not a CLI or a
// dispatcher. A provider-fenced executor must verify independent cutover
// evidence, claim the resource once, and invoke this with its enrolled
// non-redirecting client. No response authorizes automatic replay or
// settlement; even a good response still needs rollout/closure evidence.
func writeRegistryRehomeDeploymentOnce(ctx context.Context, client rest.Interface, cas rehomeDeploymentCAS) (*appsv1.Deployment, error) {
	if client == nil || cas.source == nil || cas.expected == nil || len(cas.patch) == 0 {
		return nil, errors.Join(declarativerelease.ErrWriteRejected, errors.New("registry rehome has no bound Deployment request"))
	}
	var actual appsv1.Deployment
	err := client.Patch(types.JSONPatchType).Namespace(cas.source.Namespace).Resource("deployments").Name(cas.source.Name).
		MaxRetries(0).Body(cas.patch).Do(ctx).Into(&actual)
	if err != nil {
		if apierrors.IsConflict(err) || apierrors.IsInvalid(err) || apierrors.IsForbidden(err) ||
			apierrors.IsUnauthorized(err) || apierrors.IsNotFound(err) || apierrors.IsBadRequest(err) {
			return nil, errors.Join(declarativerelease.ErrWriteRejected, err)
		}
		return nil, fmt.Errorf("%w: registry rehome did not return a definitive result: %v", declarativerelease.ErrWriteOutcomeUnknown, err)
	}
	if actual.UID != cas.source.UID || actual.Name != cas.source.Name || actual.Namespace != cas.source.Namespace ||
		actual.ResourceVersion == "" || actual.ResourceVersion == cas.source.ResourceVersion || actual.Generation != cas.source.Generation+1 ||
		actual.DeletionTimestamp != nil || !apiequality.Semantic.DeepEqual(actual.Spec, cas.expected.Spec) ||
		!apiequality.Semantic.DeepEqual(actual.Annotations, cas.source.Annotations) || !apiequality.Semantic.DeepEqual(actual.Labels, cas.source.Labels) ||
		!apiequality.Semantic.DeepEqual(actual.OwnerReferences, cas.source.OwnerReferences) || !apiequality.Semantic.DeepEqual(actual.Finalizers, cas.source.Finalizers) {
		return nil, fmt.Errorf("%w: registry rehome response did not match the exact image-only change", declarativerelease.ErrWriteOutcomeUnknown)
	}
	return &actual, nil
}

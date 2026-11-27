package releaseguardian

import (
	"context"
	"errors"
	"fmt"

	"fugue/internal/declarativerelease"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// ApplyNativeDeployment uses one provider-side compare-and-swap for an
// enrolled Deployment. The caller has already checked the approved manifest,
// artifact set, dispatch reservation, and provider permissions.
func ApplyNativeDeployment(ctx context.Context, client rest.Interface, identity declarativerelease.ResourceIdentity, manager string, manifest []byte) error {
	item, err := declarativerelease.ResourceSetItem(manifest, identity)
	if err != nil {
		return errors.Join(declarativerelease.ErrWriteRejected, err)
	}
	meta, _ := item["metadata"].(map[string]any)
	uid, _ := meta["uid"].(string)
	rv, _ := meta["resourceVersion"].(string)
	if client == nil || manager == "" || uid == "" || rv == "" ||
		identity.APIVersion != "apps/v1" || identity.Kind != "Deployment" || identity.Namespace == "" {
		return fmt.Errorf("%w: native update lacks its exact Deployment identity, version or field manager", declarativerelease.ErrWriteRejected)
	}
	body, err := declarativerelease.CanonicalJSON(item)
	if err != nil {
		return errors.Join(declarativerelease.ErrWriteRejected, err)
	}
	// Never enter the legacy field-owner transfer, which patches then
	// reapplies. Do not force ownership or retry an unknown write.
	var observed appsv1.Deployment
	err = client.Patch(types.ApplyPatchType).Namespace(identity.Namespace).Resource("deployments").Name(identity.Name).
		Param("fieldManager", manager).MaxRetries(0).Body(body).Do(ctx).Into(&observed)
	if err != nil {
		if apierrors.IsConflict(err) || apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) ||
			apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsNotFound(err) {
			return errors.Join(declarativerelease.ErrWriteRejected, err)
		}
		return fmt.Errorf("%w: Deployment compare-and-swap did not return a definitive result: %v", declarativerelease.ErrWriteOutcomeUnknown, err)
	}
	if observed.GetName() != identity.Name || observed.GetNamespace() != identity.Namespace ||
		string(observed.GetUID()) != uid || observed.GetResourceVersion() == "" {
		return fmt.Errorf("%w: Deployment response does not identify the bound object", declarativerelease.ErrWriteOutcomeUnknown)
	}
	return nil
}

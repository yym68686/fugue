package controller

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

const (
	kubeCoreV1DiscoveryPath              = "/api/v1"
	kubeSelfSubjectAccessReviewPath      = "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"
	managedPostgresResizeSubresourceName = "pods/resize"
)

type kubeAPIResourceList struct {
	GroupVersion string            `json:"groupVersion"`
	Resources    []kubeAPIResource `json:"resources"`
}

type kubeAPIResource struct {
	Name       string   `json:"name"`
	Namespaced bool     `json:"namespaced"`
	Kind       string   `json:"kind"`
	Verbs      []string `json:"verbs"`
}

type kubeResourceAttributes struct {
	Namespace   string `json:"namespace"`
	Verb        string `json:"verb"`
	Group       string `json:"group"`
	Resource    string `json:"resource"`
	Subresource string `json:"subresource"`
}

type kubeSelfSubjectAccessReview struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Spec       struct {
		ResourceAttributes kubeResourceAttributes `json:"resourceAttributes"`
	} `json:"spec"`
	Status *kubeSubjectAccessReviewStatus `json:"status,omitempty"`
}

type kubeSubjectAccessReviewStatus struct {
	Allowed         *bool  `json:"allowed"`
	Denied          *bool  `json:"denied,omitempty"`
	Reason          string `json:"reason,omitempty"`
	EvaluationError string `json:"evaluationError,omitempty"`
}

// managedPostgresResizeCapability records two independent facts: whether the
// apiserver advertises the Pod resize subresource with the patch verb, and
// whether the controller's own identity is authorized to patch that exact
// subresource in the target namespace. Available is derived rather than stored
// so callers cannot accidentally construct a contradictory capability state.
type managedPostgresResizeCapability struct {
	Namespace                   string
	ResizeSubresourceDiscovered bool
	PatchVerbDiscovered         bool
	PatchAuthorized             bool
	Reason                      string
	Message                     string
}

func (c managedPostgresResizeCapability) Available() bool {
	return c.ResizeSubresourceDiscovered && c.PatchVerbDiscovered && c.PatchAuthorized
}

// inspectManagedPostgresResizeCapability is deliberately observation-only for
// workload objects. The SelfSubjectAccessReview is a non-persisted Kubernetes
// authorization check; this method never patches a Pod or any controller-owned
// resource. Every malformed, unavailable, or denied state fails closed.
func (c *kubeClient) inspectManagedPostgresResizeCapability(
	ctx context.Context,
	namespace string,
) (managedPostgresResizeCapability, error) {
	namespace = strings.TrimSpace(c.effectiveNamespace(namespace))
	capability := managedPostgresResizeCapability{Namespace: namespace}
	if namespace == "" {
		return capability, fmt.Errorf("managed postgres resize capability requires a namespace")
	}

	var resources kubeAPIResourceList
	if _, err := c.doJSON(ctx, http.MethodGet, kubeCoreV1DiscoveryPath, nil, &resources); err != nil {
		return capability, fmt.Errorf("discover Kubernetes Pod resize subresource: %w", err)
	}
	if resources.GroupVersion != "v1" {
		return capability, fmt.Errorf("Kubernetes core discovery returned groupVersion %q, expected v1", resources.GroupVersion)
	}

	var resizeResource *kubeAPIResource
	for index := range resources.Resources {
		resource := &resources.Resources[index]
		if resource.Name != managedPostgresResizeSubresourceName {
			continue
		}
		if resizeResource != nil {
			return capability, fmt.Errorf("Kubernetes core discovery returned duplicate %s resources", managedPostgresResizeSubresourceName)
		}
		resizeResource = resource
	}
	if resizeResource == nil {
		capability.Reason = "resize_subresource_not_discovered"
		capability.Message = "Kubernetes core discovery does not advertise pods/resize"
		return capability, nil
	}
	if !resizeResource.Namespaced || resizeResource.Kind != "Pod" {
		return capability, fmt.Errorf(
			"Kubernetes %s discovery shape is invalid: namespaced=%t kind=%q",
			managedPostgresResizeSubresourceName,
			resizeResource.Namespaced,
			resizeResource.Kind,
		)
	}
	capability.ResizeSubresourceDiscovered = true
	for _, verb := range resizeResource.Verbs {
		if verb == "patch" {
			capability.PatchVerbDiscovered = true
			break
		}
	}
	if !capability.PatchVerbDiscovered {
		capability.Reason = "resize_patch_verb_not_discovered"
		capability.Message = "Kubernetes pods/resize discovery does not advertise the patch verb"
		return capability, nil
	}

	review := kubeSelfSubjectAccessReview{
		APIVersion: "authorization.k8s.io/v1",
		Kind:       "SelfSubjectAccessReview",
	}
	review.Spec.ResourceAttributes = managedPostgresResizeResourceAttributes(namespace)
	var response kubeSelfSubjectAccessReview
	if _, err := c.doJSON(ctx, http.MethodPost, kubeSelfSubjectAccessReviewPath, review, &response); err != nil {
		return capability, fmt.Errorf("review Kubernetes Pod resize authorization: %w", err)
	}
	if response.APIVersion != review.APIVersion || response.Kind != review.Kind {
		return capability, fmt.Errorf(
			"Kubernetes Pod resize authorization response identity is invalid: apiVersion=%q kind=%q",
			response.APIVersion,
			response.Kind,
		)
	}
	if response.Spec.ResourceAttributes != review.Spec.ResourceAttributes {
		return capability, fmt.Errorf("Kubernetes Pod resize authorization response changed the reviewed resource attributes")
	}
	if response.Status == nil || response.Status.Allowed == nil {
		return capability, fmt.Errorf("Kubernetes Pod resize authorization response omitted status.allowed")
	}
	if evaluationError := strings.TrimSpace(response.Status.EvaluationError); evaluationError != "" {
		return capability, fmt.Errorf("Kubernetes Pod resize authorization evaluation failed: %s", evaluationError)
	}
	denied := response.Status.Denied != nil && *response.Status.Denied
	if *response.Status.Allowed && denied {
		return capability, fmt.Errorf("Kubernetes Pod resize authorization response is contradictory: allowed and denied are both true")
	}
	if *response.Status.Allowed {
		capability.PatchAuthorized = true
		capability.Reason = "available"
		capability.Message = "Kubernetes pods/resize patch capability is available"
		return capability, nil
	}

	capability.Reason = "resize_patch_authorization_not_allowed"
	if denied {
		capability.Reason = "resize_patch_authorization_denied"
	}
	capability.Message = strings.TrimSpace(response.Status.Reason)
	if capability.Message == "" {
		capability.Message = "Kubernetes did not authorize pods/resize patch in the target namespace"
	}
	return capability, nil
}

func managedPostgresResizeResourceAttributes(namespace string) kubeResourceAttributes {
	return kubeResourceAttributes{
		Namespace:   strings.TrimSpace(namespace),
		Verb:        "patch",
		Group:       "",
		Resource:    "pods",
		Subresource: "resize",
	}
}

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"fugue/internal/inputarchive"

	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// This narrow client never forwards caller headers, follows redirects, logs
// response bodies or reads Secret/env inventories. Both input and response are
// bounded, and all metadata reads share the authentication deadline.
func pendingRootKubernetesJSON(ctx context.Context, c *clusterNodeClient, method, path string, input, output any) error {
	if c == nil || c.client == nil || c.bearerToken == "" {
		return errors.New("identity client unavailable")
	}
	var body io.Reader
	if input != nil {
		raw, err := json.Marshal(input)
		if err != nil || len(raw) > 65536 {
			return errors.New("bounded identity request invalid")
		}
		body = bytes.NewReader(raw)
	}
	r, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return errors.New("identity request unavailable")
	}
	r.Header.Set("Authorization", "Bearer "+c.bearerToken)
	r.Header.Set("Accept", "application/json")
	if input != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	client := *c.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(r)
	if err != nil {
		return errors.New("identity verification unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		return errors.New("identity verification refused")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 || len(raw) == 0 || json.Unmarshal(raw, output) != nil {
		return errors.New("bounded identity response invalid")
	}
	return nil
}

func pendingRootController(metadata metav1.ObjectMeta, kind string) (metav1.OwnerReference, bool) {
	var found metav1.OwnerReference
	for _, owner := range metadata.OwnerReferences {
		if owner.Controller != nil && *owner.Controller {
			if found.UID != "" || owner.Kind != kind || owner.APIVersion != "apps/v1" || owner.UID == "" || owner.Name == "" {
				return metav1.OwnerReference{}, false
			}
			found = owner
		}
	}
	return found, found.UID != ""
}

func pendingRootTemplate(template corev1.PodTemplateSpec, authority PendingRootAuthority) bool {
	if template.Annotations["fugue.pro/oci-revision"] != authority.SourceSHA || template.Labels["fugue.io/edge-group-id"] != strings.TrimPrefix(authority.Owner, "edge-control/") || template.Spec.ServiceAccountName != authority.ServiceAccount {
		return false
	}
	count := 0
	for _, c := range template.Spec.Containers {
		if c.Name == "edge-control" {
			if c.Image != authority.ImageRef {
				return false
			}
			count++
		}
	}
	return count == 1
}

func (s *Server) verifyPendingRootPod(ctx context.Context, token string, transport *pendingRootTransport) (*pendingRootPrincipal, int) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if !inputarchive.PodTokenHasExclusiveAudience(token, pendingRootAudience) {
		return nil, http.StatusUnauthorized
	}
	if s.newClusterNodeClient == nil || transport == nil || transport.retired.Load() {
		return nil, http.StatusServiceUnavailable
	}
	c, err := s.newClusterNodeClient()
	if err != nil || c == nil || c.client == nil || c.bearerToken == "" {
		return nil, http.StatusServiceUnavailable
	}
	defer c.closeIdleConnections()
	review := authenticationv1.TokenReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "authentication.k8s.io/v1", Kind: "TokenReview"},
		Spec:     authenticationv1.TokenReviewSpec{Token: token, Audiences: []string{pendingRootAudience}},
	}
	var verified authenticationv1.TokenReview
	if pendingRootKubernetesJSON(ctx, c, http.MethodPost, "/apis/authentication.k8s.io/v1/tokenreviews", &review, &verified) != nil {
		return nil, http.StatusServiceUnavailable
	}
	user := verified.Status.User
	if !verified.Status.Authenticated || verified.Status.Error != "" || len(verified.Status.Audiences) != 1 || verified.Status.Audiences[0] != pendingRootAudience || user.UID == "" {
		return nil, http.StatusUnauthorized
	}
	identity := strings.Split(user.Username, ":")
	if len(identity) != 4 || identity[0] != "system" || identity[1] != "serviceaccount" || identity[2] != s.controlPlaneNamespace || identity[3] == "" {
		return nil, http.StatusForbidden
	}
	names, uids := user.Extra["authentication.kubernetes.io/pod-name"], user.Extra["authentication.kubernetes.io/pod-uid"]
	if len(names) != 1 || names[0] == "" || len(uids) != 1 || uids[0] == "" {
		return nil, http.StatusUnauthorized
	}
	base := "/api/v1/namespaces/" + url.PathEscape(identity[2])
	apps := "/apis/apps/v1/namespaces/" + url.PathEscape(identity[2])
	var pod corev1.Pod
	if pendingRootKubernetesJSON(ctx, c, http.MethodGet, base+"/pods/"+url.PathEscape(names[0]), nil, &pod) != nil {
		return nil, http.StatusServiceUnavailable
	}
	if pod.Name != names[0] || pod.Namespace != identity[2] || string(pod.UID) != uids[0] || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || pod.Spec.NodeName == "" || pod.Spec.ServiceAccountName != identity[3] {
		return nil, http.StatusForbidden
	}
	owner, ok := pendingRootController(pod.ObjectMeta, "ReplicaSet")
	if !ok {
		return nil, http.StatusForbidden
	}
	var rs appsv1.ReplicaSet
	if pendingRootKubernetesJSON(ctx, c, http.MethodGet, apps+"/replicasets/"+url.PathEscape(owner.Name), nil, &rs) != nil {
		return nil, http.StatusServiceUnavailable
	}
	deploymentOwner, ok := pendingRootController(rs.ObjectMeta, "Deployment")
	if !ok || rs.UID != owner.UID || rs.Name != owner.Name || rs.Namespace != pod.Namespace || rs.DeletionTimestamp != nil || rs.ResourceVersion == "" {
		return nil, http.StatusForbidden
	}
	// Selection uses the controlling UID learned from authenticated Kubernetes
	// metadata. No owner, group, key/path or policy supplied in the body is read.
	a, ok := transport.authorities[string(deploymentOwner.UID)]
	if !ok || a.Namespace != pod.Namespace || a.Deployment != deploymentOwner.Name || a.ServiceAccount != identity[3] || a.ServiceAccountUID != user.UID {
		return nil, http.StatusForbidden
	}
	var deployment appsv1.Deployment
	var account corev1.ServiceAccount
	if pendingRootKubernetesJSON(ctx, c, http.MethodGet, apps+"/deployments/"+url.PathEscape(a.Deployment), nil, &deployment) != nil ||
		pendingRootKubernetesJSON(ctx, c, http.MethodGet, base+"/serviceaccounts/"+url.PathEscape(a.ServiceAccount), nil, &account) != nil {
		return nil, http.StatusServiceUnavailable
	}
	if string(deployment.UID) != a.DeploymentUID || deployment.Name != a.Deployment || deployment.Namespace != a.Namespace || deployment.DeletionTimestamp != nil || deployment.ResourceVersion == "" || deployment.Annotations["fugue.pro/production-config-sha"] != a.SourceSHA ||
		account.Name != a.ServiceAccount || account.Namespace != a.Namespace || string(account.UID) != a.ServiceAccountUID || account.DeletionTimestamp != nil ||
		!pendingRootTemplate(deployment.Spec.Template, a) || !pendingRootTemplate(rs.Spec.Template, a) ||
		!pendingRootTemplate(corev1.PodTemplateSpec{ObjectMeta: pod.ObjectMeta, Spec: pod.Spec}, a) {
		return nil, http.StatusForbidden
	}
	for _, selector := range []*metav1.LabelSelector{deployment.Spec.Selector, rs.Spec.Selector} {
		if selector == nil {
			return nil, http.StatusForbidden
		}
		compiled, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil || compiled.Empty() || !compiled.Matches(labels.Set(pod.Labels)) || !compiled.Matches(labels.Set(rs.Spec.Template.Labels)) {
			return nil, http.StatusForbidden
		}
	}
	imageMatches := false
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "edge-control" && strings.TrimPrefix(status.ImageID, "docker-pullable://") == a.ImageRef {
			imageMatches = true
		}
	}
	if !imageMatches {
		return nil, http.StatusForbidden
	}
	var afterPod corev1.Pod
	var afterRS appsv1.ReplicaSet
	var afterDeployment appsv1.Deployment
	var afterAccount corev1.ServiceAccount
	if pendingRootKubernetesJSON(ctx, c, http.MethodGet, base+"/pods/"+url.PathEscape(pod.Name), nil, &afterPod) != nil ||
		pendingRootKubernetesJSON(ctx, c, http.MethodGet, apps+"/replicasets/"+url.PathEscape(rs.Name), nil, &afterRS) != nil ||
		pendingRootKubernetesJSON(ctx, c, http.MethodGet, apps+"/deployments/"+url.PathEscape(a.Deployment), nil, &afterDeployment) != nil ||
		pendingRootKubernetesJSON(ctx, c, http.MethodGet, base+"/serviceaccounts/"+url.PathEscape(a.ServiceAccount), nil, &afterAccount) != nil {
		return nil, http.StatusServiceUnavailable
	}
	if !reflect.DeepEqual(pod, afterPod) || !reflect.DeepEqual(rs, afterRS) || !reflect.DeepEqual(deployment, afterDeployment) || !reflect.DeepEqual(account, afterAccount) || transport.retired.Load() || ctx.Err() != nil {
		return nil, http.StatusForbidden
	}
	return &pendingRootPrincipal{transport: transport, authority: a, podUID: string(pod.UID)}, http.StatusOK
}

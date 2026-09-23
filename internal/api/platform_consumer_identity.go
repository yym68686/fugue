package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"

	"fugue/internal/httpx"
	"fugue/internal/platformcontrol"
)

const platformConsumerIdentityAnnotation = "fugue.pro/consumer-identity"
const platformConsumerIdentityTTL = 2 * time.Minute

// Operator-managed Pod metadata authorizes component capabilities;
// callers cannot choose their component, node, scope or artifact kinds.
type platformConsumerIdentityPolicy struct {
	Version       string   `json:"version"`
	Component     string   `json:"component"`
	ScopeKey      string   `json:"scope_key"`
	ArtifactKinds []string `json:"artifact_kinds"`
}

func decodePlatformConsumerIdentityPolicy(raw string) (platformConsumerIdentityPolicy, bool) {
	var policy platformConsumerIdentityPolicy
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	valid := len(raw) <= 4096 && decoder.Decode(&policy) == nil && decoder.Decode(&struct{}{}) == io.EOF && policy.Version == "v1" && policy.Component != "" && policy.ScopeKey != "" && policy.ScopeKey == strings.ToLower(strings.TrimSpace(policy.ScopeKey)) && len(policy.ArtifactKinds) > 0
	return policy, valid
}

func (s *Server) handleExchangePlatformConsumerIdentity(w http.ResponseWriter, r *http.Request) {
	// Deliberately require a header-only credential; query credentials are not
	// accepted and neither Kubernetes nor signing errors include token material.
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || len(parts[1]) > 32768 {
		httpx.WriteError(w, http.StatusUnauthorized, "bound Kubernetes Pod credential required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	client, err := s.newClusterNodeClient()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "component identity verifier unavailable")
		return
	}
	defer client.closeIdleConnections()
	user, status := reviewPlatformConsumerSubject(ctx, client, parts[1])
	if status != http.StatusOK {
		httpx.WriteError(w, status, "bound Kubernetes Pod credential could not be verified")
		return
	}

	identity := strings.Split(user.Username, ":")
	if len(identity) != 4 || identity[0] != "system" || identity[1] != "serviceaccount" || s.controlPlaneNamespace == "" || identity[2] != s.controlPlaneNamespace || identity[3] == "" || user.UID == "" {
		httpx.WriteError(w, http.StatusForbidden, "ServiceAccount is not authorized for platform consumers")
		return
	}
	names := user.Extra["authentication.kubernetes.io/pod-name"]
	uids := user.Extra["authentication.kubernetes.io/pod-uid"]
	if len(names) != 1 || names[0] == "" || len(uids) != 1 || uids[0] == "" {
		httpx.WriteError(w, http.StatusUnauthorized, "credential must be bound to a Pod")
		return
	}
	base := "/api/v1/namespaces/" + url.PathEscape(identity[2])
	var pod corev1.Pod
	if err = client.doJSON(ctx, http.MethodGet, base+"/pods/"+url.PathEscape(names[0]), &pod); err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "bound Pod identity unavailable")
		return
	}
	if string(pod.UID) != uids[0] || pod.Namespace != identity[2] || pod.Name != names[0] || pod.Spec.ServiceAccountName != identity[3] || pod.Spec.NodeName == "" || pod.DeletionTimestamp != nil || (pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodPending) {
		httpx.WriteError(w, http.StatusForbidden, "live Pod identity does not match the credential")
		return
	}
	policy, valid := decodePlatformConsumerIdentityPolicy(pod.Annotations[platformConsumerIdentityAnnotation])
	if !valid {
		httpx.WriteError(w, http.StatusForbidden, "Pod has no valid consumer authorization")
		return
	}
	now := time.Now().UTC()
	claims := platformcontrol.PlatformComponentIdentityClaims{
		CredentialID: "kubernetes:" + identity[2] + ":" + identity[3] + ":" + string(pod.UID),
		Component:    policy.Component, NodeID: pod.Spec.NodeName, ScopeKey: policy.ScopeKey, ArtifactKinds: policy.ArtifactKinds,
	}
	// Issuance and parsing also enforce the kernel's finite component/kind schema.
	token, err := platformcontrol.IssuePlatformComponentIdentity(s.auth.PlatformComponentIdentityKeyring, claims, now, platformConsumerIdentityTTL)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "consumer identity could not be issued")
		return
	}
	verified, err := platformcontrol.ParsePlatformComponentIdentity(s.auth.PlatformComponentIdentityKeyring, token, now)
	if err != nil || len(verified.ArtifactKinds) != len(policy.ArtifactKinds) || verified.Component != policy.Component {
		httpx.WriteError(w, http.StatusForbidden, "Pod consumer capabilities are invalid")
		return
	}
	for _, kind := range policy.ArtifactKinds {
		if !slices.Contains(verified.ArtifactKinds, kind) {
			httpx.WriteError(w, http.StatusForbidden, "Pod consumer capabilities are invalid")
			return
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"token": token, "expires_at": time.Unix(verified.ExpiresAtUnix, 0).UTC(), "component": verified.Component, "node_id": verified.NodeID, "scope_key": verified.ScopeKey, "artifact_kinds": verified.ArtifactKinds})
}

// SelfSubjectReview authenticates using only the submitted Pod token. The API's
// own credential is reserved for the subsequent live Pod metadata lookup.
func reviewPlatformConsumerSubject(ctx context.Context, client *clusterNodeClient, token string) (authenticationv1.UserInfo, int) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.baseURL+"/apis/authentication.k8s.io/v1/selfsubjectreviews", strings.NewReader(`{"apiVersion":"authentication.k8s.io/v1","kind":"SelfSubjectReview"}`))
	if err != nil {
		return authenticationv1.UserInfo{}, http.StatusServiceUnavailable
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	reviewer := *client.client
	reviewer.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := reviewer.Do(request)
	if err != nil {
		return authenticationv1.UserInfo{}, http.StatusServiceUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return authenticationv1.UserInfo{}, http.StatusUnauthorized
	}
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		return authenticationv1.UserInfo{}, http.StatusServiceUnavailable
	}
	var review authenticationv1.SelfSubjectReview
	if json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(&review) != nil {
		return authenticationv1.UserInfo{}, http.StatusServiceUnavailable
	}
	if review.Status.UserInfo.Username == "" || review.Status.UserInfo.UID == "" {
		return authenticationv1.UserInfo{}, http.StatusUnauthorized
	}
	return review.Status.UserInfo, http.StatusOK
}

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
)

const publicIngressTransportManager = "fugue-front-serving-transport"
const publicIngressProbeManager = "fugue-front-probe-transport"
const publicIngressEgressManager = "fugue-public-ingress-egress"

func appAllowsPublicIngressEgress(app model.App) bool {
	p := app.Spec.NetworkPolicy
	return p != nil && p.Egress != nil && model.NormalizeAppNetworkPolicyMode(p.Egress.Mode) == model.AppNetworkPolicyModeRestricted && p.Egress.AllowPublicInternet
}

// Reconcile the network permission independently of application image/storage
// rollout. The Service declaration is the configuration source; code never
// invents an endpoint or changes the user's egress intent.
func (s *Service) reconcilePublicIngressEgress(ctx context.Context, client *kubeClient, namespace string, managed runtime.ManagedAppObject, app model.App) error {
	controlNamespace := strings.TrimSpace(s.Config.ControlPlaneNamespace)
	if controlNamespace == "" {
		return nil // No platform ingress is configured for this controller.
	}
	if len(validation.IsDNS1123Label(controlNamespace)) != 0 {
		return errors.New("public ingress control namespace is invalid")
	}
	name := runtime.RuntimeAppResourceName(app) + "-public-ingress"
	path := "/apis/networking.k8s.io/v1/namespaces/" + url.PathEscape(namespace) + "/networkpolicies/" + url.PathEscape(name)
	current, found, err := client.getRawObject(ctx, path)
	if err != nil {
		return err
	}
	if found && !publicIngressPolicyOwned(current, managed, app) {
		return errors.New("public ingress egress policy has a foreign owner")
	}
	if !appAllowsPublicIngressEgress(app) {
		if found {
			meta := objectMapField(current, "metadata")
			return client.deleteObjectWithUID(ctx, path, objectStringField(meta, "uid"), objectStringField(meta, "resourceVersion"))
		}
		return nil
	}
	var services struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
		Metadata   struct {
			Continue string `json:"continue"`
		} `json:"metadata"`
		Items []map[string]any `json:"items"`
	}
	selector := url.QueryEscape("app.kubernetes.io/managed-by in (" + publicIngressTransportManager + "," + publicIngressProbeManager + ")")
	if _, err := client.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+url.PathEscape(controlNamespace)+"/services?labelSelector="+selector, nil, &services); err != nil {
		return fmt.Errorf("read declared public ingress transport: %w", err)
	}
	if services.APIVersion != "v1" || services.Kind != "ServiceList" || services.Metadata.Continue != "" || services.Items == nil || len(services.Items) > 32 {
		return errors.New("public ingress Service observation is incomplete")
	}
	policy, err := publicIngressEgressPolicy(namespace, controlNamespace, managed, app, services.Items)
	if err != nil {
		return err // Preserve the previous policy on incomplete configuration reads.
	}
	if found && publicIngressPolicySpecEqual(current, policy) {
		return nil
	}
	if !found {
		_, err = client.doJSON(ctx, http.MethodPost, strings.TrimSuffix(path, "/"+url.PathEscape(name)), policy, nil)
		return err // Conflict requires a fresh ownership read, never force adoption.
	}
	meta := objectMapField(current, "metadata")
	if objectStringField(meta, "uid") == "" || objectStringField(meta, "resourceVersion") == "" {
		return errors.New("public ingress policy lacks an exact mutation identity")
	}
	patch := []map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": meta["uid"]},
		{"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]},
		{"op": "test", "path": "/metadata/ownerReferences", "value": meta["ownerReferences"]},
		{"op": "test", "path": "/spec", "value": current["spec"]},
		{"op": "replace", "path": "/spec", "value": policy["spec"]},
	}
	_, err = client.doRequest(ctx, http.MethodPatch, path, "application/json-patch+json", patch, nil)
	return err
}

func publicIngressPolicyOwned(policy map[string]any, managed runtime.ManagedAppObject, app model.App) bool {
	meta := objectMapField(policy, "metadata")
	labels := normalizeKubeStringMap(meta["labels"])
	if labels["app.kubernetes.io/managed-by"] != publicIngressEgressManager || labels["network.fugue.dev/app-id"] != app.ID {
		return false
	}
	owners, _ := meta["ownerReferences"].([]any)
	for _, raw := range owners {
		owner, _ := raw.(map[string]any)
		if owner["apiVersion"] == runtime.ManagedAppAPIVersion && owner["kind"] == runtime.ManagedAppKind && owner["uid"] == managed.Metadata.UID && owner["name"] == managed.Metadata.Name {
			return managed.Metadata.UID != ""
		}
	}
	return false
}

func publicIngressPolicySpecEqual(current, desired map[string]any) bool {
	left, err := json.Marshal(current["spec"])
	if err != nil {
		return false
	}
	right, err := json.Marshal(desired["spec"])
	return err == nil && string(left) == string(right)
}

func publicIngressEgressPolicy(namespace, controlNamespace string, managed runtime.ManagedAppObject, app model.App, services []map[string]any) (map[string]any, error) {
	if !appAllowsPublicIngressEgress(app) || app.ID == "" || managed.Metadata.UID == "" || managed.Metadata.Name == "" || namespace != runtime.NamespaceForTenant(app.TenantID) || namespace == controlNamespace {
		return nil, errors.New("public ingress egress requires an exact opted-in application identity")
	}
	rulesByJSON := map[string]map[string]any{}
	for _, service := range services {
		rule, err := publicIngressServiceRule(service, controlNamespace)
		if err != nil {
			return nil, err
		}
		if rule != nil {
			raw, _ := json.Marshal(rule)
			rulesByJSON[string(raw)] = rule
		}
	}
	keys := make([]string, 0, len(rulesByJSON))
	for key := range rulesByJSON {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rules := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		rules = append(rules, rulesByJSON[key])
	}
	return map[string]any{
		"apiVersion": runtime.KubernetesNetworkPolicyAPIVersion, "kind": "NetworkPolicy",
		"metadata": map[string]any{
			"name": runtime.RuntimeAppResourceName(app) + "-public-ingress", "namespace": namespace,
			// A separate owner prevents workload pruning from removing this
			// configuration during a failed application rollout. GC still binds
			// it to the exact ManagedApp UID, and opt-out deletes it explicitly.
			"labels":          map[string]string{"app.kubernetes.io/managed-by": publicIngressEgressManager, "network.fugue.dev/app-id": app.ID},
			"ownerReferences": []map[string]any{{"apiVersion": runtime.ManagedAppAPIVersion, "kind": runtime.ManagedAppKind, "name": managed.Metadata.Name, "uid": managed.Metadata.UID, "controller": false, "blockOwnerDeletion": true}},
		},
		"spec": map[string]any{"podSelector": map[string]any{"matchLabels": map[string]string{runtime.FugueLabelAppID: app.ID}}, "policyTypes": []string{"Egress"}, "egress": rules},
	}, nil
}

func publicIngressServiceRule(service map[string]any, namespace string) (map[string]any, error) {
	meta, spec := objectMapField(service, "metadata"), objectMapField(service, "spec")
	labels, annotations := normalizeKubeStringMap(meta["labels"]), normalizeKubeStringMap(meta["annotations"])
	if (service["apiVersion"] != nil && service["apiVersion"] != "v1") || (service["kind"] != nil && service["kind"] != "Service") || meta["namespace"] != namespace || (labels["app.kubernetes.io/managed-by"] != publicIngressTransportManager && labels["app.kubernetes.io/managed-by"] != publicIngressProbeManager) || meta["deletionTimestamp"] != nil || objectStringField(meta, "uid") == "" || objectStringField(meta, "resourceVersion") == "" {
		return nil, errors.New("public ingress Service identity is invalid")
	}
	probe := labels["app.kubernetes.io/managed-by"] == publicIngressProbeManager
	if !probe && annotations["transport.fugue.dev/phase"] == "staged" {
		return nil, nil
	}
	generation, err := strconv.ParseUint(annotations["transport.fugue.dev/generation"], 10, 64)
	addresses, ok := spec["externalIPs"].([]any)
	if err != nil || generation == 0 || (!probe && annotations["transport.fugue.dev/phase"] != "serving" || probe && annotations["transport.fugue.dev/phase"] != "") || spec["type"] != "ClusterIP" || spec["externalTrafficPolicy"] != "Local" || !ok || len(addresses) != 1 {
		return nil, errors.New("public ingress Service is not an explicit public serving declaration")
	}
	address, ok := addresses[0].(string)
	ip, err := netip.ParseAddr(address)
	if !ok || err != nil || !ip.Is4() || !platformconfig.PublicDNSFlattenIP(ip) {
		return nil, errors.New("public ingress address is not public")
	}
	selector := normalizeKubeStringMap(spec["selector"])
	if len(selector) == 0 || len(selector) > 16 {
		return nil, errors.New("public ingress requires an exact bounded pod selector")
	}
	for key, value := range selector {
		if len(validation.IsQualifiedName(key)) != 0 || len(validation.IsValidLabelValue(value)) != 0 || value == "" {
			return nil, errors.New("public ingress selector is invalid")
		}
	}
	ports, ok := spec["ports"].([]any)
	if !ok || (!probe && len(ports) != 2) || (probe && len(ports) != 1) {
		return nil, errors.New("public ingress ports must be HTTP and HTTPS only")
	}
	seen := map[int]bool{}
	for _, raw := range ports {
		p, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.New("public ingress port is invalid")
		}
		port := fmt.Sprint(p["port"])
		n, parseErr := strconv.Atoi(port)
		target := fmt.Sprint(p["targetPort"])
		if p["protocol"] != "TCP" || p["nodePort"] != nil || parseErr != nil ||
			(!probe && ((port != "80" && port != "443") || target != port)) ||
			(probe && (n < 1024 || n > 65535 || target != "443")) {
			return nil, errors.New("public ingress cannot expose private management ports")
		}
		if seen[n] {
			return nil, errors.New("public ingress port is duplicated")
		}
		seen[n] = true
	}
	projection := map[string]any{"type": spec["type"], "selector": spec["selector"], "ports": spec["ports"], "externalIPs": spec["externalIPs"], "externalTrafficPolicy": spec["externalTrafficPolicy"], "publishNotReadyAddresses": false}
	if value, exists := spec["publishNotReadyAddresses"]; exists && value != false {
		return nil, errors.New("public ingress cannot select unready Pods")
	}
	allowedPorts := []map[string]any{{"protocol": "TCP", "port": 80}, {"protocol": "TCP", "port": 443}}
	if probe {
		if spec["internalTrafficPolicy"] != "Local" {
			return nil, errors.New("public probe lacks declared local transport")
		}
		projection["internalTrafficPolicy"] = "Local"
		allowedPorts = []map[string]any{{"protocol": "TCP", "port": 443}}
	}
	raw, err := json.Marshal(projection)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	if annotations["transport.fugue.dev/digest"] != "sha256:"+hex.EncodeToString(digest[:]) {
		return nil, errors.New("public ingress Service differs from its declared digest")
	}
	return map[string]any{"to": []map[string]any{{"namespaceSelector": map[string]any{"matchLabels": map[string]string{"kubernetes.io/metadata.name": namespace}}, "podSelector": map[string]any{"matchLabels": selector}}}, "ports": allowedPorts}, nil
}

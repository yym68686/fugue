package controller

import (
	"context"
	"errors"
	networkingv1 "k8s.io/api/networking/v1"
	"net/http"
	"net/url"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// Reconcile before application release gates so an old or failed release does
// not leave a managed database exposed. The selector follows the CNPG instance pods.
func (s *Service) reconcilePostgresIngress(ctx context.Context, client *kubeClient, ns, cluster string, db model.BackingService) error {
	controlNS := s.Config.ControlPlaneNamespace
	if controlNS == "" {
		return nil
	}
	if db.TenantID == "" || runtime.NamespaceForTenant(db.TenantID) != ns || db.ID == "" {
		return errors.New("invalid managed database ownership")
	}
	ids, err := s.Store.PostgresConsumerIDs(ctx, db.TenantID, db.ID)
	if err != nil {
		return err
	}
	selector := func(labels map[string]string) map[string]any { return map[string]any{"matchLabels": labels} }
	instances := selector(map[string]string{"cnpg.io/cluster": cluster})
	peers := []map[string]any{{"podSelector": instances}}
	for _, id := range ids {
		peers = append(peers, map[string]any{"podSelector": selector(map[string]string{runtime.FugueLabelAppID: id, runtime.FugueLabelTenantID: db.TenantID})})
	}
	system := func(labels map[string]string) map[string]any {
		return map[string]any{"namespaceSelector": selector(map[string]string{"kubernetes.io/metadata.name": controlNS}), "podSelector": selector(labels)}
	}
	for _, component := range []string{"api", "controller"} {
		peers = append(peers, system(map[string]string{"app.kubernetes.io/name": "fugue", "app.kubernetes.io/component": component}))
	}
	rule := func(port int, from []map[string]any) map[string]any {
		return map[string]any{"ports": []map[string]any{{"protocol": "TCP", "port": port}}, "from": from}
	}
	name := "fugue-pg-ingress-" + cluster
	label := "network.fugue.dev/postgres-service-id"
	spec := map[string]any{"podSelector": instances, "policyTypes": []string{"Ingress"}, "ingress": []map[string]any{
		rule(5432, peers), rule(8000, []map[string]any{system(map[string]string{"app.kubernetes.io/name": "cloudnative-pg"})}),
		rule(9187, []map[string]any{system(map[string]string{"app.kubernetes.io/name": "fugue", "app.kubernetes.io/component": "observability-prometheus"})}),
	}}
	policy := map[string]any{"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": map[string]any{"name": name, "namespace": ns, "labels": map[string]string{label: db.ID}}, "spec": spec}
	collection := "/apis/networking.k8s.io/v1/namespaces/" + url.PathEscape(ns) + "/networkpolicies"
	path := collection + "/" + url.PathEscape(name)
	// Kubernetes adds together the allowances of overlapping policies. Yield to
	// an operator's cluster-specific isolation rule, including incident holds.
	var existing networkingv1.NetworkPolicyList
	if _, err := client.doJSON(ctx, http.MethodGet, collection, nil, &existing); err != nil {
		return err
	}
	externallyManaged := false
	for _, policy := range existing.Items {
		if policy.Name == name || policy.Spec.PodSelector.MatchLabels["cnpg.io/cluster"] != cluster {
			continue
		}
		if len(policy.Spec.PolicyTypes) == 0 {
			externallyManaged = true
		}
		for _, direction := range policy.Spec.PolicyTypes {
			if direction == networkingv1.PolicyTypeIngress {
				externallyManaged = true
			}
		}
	}
	current, found, err := client.getRawObject(ctx, path)
	if err != nil {
		return err
	}
	if externallyManaged {
		if found && objectLabels(current)[label] == db.ID {
			meta := objectMapField(current, "metadata")
			return client.deleteObjectWithUID(ctx, path, objectStringField(meta, "uid"), objectStringField(meta, "resourceVersion"))
		}
		return nil
	}
	if !found {
		_, err = client.doJSON(ctx, http.MethodPost, collection, policy, nil)
		return err
	}
	if objectLabels(current)[label] != db.ID {
		return errors.New("managed database network policy ownership conflict")
	}
	if publicIngressPolicySpecEqual(current, policy) {
		return nil
	}
	meta := objectMapField(current, "metadata")
	patch := []map[string]any{{"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]}, {"op": "test", "path": "/metadata/uid", "value": meta["uid"]}, {"op": "replace", "path": "/spec", "value": spec}}
	_, err = client.doRequest(ctx, http.MethodPatch, path, "application/json-patch+json", patch, nil)
	return err
}

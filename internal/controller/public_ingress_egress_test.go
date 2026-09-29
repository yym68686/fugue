package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/runtime"
	networkingv1 "k8s.io/api/networking/v1"
)

func publicIngressTestService() map[string]any {
	var service map[string]any
	_ = json.Unmarshal([]byte(`{"apiVersion":"v1","kind":"Service","metadata":{"name":"public-front","namespace":"platform-system","uid":"service-one","resourceVersion":"9","labels":{"app.kubernetes.io/managed-by":"fugue-front-serving-transport"},"annotations":{"transport.fugue.dev/phase":"serving","transport.fugue.dev/generation":"2"}},"spec":{"type":"ClusterIP","selector":{"role":"public-front","node":"node-a"},"ports":[{"name":"http","protocol":"TCP","port":80,"targetPort":80},{"name":"https","protocol":"TCP","port":443,"targetPort":443}],"externalIPs":["8.8.8.8"],"externalTrafficPolicy":"Local","publishNotReadyAddresses":false}}`), &service)
	raw, _ := json.Marshal(service["spec"])
	digest := sha256.Sum256(raw)
	objectMapField(objectMapField(service, "metadata"), "annotations")["transport.fugue.dev/digest"] = "sha256:" + hex.EncodeToString(digest[:])
	return service
}

func publicIngressTestApp() (model.App, runtime.ManagedAppObject) {
	app := model.App{ID: "app-one", TenantID: "tenant-one", Name: "sample", Spec: model.AppSpec{NetworkPolicy: &model.AppNetworkPolicySpec{Egress: &model.AppNetworkPolicyDirectionSpec{Mode: model.AppNetworkPolicyModeRestricted, AllowPublicInternet: true}}}}
	managed := runtime.ManagedAppObject{}
	managed.Metadata.Name, managed.Metadata.UID = "app-one", "managed-one"
	return app, managed
}

func TestPublicIngressEgressAllowsOnlyPublishedFrontPortsAndNamespace(t *testing.T) {
	app, managed := publicIngressTestApp()
	service := publicIngressTestService()
	object, err := publicIngressEgressPolicy(runtime.NamespaceForTenant(app.TenantID), "platform-system", managed, app, []map[string]any{service, service})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(object)
	var policy networkingv1.NetworkPolicy
	if err := json.Unmarshal(raw, &policy); err != nil {
		t.Fatal(err)
	}
	if len(policy.Spec.Egress) != 1 || len(policy.Spec.Egress[0].To) != 1 || len(policy.Spec.Egress[0].Ports) != 2 || len(policy.Spec.Ingress) != 0 {
		t.Fatalf("unexpected permissions: %+v", policy.Spec)
	}
	peer := policy.Spec.Egress[0].To[0]
	if peer.IPBlock != nil || peer.NamespaceSelector == nil || peer.PodSelector == nil || peer.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] != "platform-system" || len(peer.PodSelector.MatchLabels) != 2 || peer.PodSelector.MatchLabels["node"] != "node-a" {
		t.Fatalf("private destinations are not isolated: %+v", peer)
	}
	if policy.Spec.Egress[0].Ports[0].Port.IntValue() != 80 || policy.Spec.Egress[0].Ports[1].Port.IntValue() != 443 {
		t.Fatal("management port exposed")
	}
	if policy.Spec.PodSelector.MatchLabels[runtime.FugueLabelAppID] != app.ID || policy.Labels[runtime.FugueLabelAppID] != "" || len(policy.OwnerReferences) != 1 || string(policy.OwnerReferences[0].UID) != managed.Metadata.UID {
		t.Fatal("companion policy lost exact owner or entered workload pruning")
	}
	var live map[string]any
	_ = json.Unmarshal(raw, &live)
	if !publicIngressPolicyOwned(live, managed, app) {
		t.Fatal("exact owner not recognized")
	}
	managed.Metadata.UID = "replacement"
	if publicIngressPolicyOwned(live, managed, app) {
		t.Fatal("recreated ManagedApp inherited foreign policy")
	}
}

func TestPublicIngressServiceRejectsDriftAndPrivateOrBroadTargets(t *testing.T) {
	for name, change := range map[string]func(map[string]any){
		"private IP":        func(s map[string]any) { objectMapField(s, "spec")["externalIPs"] = []any{"10.0.0.2"} },
		"foreign namespace": func(s map[string]any) { objectMapField(s, "metadata")["namespace"] = "tenant-other" },
		"foreign owner": func(s map[string]any) {
			objectMapField(objectMapField(s, "metadata"), "labels")["app.kubernetes.io/managed-by"] = "foreign"
		},
		"empty selector": func(s map[string]any) { objectMapField(s, "spec")["selector"] = map[string]any{} },
		"selector drift": func(s map[string]any) { objectMapField(s, "spec")["selector"] = map[string]any{"role": "database"} },
		"management port": func(s map[string]any) {
			objectMapField(s, "spec")["ports"].([]any)[1].(map[string]any)["targetPort"] = 7831
		},
		"UDP": func(s map[string]any) {
			objectMapField(s, "spec")["ports"].([]any)[1].(map[string]any)["protocol"] = "UDP"
		},
		"unready": func(s map[string]any) { objectMapField(s, "spec")["publishNotReadyAddresses"] = true },
		"missing digest": func(s map[string]any) {
			delete(objectMapField(objectMapField(s, "metadata"), "annotations"), "transport.fugue.dev/digest")
		},
		"missing generation": func(s map[string]any) {
			delete(objectMapField(objectMapField(s, "metadata"), "annotations"), "transport.fugue.dev/generation")
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := publicIngressTestService()
			change(s)
			if _, err := publicIngressServiceRule(s, "platform-system"); err == nil {
				t.Fatal("invalid public transport widened private egress")
			}
		})
	}
	staged := publicIngressTestService()
	objectMapField(objectMapField(staged, "metadata"), "annotations")["transport.fugue.dev/phase"] = "staged"
	if rule, err := publicIngressServiceRule(staged, "platform-system"); err != nil || rule != nil {
		t.Fatal("staged transport granted egress")
	}
}

func TestPublicIngressEgressRequiresOptIn(t *testing.T) {
	app, managed := publicIngressTestApp()
	for _, mode := range []string{"no policy", "no egress", "public disabled", "unrestricted"} {
		candidate := app
		policy := *app.Spec.NetworkPolicy
		egress := *policy.Egress
		policy.Egress = &egress
		candidate.Spec.NetworkPolicy = &policy
		switch mode {
		case "no policy":
			candidate.Spec.NetworkPolicy = nil
		case "no egress":
			policy.Egress = nil
		case "public disabled":
			egress.AllowPublicInternet = false
		case "unrestricted":
			egress.Mode = "unrestricted"
		}
		if _, err := publicIngressEgressPolicy(runtime.NamespaceForTenant(app.TenantID), "platform-system", managed, candidate, []map[string]any{publicIngressTestService()}); err == nil {
			t.Fatalf("%s granted private egress", mode)
		}
	}
}

func TestPublicIngressRawAPIListItemsMayOmitTypeMeta(t *testing.T) {
	service := publicIngressTestService()
	delete(service, "apiVersion")
	delete(service, "kind")
	if _, err := publicIngressServiceRule(service, "platform-system"); err != nil {
		t.Fatalf("raw v1 ServiceList item rejected: %v", err)
	}
	service["kind"] = "Pod"
	if _, err := publicIngressServiceRule(service, "platform-system"); err == nil {
		t.Fatal("contradictory item kind accepted")
	}
}

func TestPublicIngressReconcileReadsRawServiceListAndOnlyCreatesNetworkPolicy(t *testing.T) {
	app, managed := publicIngressTestApp()
	namespace := runtime.NamespaceForTenant(app.TenantID)
	service := publicIngressTestService()
	delete(service, "kind")
	delete(service, "apiVersion")
	writes := 0
	var created map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/networkpolicies/"):
			if created == nil {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(created)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/platform-system/services":
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "ServiceList", "metadata": map[string]any{}, "items": []any{service}})
		case r.Method == http.MethodPost && r.URL.Path == "/apis/networking.k8s.io/v1/namespaces/"+namespace+"/networkpolicies":
			writes++
			_ = json.NewDecoder(r.Body).Decode(&created)
			meta := objectMapField(created, "metadata")
			meta["uid"] = "policy-one"
			meta["resourceVersion"] = "1"
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(created)
		default:
			t.Errorf("unexpected Kubernetes operation %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer server.Close()
	client := &kubeClient{client: server.Client(), baseURL: server.URL}
	s := &Service{Config: config.ControllerConfig{ControlPlaneNamespace: "platform-system"}}
	for i := 0; i < 2; i++ {
		if err := s.reconcilePublicIngressEgress(context.Background(), client, namespace, managed, app); err != nil {
			t.Fatal(err)
		}
	}
	if writes != 1 || created["kind"] != "NetworkPolicy" {
		t.Fatalf("reconcile did not converge without workload writes: writes=%d object=%+v", writes, created)
	}
}

func TestPublicProbeEgressAllowsOnlyItsPublishedTLSBackend(t *testing.T) {
	service := publicIngressTestService()
	meta, spec := objectMapField(service, "metadata"), objectMapField(service, "spec")
	objectMapField(meta, "labels")["app.kubernetes.io/managed-by"] = publicIngressProbeManager
	annotations := objectMapField(meta, "annotations")
	delete(annotations, "transport.fugue.dev/phase")
	spec["internalTrafficPolicy"] = "Local"
	spec["ports"] = []any{map[string]any{"name": "https-probe", "protocol": "TCP", "port": 15443, "targetPort": 443}}
	raw, _ := json.Marshal(spec)
	digest := sha256.Sum256(raw)
	annotations["transport.fugue.dev/digest"] = "sha256:" + hex.EncodeToString(digest[:])
	rule, err := publicIngressServiceRule(service, "platform-system")
	if err != nil {
		t.Fatal(err)
	}
	ports := rule["ports"].([]map[string]any)
	if len(ports) != 1 || ports[0]["port"] != 443 {
		t.Fatalf("probe widened its backend ports: %+v", ports)
	}
	spec["ports"].([]any)[0].(map[string]any)["targetPort"] = 7831
	if _, err := publicIngressServiceRule(service, "platform-system"); err == nil {
		t.Fatal("probe enabled management port")
	}
}

func TestPublicIngressEgressReadFailurePreservesPolicyAndOptOutDeletesExactOwner(t *testing.T) {
	for _, optOut := range []bool{false, true} {
		app, managed := publicIngressTestApp()
		namespace := runtime.NamespaceForTenant(app.TenantID)
		object, _ := publicIngressEgressPolicy(namespace, "platform-system", managed, app, []map[string]any{publicIngressTestService()})
		meta := objectMapField(object, "metadata")
		meta["uid"] = "policy-one"
		meta["resourceVersion"] = "14"
		if optOut {
			app.Spec.NetworkPolicy.Egress.AllowPublicInternet = false
		}
		writes := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/networkpolicies/") {
				_ = json.NewEncoder(w).Encode(object)
				return
			}
			if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/services") {
				http.Error(w, "observation unavailable", 503)
				return
			}
			writes++
			if !optOut || r.Method != http.MethodDelete {
				t.Errorf("unexpected write %s %s", r.Method, r.URL.Path)
				return
			}
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			conditions := objectMapField(body, "preconditions")
			if conditions["uid"] != "policy-one" || conditions["resourceVersion"] != "14" {
				t.Errorf("missing exact delete preconditions: %+v", body)
			}
			_, _ = w.Write([]byte(`{}`))
		}))
		client := &kubeClient{client: server.Client(), baseURL: server.URL}
		s := &Service{Config: config.ControllerConfig{ControlPlaneNamespace: "platform-system"}}
		err := s.reconcilePublicIngressEgress(context.Background(), client, namespace, managed, app)
		server.Close()
		if optOut && (err != nil || writes != 1) || !optOut && (err == nil || writes != 0) {
			t.Fatalf("optOut=%t error=%v writes=%d", optOut, err, writes)
		}
	}
}

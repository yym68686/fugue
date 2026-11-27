package participation

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/rest"
)

func TestObserveParticipationMemberObjectsOverTLS(t *testing.T) {
	const ns = "fixture"
	for _, tc := range []struct {
		mode    string
		serving bool
		wantOK  bool
	}{
		{"serving", true, true}, {"retained", false, true},
		{"replaced Node", true, false}, {"incomplete group", true, false},
		{"ambiguous service", true, false}, {"incomplete services", true, false},
		{"service mismatch", true, false}, {"service drift", true, false},
		{"wrong rollout strategy", false, false},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			node := corev1.Node{
				TypeMeta: metav1.TypeMeta{Kind: "Node", APIVersion: "v1"},
				ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: "node-a-uid", ResourceVersion: "10",
					Labels: map[string]string{participationGroup: "cell-a", participationRole: "true"}},
				Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown}}},
			}
			if tc.mode == "replaced Node" {
				node.UID = "replacement-uid"
			}
			const projection = `{"externalIPs":["192.0.2.8"],"externalTrafficPolicy":"Local","ports":[{"name":"https","port":443,"protocol":"TCP","targetPort":8443}],"publishNotReadyAddresses":false,"selector":{"fugue.io/edge-node":"node-a"},"type":"ClusterIP"}`
			service := corev1.Service{
				TypeMeta: metav1.TypeMeta{Kind: "Service", APIVersion: "v1"},
				ObjectMeta: metav1.ObjectMeta{Name: "front-service", Namespace: ns, UID: "service-uid", ResourceVersion: "20",
					Labels:      map[string]string{frontManagerKey: frontManager},
					Annotations: map[string]string{frontPhase: "serving", frontGeneration: "4", frontDigest: digest([]byte(projection))}},
				Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal,
					Selector: map[string]string{frontNodeKey: node.Name}, ExternalIPs: []string{"192.0.2.8"},
					Ports: []corev1.ServicePort{{Name: "https", Protocol: corev1.ProtocolTCP, Port: 443, TargetPort: intstr.FromInt(8443)}}},
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet {
					t.Errorf("unexpected %s %s", req.Method, req.URL.Path)
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				var out any
				switch path := req.URL.Path; {
				case path == "/api/v1/nodes/node-a":
					out = node
				case path == "/api/v1/nodes":
					if req.URL.Query().Get("labelSelector") != participationGroup+"=cell-a" || req.URL.Query().Get("limit") != "1001" {
						t.Errorf("unexpected group query: %s", req.URL.RawQuery)
					}
					group := corev1.NodeList{TypeMeta: metav1.TypeMeta{Kind: "NodeList", APIVersion: "v1"},
						ListMeta: metav1.ListMeta{ResourceVersion: "14"}, Items: []corev1.Node{node}}
					if tc.mode == "incomplete group" {
						group.Continue = "next"
					}
					out = group
				case path == "/api/v1/namespaces/fixture/services":
					if req.URL.Query().Get("labelSelector") != frontManagerKey+"="+frontManager || req.URL.Query().Get("limit") != "257" {
						t.Errorf("unexpected service query: %s", req.URL.RawQuery)
					}
					services := corev1.ServiceList{TypeMeta: metav1.TypeMeta{Kind: "ServiceList", APIVersion: "v1"},
						ListMeta: metav1.ListMeta{ResourceVersion: "21"}}
					if tc.serving {
						services.Items = append(services.Items, service)
					}
					if tc.mode == "ambiguous service" {
						services.Items = append(services.Items, service)
					}
					if tc.mode == "incomplete services" {
						services.Continue = "next"
					}
					out = services
				case path == "/api/v1/namespaces/fixture/services/front-service":
					s := service.DeepCopy()
					if tc.mode == "service mismatch" {
						s.Spec.Ports[0].TargetPort = intstr.FromInt(9443)
					}
					if tc.mode == "service drift" {
						s.ResourceVersion = "22"
					}
					out = s
				case strings.HasPrefix(path, "/apis/apps/v1/namespaces/fixture/daemonsets/"):
					name := strings.TrimPrefix(path, "/apis/apps/v1/namespaces/fixture/daemonsets/")
					d := appsv1.DaemonSet{TypeMeta: metav1.TypeMeta{Kind: "DaemonSet", APIVersion: "apps/v1"},
						ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(name + "-uid"), ResourceVersion: "30"},
						Spec: appsv1.DaemonSetSpec{UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType},
							Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{participationGroup: "cell-a"}}}}}
					if tc.mode == "wrong rollout strategy" {
						d.Spec.UpdateStrategy.Type = appsv1.RollingUpdateDaemonSetStrategyType
					}
					out = d
				default:
					t.Errorf("unexpected request: %s", path)
					http.NotFound(w, req)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(out); err != nil {
					t.Errorf("writing response: %v", err)
				}
			}))
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			observer, err := NewKubernetesObserver(&rest.Config{Host: server.URL, TLSClientConfig: rest.TLSClientConfig{CAData: ca}, Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			policy := productioncontrol.ParticipationPolicy{Namespace: ns, Members: []productioncontrol.ParticipationMember{{
				Node: "node-a", NodeUID: "node-a-uid", Group: "cell-a", Front: "front", Workers: productioncontrol.ParticipationWorkers{A: "worker-a", B: "worker-b"},
			}}}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			got, err := observer.ObserveMemberObjects(ctx, policy, "node-a")
			if !tc.wantOK {
				if err == nil || got.Node.UID != "" {
					t.Fatalf("unverified observation: %+v, %v", got, err)
				}
				return
			}
			if err != nil || got.Node.UID != "node-a-uid" || got.Readiness != corev1.ConditionUnknown || got.DNSObserved || len(got.Nodes) != 1 {
				t.Fatalf("unexpected observation: %+v, %v", got, err)
			}
			if tc.serving {
				if got.Service == nil || got.Service.Generation != 4 || got.Service.Address != "192.0.2.8" || len(got.Retained) != 0 {
					t.Fatalf("missing serving Service observation: %+v", got)
				}
			} else if got.Service != nil || len(got.Retained) != 3 {
				t.Fatalf("missing retained workload observation: %+v", got)
			}
		})
	}
}

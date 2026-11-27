package participation

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	admission "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
)

func TestObserveNodeFreezeReadsExactStoredConfiguration(t *testing.T) {
	const node = "member-a"
	const path = "/apis/admissionregistration.k8s.io/v1/"
	for _, tc := range []struct {
		mode        string
		wantMatch   bool
		typeChecked bool
	}{
		{"exact", true, true},
		{"not type checked", true, false},
		{"missing policy", false, false},
		{"missing binding", false, false},
		{"policy fail open", false, false},
		{"policy narrowed", false, false},
		{"binding narrowed", false, false},
		{"binding does not deny", false, false},
		{"expression warning", false, false},
		{"replaced binding", false, false},
		{"binding being deleted", false, false},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			wantPolicy, wantBinding, err := FreezeNodePolicy(node)
			if err != nil {
				t.Fatal(err)
			}
			var policyReads, bindingReads atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet || req.URL.RawQuery != "" || req.Body != http.NoBody {
					t.Errorf("unexpected request to admission API: %s %s", req.Method, req.URL.Path)
					http.Error(w, "expected ordinary GET", http.StatusBadRequest)
					return
				}
				var payload any
				switch req.URL.Path {
				case path + "validatingadmissionpolicies/" + wantPolicy.Name:
					policyReads.Add(1)
					p := wantPolicy.DeepCopy()
					p.UID, p.ResourceVersion, p.Generation = "policy-a", "5", 2
					p.Spec.MatchConstraints.ObjectSelector = &metav1.LabelSelector{}
					p.Spec.MatchConstraints.NamespaceSelector = &metav1.LabelSelector{}
					p.Status = admission.ValidatingAdmissionPolicyStatus{ObservedGeneration: 2, TypeChecking: &admission.TypeChecking{}}
					switch tc.mode {
					case "not type checked":
						p.Status.ObservedGeneration = 1
					case "missing policy":
						http.NotFound(w, req)
						return
					case "policy fail open":
						p.Spec.FailurePolicy = ptr.To(admission.Ignore)
					case "policy narrowed":
						p.Spec.MatchConstraints.ObjectSelector.MatchLabels = map[string]string{"fixture.example/skip-unlabeled": "true"}
					case "expression warning":
						p.Status.TypeChecking.ExpressionWarnings = []admission.ExpressionWarning{{FieldRef: "spec.validations[0].expression", Warning: "fixture warning"}}
					}
					payload = p
				case path + "validatingadmissionpolicybindings/" + wantBinding.Name:
					attempt := bindingReads.Add(1)
					b := wantBinding.DeepCopy()
					b.UID, b.ResourceVersion = "binding-a", "10"
					b.Spec.ValidationActions = []admission.ValidationAction{admission.Audit, admission.Deny}
					switch tc.mode {
					case "missing binding":
						http.NotFound(w, req)
						return
					case "binding narrowed":
						b.Spec.MatchResources = &admission.MatchResources{ObjectSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"fixture.example/filter": "true"}}}
					case "binding does not deny":
						b.Spec.ValidationActions = []admission.ValidationAction{admission.Audit}
					case "replaced binding":
						if attempt == 2 {
							b.UID, b.ResourceVersion = "binding-b", "11"
						}
					case "binding being deleted":
						b.DeletionTimestamp = ptr.To(metav1.Now())
					}
					payload = b
				default:
					t.Errorf("unexpected admission endpoint: %s", req.URL.Path)
					http.NotFound(w, req)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(payload); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			observer, err := NewKubernetesObserver(&rest.Config{Host: server.URL, TLSClientConfig: rest.TLSClientConfig{CAData: ca}})
			if err != nil {
				t.Fatal(err)
			}
			policy := productioncontrol.ParticipationPolicy{Namespace: "fixture", Members: []productioncontrol.ParticipationMember{
				{Node: node, NodeUID: "member-a-uid", Group: "cell-a"},
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			got, err := observer.ObserveNodeFreeze(ctx, policy, node)
			if !tc.wantMatch {
				if err == nil || got != (FreezeObservation{}) {
					t.Fatalf("unproven freeze produced an observation: %+v, %v", got, err)
				}
				return
			}
			if err != nil || got.Node != node || got.Policy.UID != "policy-a" || got.Binding.UID != "binding-a" ||
				got.CurrentTypeChecked != tc.typeChecked || policyReads.Load() != 2 || bindingReads.Load() != 2 {
				t.Fatalf("wrong stored freeze observation: %+v, %v (reads: %d, %d)", got, err, policyReads.Load(), bindingReads.Load())
			}
		})
	}
}

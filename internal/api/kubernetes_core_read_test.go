package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
)

func coreReadWire(t *testing.T, obj kuberuntime.Object) []byte {
	t.Helper()
	var wire bytes.Buffer
	if err := clusterCoreProtobuf.Encode(obj, &wire); err != nil {
		t.Fatal(err)
	}
	return wire.Bytes()
}

func TestKubernetesNodeReadNegotiatesWithoutChangingObservation(t *testing.T) {
	original, _ := agentCapacityFixture("edge-a", "192.0.2.8", time.Now().UTC().Truncate(time.Second))
	original.APIVersion, original.Kind, original.ResourceVersion = "v1", "Node", "17"
	proto := coreReadWire(t, &original)
	original.TypeMeta = metav1.TypeMeta{}
	// The wire envelope names a kind; the protobuf payload does not.
	for _, mode := range []string{"protobuf", "json", "fallback", "forbidden", "wrong_kind", "damaged"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			wrongKind := coreReadWire(t, &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}})
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != http.MethodGet || r.URL.RequestURI() != "/api/v1/nodes/edge-a" || r.Header.Get("Authorization") != "Bearer fixture" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.RequestURI())
				}
				if calls == 1 && !strings.HasPrefix(r.Header.Get("Accept"), kuberuntime.ContentTypeProtobuf) ||
					calls == 2 && r.Header.Get("Accept") != kuberuntime.ContentTypeJSON {
					t.Errorf("unexpected negotiation: %q", r.Header.Get("Accept"))
				}
				switch mode {
				case "fallback":
					if calls == 1 {
						w.WriteHeader(http.StatusNotAcceptable)
						return
					}
					fallthrough
				case "json":
					_ = json.NewEncoder(w).Encode(original)
				case "forbidden":
					w.WriteHeader(http.StatusForbidden)
				default:
					w.Header().Set("Content-Type", kuberuntime.ContentTypeProtobuf)
					switch mode {
					case "wrong_kind":
						_, _ = w.Write(wrongKind)
					case "damaged":
						_, _ = w.Write(proto[:6])
					default:
						_, _ = w.Write(proto)
					}
				}
			}))
			defer ts.Close()
			client := &clusterNodeClient{baseURL: ts.URL, client: ts.Client(), bearerToken: "fixture"}
			var got corev1.Node
			err := client.getKubernetesNode(t.Context(), "/api/v1/nodes/edge-a", &got)
			wantCalls := 1
			if mode == "fallback" {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("calls=%d, want %d", calls, wantCalls)
			}
			switch mode {
			case "protobuf", "json", "fallback":
				// Quantity formatting/cache and timezone representation are
				// immaterial; compare the JSON-visible Node data.
				wantJSON, _ := json.Marshal(original)
				gotJSON, _ := json.Marshal(got)
				if err != nil || !bytes.Equal(wantJSON, gotJSON) {
					t.Fatalf("Node projection differs: err=%v, actual=%s", err, gotJSON)
				}
			default:
				if err == nil {
					t.Fatal("bad Node read succeeded")
				}
			}
		})
	}
}

func TestKubernetesObjectRevisionFailsClosed(t *testing.T) {
	original := metav1.ObjectMeta{Name: "pod-a", UID: "uid-1", ResourceVersion: "25"}
	meta := metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "PartialObjectMetadata"}, ObjectMeta: original,
	}
	changed := meta
	changed.ResourceVersion = "26"
	proto, different := coreReadWire(t, &meta), coreReadWire(t, &changed)
	for _, mode := range []string{"protobuf", "changed", "fallback", "full_json", "forbidden", "blank_rv", "wrong_kind"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if calls == 1 && r.Header.Get("Accept") != kubernetesMetadataAccept || calls == 2 && r.Header.Get("Accept") != "application/json" ||
					r.URL.RequestURI() != "/api/v1/namespaces/fugue-system/pods/pod-a" {
					t.Errorf("unexpected metadata request: %s %q", r.URL.RequestURI(), r.Header.Get("Accept"))
				}
				switch mode {
				case "protobuf", "changed":
					w.Header().Set("Content-Type", kuberuntime.ContentTypeProtobuf)
					if mode == "changed" {
						_, _ = w.Write(different)
					} else {
						_, _ = w.Write(proto)
					}
				case "fallback":
					if calls == 1 {
						w.WriteHeader(http.StatusNotAcceptable)
						return
					}
					fallthrough
				case "full_json":
					_ = json.NewEncoder(w).Encode(corev1.Pod{TypeMeta: metav1.TypeMeta{Kind: "Pod"}, ObjectMeta: original})
				case "forbidden":
					w.WriteHeader(http.StatusForbidden)
				case "blank_rv":
					_ = json.NewEncoder(w).Encode(corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: original.UID}})
				case "wrong_kind":
					_ = json.NewEncoder(w).Encode(corev1.Node{TypeMeta: metav1.TypeMeta{Kind: "Node"}, ObjectMeta: original})
				}
			}))
			defer ts.Close()
			c := &clusterNodeClient{baseURL: ts.URL, client: ts.Client()}
			var got kubernetesObjectRevision
			err := c.getKubernetesObjectRevision(t.Context(), "/api/v1/namespaces/fugue-system/pods/pod-a", "Pod", &got)
			wantMatch := mode == "protobuf" || mode == "full_json" || mode == "fallback"
			wantErr := mode == "forbidden" || mode == "wrong_kind"
			if got.matches(&original) != wantMatch || (err != nil) != wantErr ||
				(calls == 2) != (mode == "fallback") {
				t.Fatalf("unexpected match/calls: %+v err=%v calls=%d", got, err, calls)
			}
		})
	}
}

func TestNodeCapacitySummaryOnlyRetriesExplicitRejection(t *testing.T) {
	_, sample := agentCapacityFixture("edge-a", "192.0.2.8", time.Now())
	for _, tc := range []struct{ first, calls int }{
		{200, 1}, {400, 2}, {406, 2}, {401, 1}, {403, 1}, {503, 1},
	} {
		t.Run(http.StatusText(tc.first), func(t *testing.T) {
			calls := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				query := "only_cpu_and_memory=true"
				if calls > 1 {
					query = ""
				}
				if r.URL.RawQuery != query || r.URL.Path != "/api/v1/nodes/edge-a/proxy/stats/summary" {
					t.Errorf("unexpected stats query: %s", r.URL.RequestURI())
				}
				if calls == 1 {
					w.WriteHeader(tc.first)
					if tc.first != http.StatusOK {
						return
					}
				}
				_ = json.NewEncoder(w).Encode(sample)
			}))
			defer ts.Close()
			c := &clusterNodeClient{baseURL: ts.URL, client: ts.Client()}
			got, err := c.getNodeCapacitySummary(t.Context(), "edge-a")
			success := tc.first == 200 || tc.calls == 2
			if (err == nil) != success || calls != tc.calls || success && !reflect.DeepEqual(got, &sample.Node) {
				t.Fatalf("unexpected summary result: %+v err=%v calls=%d", got, err, calls)
			}
		})
	}
}

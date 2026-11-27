package participation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model/edgecohort"
	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

type nodePatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// offlineHoldPatch builds a narrow provider CAS after matching the last
// live Node read to the exact reviewed predecessor. This is not authority
// to execute; the caller would still need a fresh preflight, an unspent
// journal dispatch reservation and independently proven write ownership.
func offlineHoldPatch(s productioncontrol.Submission, live *corev1.Node) (holdIntentPayload, []byte, error) {
	p, err := decodeHold(s)
	if err != nil || p.Phase != ParticipationOffline || len(validation.IsDNS1123Subdomain(p.To.Node)) != 0 ||
		live == nil || !exactLiveObject(live, "", p.To.Node) || string(live.UID) != p.To.UID ||
		live.ResourceVersion != p.ResourceVersion || live.Labels[participationGroup] != p.To.Group ||
		live.Labels[participationRole] != "true" ||
		HoldStateDigest(live.Name, string(live.UID), p.To.Group, live.Annotations) != s.Intent.CurrentDigest {
		return holdIntentPayload{}, nil, errors.New("offline hold does not match the exact live Node and verified predecessor")
	}
	patch := []nodePatchOperation{
		{Op: "test", Path: "/metadata/uid", Value: p.To.UID},
		{Op: "test", Path: "/metadata/resourceVersion", Value: p.ResourceVersion},
	}
	// An empty annotation map may be absent from the API wire object.
	// Adding the parent is safe only at this exact observed version.
	if len(live.Annotations) == 0 {
		patch = append(patch, nodePatchOperation{Op: "add", Path: "/metadata/annotations", Value: p.To.Annotations})
	} else {
		for _, key := range []string{participationAnnotation, edgecohort.ReleaseHoldAnnotation} {
			pointer := strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
			patch = append(patch, nodePatchOperation{Op: "add", Path: "/metadata/annotations/" + pointer, Value: *p.To.Annotations[key]})
		}
	}
	body, err := json.Marshal(patch)
	return p, body, err
}

// writeOfflineHoldOnce is intentionally not wired to a command or controller.
// After a request is sent, no error authorizes a replay or settlement.
// The result is a provider response, not an independent execution witness.
func writeOfflineHoldOnce(ctx context.Context, config *rest.Config, s productioncontrol.Submission, live *corev1.Node) (*corev1.Node, error) {
	p, body, err := offlineHoldPatch(s, live)
	if err != nil {
		return nil, err
	}
	client, err := offlineHoldClient(config)
	if err != nil {
		return nil, err
	}
	var observed corev1.Node
	err = client.Patch(types.JSONPatchType).Resource("nodes").Name(p.To.Node).
		MaxRetries(0).Body(body).Do(ctx).Into(&observed)
	if err != nil {
		return nil, fmt.Errorf("offline hold was not verified; leave the claim for independent reconciliation: %w", err)
	}
	if !exactLiveObject(&observed, "", p.To.Node) || string(observed.UID) != p.To.UID ||
		observed.ResourceVersion == p.ResourceVersion || observed.Labels[participationGroup] != p.To.Group ||
		observed.Labels[participationRole] != "true" ||
		HoldStateDigest(observed.Name, string(observed.UID), p.To.Group, observed.Annotations) != s.Intent.TargetDigest ||
		!sameUnownedNodeAnnotations(live.Annotations, observed.Annotations) {
		return nil, errors.New("offline hold response did not verify the exact changed Node; leave the claim for independent reconciliation")
	}
	return &observed, nil
}

func offlineHoldClient(config *rest.Config) (rest.Interface, error) {
	if config == nil || config.Insecure || config.ExecProvider != nil || config.AuthProvider != nil ||
		config.WrapTransport != nil || config.Transport != nil || config.Proxy != nil || config.Dial != nil || config.ServerName != "" ||
		config.Impersonate.UserName != "" || config.Impersonate.UID != "" || len(config.Impersonate.Groups) != 0 ||
		len(config.Impersonate.Extra) != 0 {
		return nil, errors.New("offline hold requires a dedicated verifiable Kubernetes identity and TLS transport")
	}
	u, err := url.Parse(config.Host)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Path != "" ||
		u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || strings.Contains(config.Host, "#") {
		return nil, errors.New("offline hold requires one HTTPS Kubernetes origin")
	}
	cfg := rest.CopyConfig(config)
	cfg.NextProtos = []string{"http/1.1"}
	tlsConfig, err := rest.TLSConfigFor(cfg)
	if err != nil {
		return nil, errors.New("offline hold Kubernetes transport configuration is unavailable")
	}
	// Fresh HTTP/1.1 connection only: neither reused-connection retry nor
	// HTTP/2 stream retry is appropriate for this write.
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	transport := &http.Transport{Protocols: protocols, TLSClientConfig: tlsConfig, DisableKeepAlives: true}
	wrapped, err := rest.HTTPWrappersForConfig(cfg, transport)
	if err != nil {
		return nil, errors.New("offline hold Kubernetes authentication configuration is unavailable")
	}
	timeout := cfg.Timeout
	if timeout <= 0 || timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	singleAttempt := &http.Client{Transport: wrapped, Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	typed, err := coreclient.NewForConfigAndClient(cfg, singleAttempt)
	if err != nil {
		return nil, errors.New("offline hold Kubernetes client configuration is unavailable")
	}
	return typed.RESTClient(), nil
}

func sameUnownedNodeAnnotations(a, b map[string]string) bool {
	for k, value := range a {
		if k == participationAnnotation || k == edgecohort.ReleaseHoldAnnotation {
			continue
		}
		if other, found := b[k]; !found || value != other {
			return false
		}
	}
	for k := range b {
		if _, found := a[k]; !found && k != participationAnnotation && k != edgecohort.ReleaseHoldAnnotation {
			return false
		}
	}
	return true
}

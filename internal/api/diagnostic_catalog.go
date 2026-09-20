package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/livediagnostics"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var errDiagnosticCatalogAbsent = errors.New("diagnostic catalog has not been published")

type diagnosticCatalogBackend interface {
	DiagnosticCatalog(context.Context) (livediagnostics.VerifiedCatalog, error)
}

func loadBackendDiagnosticCatalog(ctx context.Context, b diagnosticSessionBackend) (livediagnostics.VerifiedCatalog, error) {
	reader, ok := b.(diagnosticCatalogBackend)
	if !ok {
		return livediagnostics.VerifiedCatalog{}, errDiagnosticCatalogAbsent
	}
	return reader.DiagnosticCatalog(ctx)
}
func (s *Server) handleListPlatformDiagnosticProbes(w http.ResponseWriter, r *http.Request) {
	if _, ok := requirePlatformAdmin(w, r); !ok {
		return
	}
	b, err := s.newDiagnosticSessionBackend()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	catalog, err := loadBackendDiagnosticCatalog(r.Context(), b)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"source_revision": catalog.SourceRevision, "catalog_digest": catalog.Digest, "catalog_status": catalog.Status, "runner_image": catalog.RunnerImage, "probes": catalog.Descriptors()})
}
func (b *kubeDiagnosticSessionBackend) DiagnosticCatalog(ctx context.Context) (livediagnostics.VerifiedCatalog, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	catalog, err := b.diagnosticConfigMap(ctx, livediagnostics.CatalogConfigMap)
	if err != nil {
		return livediagnostics.VerifiedCatalog{}, err
	}
	trust, err := b.diagnosticConfigMap(ctx, livediagnostics.TrustConfigMap)
	if err != nil {
		return livediagnostics.VerifiedCatalog{}, errors.New("diagnostic trust configuration is unavailable")
	}
	keys := map[string]string{}
	if err := livediagnostics.DecodeStrict([]byte(trust["keys.json"]), &keys); err != nil {
		return livediagnostics.VerifiedCatalog{}, errors.New("diagnostic trust configuration is invalid")
	}
	return livediagnostics.LoadPublishedCatalog([]byte(catalog["catalog.json"]), []byte(catalog["previous-catalog.json"]), keys, b.catalogCache)
}
func (b *kubeDiagnosticSessionBackend) diagnosticConfigMap(ctx context.Context, name string) (map[string]string, error) {
	path := "/api/v1/namespaces/" + url.PathEscape(b.controlNamespace) + "/configmaps/" + url.PathEscape(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.cluster.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+b.cluster.bearerToken)
	resp, err := b.cluster.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, errDiagnosticCatalogAbsent
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("read diagnostic configuration: status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 1<<20 {
		return nil, errors.New("diagnostic configuration exceeds 1 MiB")
	}
	var value struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return value.Data, nil
}
func (s *Server) resolveRegisteredDiagnosticTarget(ctx context.Context, b diagnosticSessionBackend, c livediagnostics.VerifiedCatalog, req platformDiagnosticTargetRequest) (livediagnostics.Target, error) {
	req.Type = livediagnostics.TargetType(strings.TrimSpace(string(req.Type)))
	if req.Type == livediagnostics.TargetNode {
		node, err := b.GetNode(ctx, strings.TrimSpace(req.Node))
		if err != nil {
			return livediagnostics.Target{}, err
		}
		if !diagnosticNodeReady(node) {
			return livediagnostics.Target{}, errors.New("node is not Ready for a bounded diagnostic Job")
		}
		return livediagnostics.Target{Type: req.Type, Node: node.Name}, nil
	}
	if req.Type == livediagnostics.TargetNodeProcess {
		return resolveNodeProcessDiagnosticTarget(ctx, b, req)
	}
	if req.Type != livediagnostics.TargetPlatformComponent {
		return livediagnostics.Target{}, errors.New("unsupported registered diagnostic target")
	}
	ns := strings.TrimSpace(req.Namespace)
	if ns == "" {
		ns = b.SessionNamespace()
	}
	if !c.AllowsNamespace(ns) {
		return livediagnostics.Target{}, errors.New("target namespace is not authorized by diagnostic catalog")
	}
	if strings.TrimSpace(req.Pod) == "" {
		if ns != s.controlPlaneNamespace && ns != "kube-system" {
			return livediagnostics.Target{}, errors.New("registered workload diagnostics require an exact Pod")
		}
		return s.resolvePlatformDiagnosticTarget(ctx, b, req)
	}
	var pods []kubePodInfo
	var err error
	if reader, ok := b.(interface {
		GetDiagnosticPod(context.Context, string, string) (kubePodInfo, error)
	}); ok {
		var pod kubePodInfo
		pod, err = reader.GetDiagnosticPod(ctx, ns, strings.TrimSpace(req.Pod))
		if err == nil {
			pods = []kubePodInfo{pod}
		}
	} else {
		pods, err = b.ListPods(ctx, ns, "")
	}
	if err != nil {
		return livediagnostics.Target{}, err
	}
	for _, pod := range pods {
		if pod.Metadata.Name != strings.TrimSpace(req.Pod) || pod.Metadata.UID == "" || pod.Spec.NodeName == "" || pod.Status.Phase != "Running" {
			continue
		}
		container := strings.TrimSpace(req.Container)
		if container == "" && len(pod.Spec.Containers) == 1 {
			container = pod.Spec.Containers[0].Name
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != container || status.ContainerID == "" || status.State.Running == nil {
				continue
			}
			return livediagnostics.Target{Type: req.Type, Component: pod.Metadata.Labels["app.kubernetes.io/component"], Namespace: ns, Pod: pod.Metadata.Name, PodUID: pod.Metadata.UID, Container: container, ContainerID: status.ContainerID, Node: pod.Spec.NodeName, ImageDigest: firstNonEmptyString(status.ImageID, status.Image)}, nil
		}
	}
	return livediagnostics.Target{}, errors.New("no matching running container found for registered diagnostic target")
}

// Reuse the verified cluster TLS transport while attaching credentials only to
// requests made by this Kubernetes client.
type diagnosticAuthTransport struct {
	base  http.RoundTripper
	token string
}

func (t diagnosticAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(clone)
}
func (b *kubeDiagnosticSessionBackend) diagnosticKubernetesClient() (kubernetes.Interface, error) {
	client := *b.cluster.client
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = diagnosticAuthTransport{base: base, token: b.cluster.bearerToken}
	return kubernetes.NewForConfigAndClient(&rest.Config{Host: b.cluster.baseURL, UserAgent: "fugue-live-diagnostics", QPS: 5, Burst: 10}, &client)
}

func (b *kubeDiagnosticSessionBackend) GetDiagnosticPod(ctx context.Context, namespace, name string) (kubePodInfo, error) {
	var pod kubePodInfo
	err := b.cluster.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+url.PathEscape(namespace)+"/pods/"+url.PathEscape(name), &pod)
	return pod, err
}

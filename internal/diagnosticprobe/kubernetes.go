package diagnosticprobe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
	"fugue/internal/observability"
)

var resourcePart = regexp.MustCompile(`^[a-z][a-z0-9.-]*$`)

type kubeReader struct {
	client       *http.Client
	publicClient *http.Client
	token        string
	base         string
	lastRequest  time.Time
	requests     int
}

func (k *kubeReader) init() error {
	if k.client != nil {
		return nil
	}
	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return errors.New("cluster read capability is not available")
	}
	ca, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return errors.New("cluster trust bundle is unavailable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return errors.New("invalid cluster trust bundle")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext, TLSHandshakeTimeout: 3 * time.Second, MaxIdleConns: 4, MaxIdleConnsPerHost: 2, IdleConnTimeout: 15 * time.Second}
	k.client = &http.Client{Transport: transport, Timeout: 6 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	k.publicClient = &http.Client{Transport: transport.Clone(), Timeout: 6 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	k.token = strings.TrimSpace(string(token))
	k.base = "https://kubernetes.default.svc"
	return nil
}
func (k *kubeReader) get(ctx context.Context, endpoint string, kubernetes bool) ([]byte, error) {
	if err := k.init(); err != nil {
		return nil, err
	}
	if k.requests >= 240 {
		return nil, errors.New("collector HTTP request budget exhausted")
	}
	if wait := time.Until(k.lastRequest.Add(250 * time.Millisecond)); wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	k.requests++
	k.lastRequest = time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, errors.New("invalid collector endpoint")
	}
	client := k.publicClient
	if kubernetes {
		req.Header.Set("Authorization", "Bearer "+k.token)
		client = k.client
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("read failed: %s", safeText(err.Error()))
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("read returned HTTP %d", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 2<<20 {
		return nil, errors.New("source response exceeds 2 MiB")
	}
	return raw, nil
}
func (k *kubeReader) objects(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	if err := k.init(); err != nil {
		return nil, err
	}
	resource := parameter(c.Resource, req)
	version := c.Version
	if version == "" {
		version = "v1"
	}
	if !resourcePart.MatchString(resource) || !resourcePart.MatchString(version) || (c.Group != "" && !resourcePart.MatchString(c.Group)) {
		return nil, errors.New("invalid Kubernetes resource reference")
	}
	// The package deliberately has no credential, exec, proxy or mutation reader.
	if resource == "secrets" || resource == "serviceaccounts" || strings.Contains(resource, "/") {
		return nil, errors.New("resource is not available to the observation pack")
	}
	endpoint := "/api/" + version
	if c.Group != "" {
		endpoint = "/apis/" + c.Group + "/" + version
	}
	namespace := parameter(c.Namespace, req)
	if namespace != "" && namespace != "*" {
		endpoint += "/namespaces/" + url.PathEscape(namespace)
	}
	endpoint += "/" + resource
	name := parameter(c.ObjectName, req)
	if name != "" {
		endpoint += "/" + url.PathEscape(name)
	}
	query := url.Values{}
	if selector := parameter(c.Selector, req); selector != "" {
		query.Set("labelSelector", selector)
	}
	if name == "" {
		query.Set("limit", "100")
	}
	items := []any{}
	var rv string
	for page := 0; page < 4; page++ {
		raw, err := k.get(ctx, k.base+endpoint+"?"+query.Encode(), true)
		if err != nil {
			return nil, err
		}
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, errors.New("invalid Kubernetes JSON")
		}
		if name != "" {
			return projectObject(doc, c.Fields), nil
		}
		list, _ := doc["items"].([]any)
		for _, item := range list {
			if object, ok := item.(map[string]any); ok {
				items = append(items, projectObject(object, c.Fields))
			}
		}
		metadata, _ := doc["metadata"].(map[string]any)
		rv, _ = metadata["resourceVersion"].(string)
		cursor, _ := metadata["continue"].(string)
		if cursor == "" {
			return map[string]any{"resource_version": rv, "items": items, "truncated": false}, nil
		}
		query.Set("continue", cursor)
	}
	return partialValue{Value: map[string]any{"resource_version": rv, "items": items, "truncated": true}, Gaps: []string{"Kubernetes pagination budget exhausted"}, Truncated: true}, nil
}
func projectObject(object map[string]any, fields []string) map[string]any {
	out := map[string]any{"apiVersion": object["apiVersion"], "kind": object["kind"]}
	metadata, _ := object["metadata"].(map[string]any)
	meta := map[string]any{}
	for _, key := range []string{"name", "namespace", "uid", "resourceVersion", "generation", "creationTimestamp", "deletionTimestamp", "labels", "ownerReferences"} {
		if value, ok := metadata[key]; ok {
			meta[key] = value
		}
	}
	out["metadata"] = meta
	if len(fields) == 0 {
		fields = []string{"status"}
	}
	for _, field := range fields {
		// Generic observation is limited to explicitly named status/spec paths.
		// Full Pod/Deployment templates and environment variables never leave here.
		if strings.Contains(strings.ToLower(field), "secret") || strings.Contains(strings.ToLower(field), "password") || strings.Contains(field, "env") || field == "spec" || strings.Contains(field, "template") || strings.HasPrefix(field, "spec.containers") || strings.HasPrefix(field, "spec.initContainers") || strings.HasPrefix(field, "spec.ephemeralContainers") {
			continue
		}
		if value, ok := jsonField(object, strings.Split(field, ".")); ok {
			out[field] = redactJSON(value)
		}
	}
	return out
}
func jsonField(object map[string]any, parts []string) (any, bool) {
	var current any = object
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}
func redactJSON(value any) any {
	switch v := value.(type) {
	case string:
		return safeText(v)
	case []any:
		out := make([]any, len(v))
		for i, x := range v {
			out[i] = redactJSON(x)
		}
		return out
	case map[string]any:
		out := map[string]any{}
		for key, x := range v {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "password") || strings.Contains(lower, "token") || strings.Contains(lower, "secret") || lower == "authorization" {
				out[key] = "[REDACTED]"
			} else {
				out[key] = redactJSON(x)
			}
		}
		return out
	default:
		return value
	}
}
func (k *kubeReader) logs(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	if err := k.init(); err != nil {
		return nil, err
	}
	namespace := parameter(c.Namespace, req)
	pod := parameter(c.ObjectName, req)
	if namespace == "" || pod == "" {
		return nil, errors.New("log observation requires exact namespace and Pod")
	}
	since := c.SinceSeconds
	if since == 0 {
		since = req.DurationSeconds + 60
	}
	if since < 1 || since > 900 {
		return nil, errors.New("log lookback exceeds 900-second bound")
	}
	query := url.Values{"sinceSeconds": {strconv.Itoa(since)}, "timestamps": {"true"}, "limitBytes": {strconv.Itoa(512 << 10)}}
	if container := parameter(c.Container, req); container != "" {
		query.Set("container", container)
	}
	raw, err := k.get(ctx, k.base+"/api/v1/namespaces/"+url.PathEscape(namespace)+"/pods/"+url.PathEscape(pod)+"/log?"+query.Encode(), true)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	examples := []string{}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	for _, line := range lines {
		matched := len(c.Match) == 0
		for _, pattern := range c.Match {
			if strings.Contains(line, pattern) {
				counts[pattern]++
				matched = true
			}
		}
		if matched && len(examples) < 30 {
			safe := safeText(line)
			if len(safe) > 1600 {
				safe = safe[:1600]
			}
			examples = append(examples, safe)
		}
	}
	result := map[string]any{"pod": pod, "namespace": namespace, "lookback_seconds": since, "lines_returned": len(lines), "pattern_counts": counts, "examples": examples, "byte_limit": 512 << 10, "note": "bounded recent source log sample; overlapping windows must not be summed"}
	if len(raw) >= 512<<10 {
		return partialValue{Value: result, Gaps: []string{"log byte limit reached; source coverage is partial"}, Truncated: true}, nil
	}
	return result, nil
}
func (k *kubeReader) prometheus(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	if err := k.init(); err != nil {
		return nil, err
	}
	if c.Service == nil || c.Service.Name == "" || c.Service.Namespace == "" {
		return nil, errors.New("metrics reader requires an explicit Kubernetes service reference")
	}
	raw, err := k.get(ctx, k.base+"/api/v1/namespaces/"+url.PathEscape(c.Service.Namespace)+"/services/"+url.PathEscape(c.Service.Name), true)
	if err != nil {
		return nil, err
	}
	var service struct {
		Spec struct {
			ClusterIP string `json:"clusterIP"`
			Ports     []struct {
				Name string `json:"name"`
				Port int    `json:"port"`
			} `json:"ports"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(raw, &service); err != nil {
		return nil, err
	}
	port := 0
	for _, p := range service.Spec.Ports {
		if p.Name == c.Service.Port {
			port = p.Port
		}
	}
	ip := net.ParseIP(service.Spec.ClusterIP)
	if ip == nil || port < 1 || port > 65535 {
		return nil, errors.New("metrics service has no matching cluster endpoint")
	}
	endpoint := "http://" + net.JoinHostPort(ip.String(), strconv.Itoa(port)) + "/api/v1/query"
	names := make([]string, 0, len(c.Queries))
	for name := range c.Queries {
		names = append(names, name)
	}
	sort.Strings(names)
	results := map[string]any{}
	gaps := []string{}
	for _, name := range names {
		q := c.Queries[name]
		for key, value := range map[string]string{"node": req.Target.Node, "namespace": req.Target.Namespace, "pod": req.Target.Pod} {
			quoted, _ := json.Marshal(value)
			q = strings.ReplaceAll(q, "{{"+key+"}}", string(quoted))
		}
		for key, value := range req.Parameters {
			quoted, _ := json.Marshal(value)
			q = strings.ReplaceAll(q, "{{param."+key+"}}", string(quoted))
		}
		body, err := k.get(ctx, endpoint+"?"+url.Values{"query": {q}, "timeout": {"3s"}}.Encode(), false)
		if err != nil {
			gaps = append(gaps, name+": "+boundedError(err))
			continue
		}
		var response map[string]any
		if err := json.Unmarshal(body, &response); err != nil {
			gaps = append(gaps, name+": invalid metrics JSON")
			continue
		}
		if response["status"] != "success" {
			gaps = append(gaps, name+": metrics query failed")
			continue
		}
		results[name] = response["data"]
	}
	if len(gaps) > 0 {
		return partialValue{Value: results, Gaps: gaps}, nil
	}
	return results, nil
}

func safeText(value string) string { result, _ := observability.RedactText(value); return result }

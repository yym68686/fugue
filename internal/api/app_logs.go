package api

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const serviceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

type kubeJobList struct {
	Items []struct {
		Metadata struct {
			Name              string    `json:"name"`
			CreationTimestamp time.Time `json:"creationTimestamp"`
		} `json:"metadata"`
	} `json:"items"`
}

type kubePodList struct {
	Items []kubePodInfo `json:"items"`
}

type kubePodInfo struct {
	Metadata struct {
		Name              string    `json:"name"`
		CreationTimestamp time.Time `json:"creationTimestamp"`
	} `json:"metadata"`
	Spec struct {
		InitContainers []struct {
			Name string `json:"name"`
		} `json:"initContainers"`
		Containers []struct {
			Name string `json:"name"`
		} `json:"containers"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

type kubeLogsClient struct {
	client      *http.Client
	baseURL     string
	bearerToken string
	namespace   string
}

type kubeLogOptions struct {
	Container string
	TailLines int
	Previous  bool
}

type runtimeLogSection struct {
	Pod       string
	Container string
	Logs      string
}

type kubeStatusError struct {
	StatusCode int
	Message    string
}

func (e *kubeStatusError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func newKubeLogsClient(namespace string) (*kubeLogsClient, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		return nil, fmt.Errorf("kubernetes service host/port is not available in the environment")
	}

	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	caData, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read service account CA: %w", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caData) {
		return nil, fmt.Errorf("load service account CA")
	}

	if strings.TrimSpace(namespace) == "" {
		namespace, err = kubeNamespace()
		if err != nil {
			return nil, err
		}
	}

	return &kubeLogsClient{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: rootCAs},
			},
			Timeout: 20 * time.Second,
		},
		baseURL:     "https://" + host + ":" + port,
		bearerToken: strings.TrimSpace(string(token)),
		namespace:   strings.TrimSpace(namespace),
	}, nil
}

func (c *kubeLogsClient) effectiveNamespace(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace != "" {
		return namespace
	}
	return c.namespace
}

func (c *kubeLogsClient) listJobsBySelector(ctx context.Context, namespace, selector string) ([]struct {
	Metadata struct {
		Name              string    `json:"name"`
		CreationTimestamp time.Time `json:"creationTimestamp"`
	} `json:"metadata"`
}, error) {
	query := url.Values{}
	if strings.TrimSpace(selector) != "" {
		query.Set("labelSelector", selector)
	}

	var jobs kubeJobList
	apiPath := "/apis/batch/v1/namespaces/" + c.effectiveNamespace(namespace) + "/jobs"
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &jobs); err != nil {
		return nil, err
	}
	return jobs.Items, nil
}

func (c *kubeLogsClient) listPodsBySelector(ctx context.Context, namespace, selector string) ([]kubePodInfo, error) {
	query := url.Values{}
	if strings.TrimSpace(selector) != "" {
		query.Set("labelSelector", selector)
	}

	var pods kubePodList
	apiPath := "/api/v1/namespaces/" + c.effectiveNamespace(namespace) + "/pods"
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &pods); err != nil {
		return nil, err
	}
	return pods.Items, nil
}

func (c *kubeLogsClient) readPodLogs(ctx context.Context, namespace, podName string, opts kubeLogOptions) (string, error) {
	query := url.Values{}
	if strings.TrimSpace(opts.Container) != "" {
		query.Set("container", strings.TrimSpace(opts.Container))
	}
	if opts.TailLines > 0 {
		query.Set("tailLines", strconv.Itoa(opts.TailLines))
	}
	if opts.Previous {
		query.Set("previous", "true")
	}

	apiPath := "/api/v1/namespaces/" + c.effectiveNamespace(namespace) + "/pods/" + url.PathEscape(strings.TrimSpace(podName)) + "/log"
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	return c.doText(ctx, http.MethodGet, apiPath)
}

func (c *kubeLogsClient) doJSON(ctx context.Context, method, apiPath string, out any) error {
	body, err := c.doRequest(ctx, method, apiPath)
	if err != nil {
		return err
	}
	if out != nil && len(body) > 0 {
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("decode kubernetes response: %w", err)
		}
	}
	return nil
}

func (c *kubeLogsClient) doText(ctx context.Context, method, apiPath string) (string, error) {
	body, err := c.doRequest(ctx, method, apiPath)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (c *kubeLogsClient) doRequest(ctx context.Context, method, apiPath string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+apiPath, nil)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kubernetes request %s %s: %w", method, apiPath, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, &kubeStatusError{
			StatusCode: resp.StatusCode,
			Message:    fmt.Sprintf("kubernetes request %s %s failed: status=%d body=%s", method, apiPath, resp.StatusCode, strings.TrimSpace(string(body))),
		}
	}
	return body, nil
}

func isKubeNotFound(err error) bool {
	var statusErr *kubeStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound
}

func podContainerNames(pod kubePodInfo, includeInit bool) []string {
	names := make([]string, 0, len(pod.Spec.Containers)+len(pod.Spec.InitContainers))
	if includeInit {
		for _, container := range pod.Spec.InitContainers {
			if name := strings.TrimSpace(container.Name); name != "" {
				names = append(names, name)
			}
		}
	}
	for _, container := range pod.Spec.Containers {
		if name := strings.TrimSpace(container.Name); name != "" {
			names = append(names, name)
		}
	}
	return names
}

func readJobLogs(ctx context.Context, client *kubeLogsClient, namespace, jobName string, tailLines int) (string, error) {
	pods, err := client.listPodsBySelector(ctx, namespace, "job-name="+jobName)
	if err != nil {
		return "", err
	}
	if len(pods) == 0 {
		return "", nil
	}
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].Metadata.CreationTimestamp.Before(pods[j].Metadata.CreationTimestamp)
	})

	sections := make([]string, 0, len(pods))
	for _, pod := range pods {
		containerSections := make([]string, 0)
		for _, containerName := range podContainerNames(pod, true) {
			logs, err := client.readPodLogs(ctx, namespace, pod.Metadata.Name, kubeLogOptions{
				Container: containerName,
				TailLines: tailLines,
			})
			if err != nil {
				if isKubeNotFound(err) {
					continue
				}
				return "", err
			}
			logs = strings.TrimSpace(logs)
			if logs == "" {
				continue
			}
			containerSections = append(containerSections, fmt.Sprintf("==> %s/%s <==\n%s", pod.Metadata.Name, containerName, logs))
		}
		if len(containerSections) > 0 {
			sections = append(sections, strings.Join(containerSections, "\n\n"))
		}
	}
	return strings.TrimSpace(strings.Join(sections, "\n\n")), nil
}

func collectRuntimeLogs(ctx context.Context, client *kubeLogsClient, namespace string, pods []kubePodInfo, containerName string, tailLines int, previous bool) (string, []string) {
	sections := make([]string, 0, len(pods))
	warnings := make([]string, 0)
	for _, pod := range pods {
		out, err := client.readPodLogs(ctx, namespace, pod.Metadata.Name, kubeLogOptions{
			Container: containerName,
			TailLines: tailLines,
			Previous:  previous,
		})
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", pod.Metadata.Name, err))
			continue
		}
		sections = append(sections, fmt.Sprintf("==> %s <==\n%s", pod.Metadata.Name, strings.TrimSpace(out)))
	}
	return strings.TrimSpace(strings.Join(sections, "\n\n")), warnings
}

func filterPodsByName(pods []kubePodInfo, requestedPod string) []kubePodInfo {
	if strings.TrimSpace(requestedPod) == "" {
		return pods
	}
	filtered := pods[:0]
	for _, pod := range pods {
		if pod.Metadata.Name == requestedPod {
			filtered = append(filtered, pod)
		}
	}
	return filtered
}

func podNames(pods []kubePodInfo) []string {
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		names = append(names, pod.Metadata.Name)
	}
	sort.Strings(names)
	return names
}

func sortPodsByCreation(pods []kubePodInfo) {
	sort.Slice(pods, func(i, j int) bool {
		if !pods[i].Metadata.CreationTimestamp.Equal(pods[j].Metadata.CreationTimestamp) {
			return pods[i].Metadata.CreationTimestamp.Before(pods[j].Metadata.CreationTimestamp)
		}
		return pods[i].Metadata.Name < pods[j].Metadata.Name
	})
}

func buildLogPodNames(pods []kubePodInfo) []string {
	return podNames(pods)
}

func runtimeLogPodNames(pods []kubePodInfo) []string {
	return podNames(pods)
}

func latestBuilderJobName(ctx context.Context, client *kubeLogsClient, namespace, operationID string) (string, error) {
	jobs, err := client.listJobsBySelector(ctx, namespace, "fugue.pro/operation-id="+operationID)
	if err != nil {
		return "", err
	}
	if len(jobs) == 0 {
		return "", nil
	}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].Metadata.CreationTimestamp.Before(jobs[j].Metadata.CreationTimestamp)
	})
	return jobs[len(jobs)-1].Metadata.Name, nil
}

func readBuildLogs(ctx context.Context, op model.Operation, tailLines int) (logs, source, jobName string, available bool, err error) {
	namespace, err := kubeNamespace()
	if err != nil {
		return "", "", "", false, err
	}
	client, err := newKubeLogsClient(namespace)
	if err != nil {
		return "", "", "", false, err
	}

	jobName, err = latestBuilderJobName(ctx, client, namespace, op.ID)
	if err == nil && jobName != "" {
		logs, err = readJobLogs(ctx, client, namespace, jobName, tailLines)
		if err == nil && strings.TrimSpace(logs) != "" {
			return logs, "kubernetes.job", jobName, true, nil
		}
	}

	if strings.TrimSpace(op.ErrorMessage) != "" {
		return op.ErrorMessage, "operation.error_message", jobName, true, nil
	}
	if strings.TrimSpace(op.ResultMessage) != "" {
		return op.ResultMessage, "operation.result_message", jobName, true, nil
	}
	return "", "unavailable", jobName, false, nil
}

func kubeNamespace() (string, error) {
	if value := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); value != "" {
		return value, nil
	}
	data, err := os.ReadFile(serviceAccountNamespacePath)
	if err != nil {
		return "", fmt.Errorf("read service account namespace: %w", err)
	}
	namespace := strings.TrimSpace(string(data))
	if namespace == "" {
		return "", fmt.Errorf("service account namespace is empty")
	}
	return namespace, nil
}

func parseTailLines(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 200, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("tail_lines must be an integer")
	}
	if value < 1 || value > 5000 {
		return 0, fmt.Errorf("tail_lines must be between 1 and 5000")
	}
	return value, nil
}

func parseBoolQuery(raw string) (bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid boolean value %q", raw)
	}
	return value, nil
}

func runtimeLogTarget(app model.App, component string) (string, string, error) {
	appName := runtimeResourceName(app.Name)
	switch component {
	case "app":
		return "app.kubernetes.io/name=" + appName + ",app.kubernetes.io/managed-by=fugue", appName, nil
	case "postgres":
		if app.Spec.Postgres == nil {
			return "", "", fmt.Errorf("app does not declare postgres")
		}
		return "app.kubernetes.io/name=" + appName + "-postgres,app.kubernetes.io/component=postgres,app.kubernetes.io/managed-by=fugue", "postgres", nil
	default:
		return "", "", fmt.Errorf("unsupported component %q", component)
	}
}

func runtimeResourceName(name string) string {
	name = model.Slugify(name)
	if len(name) > 50 {
		return name[:50]
	}
	return name
}

func buildStrategyFromOperation(op model.Operation) string {
	if op.DesiredSource == nil {
		return ""
	}
	return strings.TrimSpace(op.DesiredSource.BuildStrategy)
}

func (s *Server) handleGetAppBuildLogs(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	op, err := s.resolveBuildOperation(app, strings.TrimSpace(r.URL.Query().Get("operation_id")))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	tailLines, err := parseTailLines(r.URL.Query().Get("tail_lines"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	logs, source, jobName, available, err := readBuildLogs(r.Context(), op, tailLines)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.appendAudit(principal, "app.build_logs.read", "app", app.ID, app.TenantID, map[string]string{"operation_id": op.ID})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"operation_id":     op.ID,
		"operation_status": op.Status,
		"job_name":         jobName,
		"available":        available,
		"source":           source,
		"logs":             logs,
		"build_strategy":   buildStrategyFromOperation(op),
		"error_message":    op.ErrorMessage,
		"result_message":   op.ResultMessage,
		"last_updated_at":  op.UpdatedAt,
		"completed_at":     op.CompletedAt,
		"started_at":       op.StartedAt,
	})
}

func (s *Server) handleGetAppRuntimeLogs(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	runtimeObj, err := s.store.GetRuntime(app.Spec.RuntimeID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if runtimeObj.Type == model.RuntimeTypeExternalOwned {
		httpx.WriteError(w, http.StatusBadRequest, "runtime logs are only available for managed runtimes")
		return
	}

	component := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("component")))
	if component == "" {
		component = "app"
	}
	if component != "app" && component != "postgres" {
		httpx.WriteError(w, http.StatusBadRequest, "component must be app or postgres")
		return
	}

	tailLines, err := parseTailLines(r.URL.Query().Get("tail_lines"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	previous, err := parseBoolQuery(r.URL.Query().Get("previous"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	requestedPod := strings.TrimSpace(r.URL.Query().Get("pod"))

	namespace := runtime.NamespaceForTenant(app.TenantID)
	client, err := newKubeLogsClient(namespace)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	selector, containerName, err := runtimeLogTarget(app, component)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	pods, err := client.listPodsBySelector(r.Context(), namespace, selector)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	sortPodsByCreation(pods)
	pods = filterPodsByName(pods, requestedPod)
	if len(pods) == 0 {
		httpx.WriteError(w, http.StatusNotFound, "no matching pods found")
		return
	}

	logs, warnings := collectRuntimeLogs(r.Context(), client, namespace, pods, containerName, tailLines, previous)
	s.appendAudit(principal, "app.runtime_logs.read", "app", app.ID, app.TenantID, map[string]string{"component": component})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"component": component,
		"namespace": namespace,
		"selector":  selector,
		"container": containerName,
		"pods":      runtimeLogPodNames(pods),
		"logs":      logs,
		"warnings":  warnings,
	})
}

func (s *Server) resolveBuildOperation(app model.App, requestedOperationID string) (model.Operation, error) {
	if requestedOperationID != "" {
		op, err := s.store.GetOperation(requestedOperationID)
		if err != nil {
			return model.Operation{}, err
		}
		if op.AppID != app.ID || op.TenantID != app.TenantID || op.Type != model.OperationTypeImport {
			return model.Operation{}, store.ErrNotFound
		}
		return op, nil
	}

	ops, err := s.store.ListOperations(app.TenantID, false)
	if err != nil {
		return model.Operation{}, err
	}
	var latest model.Operation
	found := false
	for _, op := range ops {
		if op.AppID != app.ID || op.Type != model.OperationTypeImport {
			continue
		}
		if !found || op.CreatedAt.After(latest.CreatedAt) {
			latest = op
			found = true
		}
	}
	if !found {
		return model.Operation{}, store.ErrNotFound
	}
	return latest, nil
}

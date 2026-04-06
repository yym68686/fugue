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
	Items []kubeJobInfo `json:"items"`
}

type kubeJobInfo struct {
	Metadata struct {
		Name              string    `json:"name"`
		CreationTimestamp time.Time `json:"creationTimestamp"`
	} `json:"metadata"`
	Status kubeJobStatus `json:"status"`
}

type kubeJobStatus struct {
	Active     int                `json:"active,omitempty"`
	Succeeded  int                `json:"succeeded,omitempty"`
	Failed     int                `json:"failed,omitempty"`
	Conditions []kubeJobCondition `json:"conditions,omitempty"`
}

type kubeJobCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
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
		NodeName       string `json:"nodeName,omitempty"`
		InitContainers []struct {
			Name string `json:"name"`
		} `json:"initContainers"`
		Containers []struct {
			Name string `json:"name"`
		} `json:"containers"`
	} `json:"spec"`
	Status struct {
		Phase                 string                `json:"phase"`
		Reason                string                `json:"reason,omitempty"`
		Message               string                `json:"message,omitempty"`
		InitContainerStatuses []kubeContainerStatus `json:"initContainerStatuses,omitempty"`
		ContainerStatuses     []kubeContainerStatus `json:"containerStatuses,omitempty"`
	} `json:"status"`
}

type kubeContainerStatus struct {
	Name      string           `json:"name"`
	State     kubeRuntimeState `json:"state,omitempty"`
	LastState kubeRuntimeState `json:"lastState,omitempty"`
}

type kubeRuntimeState struct {
	Waiting    *kubeStateDetail `json:"waiting,omitempty"`
	Terminated *kubeStateDetail `json:"terminated,omitempty"`
}

type kubeStateDetail struct {
	Reason   string `json:"reason,omitempty"`
	Message  string `json:"message,omitempty"`
	ExitCode int    `json:"exitCode,omitempty"`
}

type kubeLogsClient struct {
	client       *http.Client
	streamClient *http.Client
	baseURL      string
	bearerToken  string
	namespace    string
}

type appLogsClient interface {
	listJobsBySelector(ctx context.Context, namespace, selector string) ([]kubeJobInfo, error)
	getJob(ctx context.Context, namespace, jobName string) (kubeJobInfo, error)
	listPodsBySelector(ctx context.Context, namespace, selector string) ([]kubePodInfo, error)
	readPodLogs(ctx context.Context, namespace, podName string, opts kubeLogOptions) (string, error)
	streamPodLogs(ctx context.Context, namespace, podName string, opts kubeLogOptions) (io.ReadCloser, error)
}

type kubeLogOptions struct {
	Container  string
	TailLines  int
	Previous   bool
	Follow     bool
	SinceTime  *time.Time
	Timestamps bool
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
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: rootCAs},
	}

	return &kubeLogsClient{
		client: &http.Client{
			Transport: transport,
			Timeout:   20 * time.Second,
		},
		streamClient: &http.Client{
			Transport: transport,
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

func (c *kubeLogsClient) listJobsBySelector(ctx context.Context, namespace, selector string) ([]kubeJobInfo, error) {
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

func (c *kubeLogsClient) getJob(ctx context.Context, namespace, jobName string) (kubeJobInfo, error) {
	var job kubeJobInfo
	apiPath := "/apis/batch/v1/namespaces/" + c.effectiveNamespace(namespace) + "/jobs/" + url.PathEscape(strings.TrimSpace(jobName))
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &job); err != nil {
		return kubeJobInfo{}, err
	}
	return job, nil
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
	apiPath := c.podLogsAPIPath(namespace, podName, opts)
	return c.doText(ctx, http.MethodGet, apiPath)
}

func (c *kubeLogsClient) streamPodLogs(ctx context.Context, namespace, podName string, opts kubeLogOptions) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+c.podLogsAPIPath(namespace, podName, opts), nil)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes log stream request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "*/*")

	resp, err := c.streamClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kubernetes log stream %s: %w", req.URL.Path, err)
	}
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, &kubeStatusError{
			StatusCode: resp.StatusCode,
			Message:    fmt.Sprintf("kubernetes log stream %s failed: status=%d body=%s", req.URL.Path, resp.StatusCode, strings.TrimSpace(string(body))),
		}
	}
	return resp.Body, nil
}

func (c *kubeLogsClient) podLogsAPIPath(namespace, podName string, opts kubeLogOptions) string {
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
	if opts.Follow {
		query.Set("follow", "true")
	}
	if opts.Timestamps {
		query.Set("timestamps", "true")
	}
	if opts.SinceTime != nil && !opts.SinceTime.IsZero() {
		query.Set("sinceTime", opts.SinceTime.UTC().Format(time.RFC3339Nano))
	}

	apiPath := "/api/v1/namespaces/" + c.effectiveNamespace(namespace) + "/pods/" + url.PathEscape(strings.TrimSpace(podName)) + "/log"
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	return apiPath
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

func podStatusContainerNames(pod kubePodInfo, includeInit bool) []string {
	names := make([]string, 0, len(pod.Status.ContainerStatuses)+len(pod.Status.InitContainerStatuses))
	if includeInit {
		for _, status := range pod.Status.InitContainerStatuses {
			if name := strings.TrimSpace(status.Name); name != "" {
				names = append(names, name)
			}
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if name := strings.TrimSpace(status.Name); name != "" {
			names = append(names, name)
		}
	}
	return names
}

func readJobLogs(ctx context.Context, client appLogsClient, namespace, jobName string, tailLines int) (string, error) {
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

func collectRuntimeLogs(ctx context.Context, client appLogsClient, namespace string, pods []kubePodInfo, containerName string, tailLines int, previous bool) (string, []string) {
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

func latestBuilderJobName(ctx context.Context, client appLogsClient, namespace, operationID string) (string, error) {
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

func kubeJobCompleted(status kubeJobStatus) bool {
	for _, condition := range status.Conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), "Complete") && strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
			return true
		}
	}
	return status.Succeeded > 0
}

func kubeJobFailed(status kubeJobStatus) bool {
	for _, condition := range status.Conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), "Failed") && strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
			return true
		}
	}
	return status.Failed > 0 && status.Active == 0 && status.Succeeded == 0
}

func summarizeKubeFailureLine(subject, reason, message string) string {
	subject = strings.TrimSpace(subject)
	reason = strings.TrimSpace(reason)
	message = strings.TrimSpace(message)
	switch {
	case reason != "" && message != "":
		return fmt.Sprintf("%s failed: %s: %s", subject, reason, message)
	case reason != "":
		return fmt.Sprintf("%s failed: %s", subject, reason)
	case message != "":
		return fmt.Sprintf("%s failed: %s", subject, message)
	default:
		return fmt.Sprintf("%s failed", subject)
	}
}

func summarizeKubeContainerFailure(prefix, containerName, state string, detail kubeStateDetail) string {
	subject := prefix
	if strings.TrimSpace(containerName) != "" {
		subject += " container " + strings.TrimSpace(containerName)
	}
	reason := strings.TrimSpace(detail.Reason)
	message := strings.TrimSpace(detail.Message)
	if detail.ExitCode != 0 {
		if message == "" {
			message = fmt.Sprintf("exit_code=%d", detail.ExitCode)
		} else {
			message = fmt.Sprintf("%s (exit_code=%d)", message, detail.ExitCode)
		}
	}
	if reason == "" {
		reason = state
	}
	return summarizeKubeFailureLine(subject, reason, message)
}

func summarizeKubePodFailure(pod kubePodInfo) string {
	prefix := "pod " + strings.TrimSpace(pod.Metadata.Name)
	if node := strings.TrimSpace(pod.Spec.NodeName); node != "" {
		prefix += " on node " + node
	}
	if reason := strings.TrimSpace(pod.Status.Reason); reason != "" {
		return summarizeKubeFailureLine(prefix, reason, strings.TrimSpace(pod.Status.Message))
	}
	statuses := append([]kubeContainerStatus(nil), pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	for _, status := range statuses {
		if status.State.Terminated != nil {
			return summarizeKubeContainerFailure(prefix, status.Name, "terminated", *status.State.Terminated)
		}
		if status.LastState.Terminated != nil {
			return summarizeKubeContainerFailure(prefix, status.Name, "terminated", *status.LastState.Terminated)
		}
		if status.State.Waiting != nil {
			return summarizeKubeContainerFailure(prefix, status.Name, "waiting", *status.State.Waiting)
		}
		if status.LastState.Waiting != nil {
			return summarizeKubeContainerFailure(prefix, status.Name, "waiting", *status.LastState.Waiting)
		}
	}
	phase := strings.TrimSpace(pod.Status.Phase)
	if phase != "" && !strings.EqualFold(phase, "Running") && !strings.EqualFold(phase, "Succeeded") {
		return fmt.Sprintf("%s failed with phase %s", prefix, phase)
	}
	return ""
}

func summarizeBuilderJobFailure(ctx context.Context, client appLogsClient, namespace, jobName string) (string, error) {
	job, err := client.getJob(ctx, namespace, jobName)
	if err != nil {
		return "", err
	}

	pods, err := client.listPodsBySelector(ctx, namespace, "job-name="+jobName)
	if err != nil {
		return "", err
	}
	sortPodsByCreation(pods)

	lines := make([]string, 0, len(pods)+1)
	for _, pod := range pods {
		if summary := summarizeKubePodFailure(pod); summary != "" {
			lines = append(lines, summary)
		}
	}
	if len(lines) > 0 {
		return strings.Join(lines, "\n"), nil
	}
	if kubeJobFailed(job.Status) {
		for _, condition := range job.Status.Conditions {
			if strings.EqualFold(strings.TrimSpace(condition.Type), "Failed") && strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
				return summarizeKubeFailureLine("job "+jobName, condition.Reason, condition.Message), nil
			}
		}
	}
	return "", nil
}

func readBuildLogsWithClient(ctx context.Context, client appLogsClient, namespace string, op model.Operation, tailLines int) (logs, source, jobName string, available bool, err error) {
	jobName, err = latestBuilderJobName(ctx, client, namespace, op.ID)
	if err == nil && jobName != "" {
		logs, err = readJobLogs(ctx, client, namespace, jobName, tailLines)
		if err == nil && strings.TrimSpace(logs) != "" {
			return logs, "kubernetes.job", jobName, true, nil
		}
		summary, summaryErr := summarizeBuilderJobFailure(ctx, client, namespace, jobName)
		if summaryErr == nil && strings.TrimSpace(summary) != "" {
			return summary, "kubernetes.job_summary", jobName, true, nil
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

func readBuildLogs(ctx context.Context, op model.Operation, tailLines int) (logs, source, jobName string, available bool, err error) {
	client, err := newKubeLogsClient("")
	if err != nil {
		return "", "", "", false, err
	}
	return readBuildLogsWithClient(ctx, client, "", op, tailLines)
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
	appName := runtime.RuntimeResourceName(app.Name)
	switch component {
	case "app":
		selectors := []string{
			runtime.FugueLabelManagedBy + "=" + runtime.FugueLabelManagedByValue,
		}
		if appName != "" {
			selectors = append(selectors, runtime.FugueLabelName+"="+appName)
		}
		if appID := strings.TrimSpace(app.ID); appID != "" {
			selectors = append(selectors, runtime.FugueLabelAppID+"="+appID)
		}
		return strings.Join(selectors, ","), appName, nil
	case "postgres":
		bound, ok := firstManagedPostgresBinding(app)
		if !ok || bound.Service.Spec.Postgres == nil {
			return "", "", fmt.Errorf("app does not declare managed postgres")
		}
		serviceName := strings.TrimSpace(bound.Service.Spec.Postgres.ServiceName)
		if serviceName == "" {
			serviceName = runtime.RuntimeResourceName(bound.Service.Name)
			if serviceName == "" {
				serviceName = appName
			}
			serviceName += "-postgres"
		}
		return "cnpg.io/cluster=" + serviceName + ",app.kubernetes.io/managed-by=cloudnative-pg", "postgres", nil
	default:
		return "", "", fmt.Errorf("unsupported component %q", component)
	}
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

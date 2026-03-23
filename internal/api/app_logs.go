package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
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

type kubeObjectList struct {
	Items []struct {
		Metadata struct {
			Name              string    `json:"name"`
			CreationTimestamp time.Time `json:"creationTimestamp"`
		} `json:"metadata"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	} `json:"items"`
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
	selector, containerName, err := runtimeLogTarget(app, component)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	pods, err := kubectlListPodsBySelector(r.Context(), namespace, selector)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if requestedPod != "" {
		filtered := pods[:0]
		for _, pod := range pods {
			if pod == requestedPod {
				filtered = append(filtered, pod)
			}
		}
		pods = filtered
	}
	if len(pods) == 0 {
		httpx.WriteError(w, http.StatusNotFound, "no matching pods found")
		return
	}

	logs, warnings := collectRuntimeLogs(r.Context(), namespace, pods, containerName, tailLines, previous)
	s.appendAudit(principal, "app.runtime_logs.read", "app", app.ID, app.TenantID, map[string]string{"component": component})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"component": component,
		"namespace": namespace,
		"selector":  selector,
		"container": containerName,
		"pods":      pods,
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

func readBuildLogs(ctx context.Context, op model.Operation, tailLines int) (logs, source, jobName string, available bool, err error) {
	namespace, err := kubeNamespace()
	if err != nil {
		return "", "", "", false, err
	}

	jobName, err = latestBuilderJobName(ctx, namespace, op.ID)
	if err == nil && jobName != "" {
		logs, err = kubectlOutputWithTimeout(ctx, 20*time.Second, "-n", namespace, "logs", "job/"+jobName, "--all-containers=true", "--tail", strconv.Itoa(tailLines))
		if err == nil {
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

func latestBuilderJobName(ctx context.Context, namespace, operationID string) (string, error) {
	var jobs kubeObjectList
	if err := kubectlJSONWithTimeout(ctx, 15*time.Second, &jobs, "-n", namespace, "get", "jobs", "-l", "fugue.pro/operation-id="+operationID, "-o", "json"); err != nil {
		return "", err
	}
	if len(jobs.Items) == 0 {
		return "", nil
	}
	sort.Slice(jobs.Items, func(i, j int) bool {
		return jobs.Items[i].Metadata.CreationTimestamp.Before(jobs.Items[j].Metadata.CreationTimestamp)
	})
	return jobs.Items[len(jobs.Items)-1].Metadata.Name, nil
}

func kubectlListPodsBySelector(ctx context.Context, namespace, selector string) ([]string, error) {
	var podList kubeObjectList
	if err := kubectlJSONWithTimeout(ctx, 15*time.Second, &podList, "-n", namespace, "get", "pods", "-l", selector, "-o", "json"); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(podList.Items))
	for _, item := range podList.Items {
		names = append(names, item.Metadata.Name)
	}
	sort.Strings(names)
	return names, nil
}

func collectRuntimeLogs(ctx context.Context, namespace string, pods []string, containerName string, tailLines int, previous bool) (string, []string) {
	sections := make([]string, 0, len(pods))
	warnings := make([]string, 0)
	for _, podName := range pods {
		args := []string{"-n", namespace, "logs", "pod/" + podName, "-c", containerName, "--tail", strconv.Itoa(tailLines)}
		if previous {
			args = append(args, "--previous=true")
		}
		out, err := kubectlOutputWithTimeout(ctx, 20*time.Second, args...)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", podName, err))
			continue
		}
		sections = append(sections, fmt.Sprintf("==> %s <==\n%s", podName, out))
	}
	return strings.TrimSpace(strings.Join(sections, "\n\n")), warnings
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

func kubectlJSONWithTimeout(ctx context.Context, timeout time.Duration, dst any, args ...string) error {
	output, err := kubectlOutputWithTimeout(ctx, timeout, args...)
	if err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(output), dst); err != nil {
		return fmt.Errorf("decode kubectl json: %w", err)
	}
	return nil
}

func kubectlOutputWithTimeout(parent context.Context, timeout time.Duration, args ...string) (string, error) {
	ctx := parent
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return "", fmt.Errorf("kubectl is not available in fugue-api image")
		}
		return "", fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

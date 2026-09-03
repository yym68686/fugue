package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	diagnosticManagedByLabel                  = "fugue.pro/live-diagnostics"
	diagnosticManagedByValue                  = "true"
	diagnosticAppIDLabel                      = "fugue.pro/diagnostic-app-id"
	diagnosticSessionIDLabel                  = "fugue.pro/diagnostic-session-id"
	diagnosticKindLabel                       = "fugue.pro/diagnostic-kind"
	diagnosticTargetPodAnnotation             = "fugue.pro/diagnostic-target-pod"
	diagnosticTargetContainerAnnotation       = "fugue.pro/diagnostic-target-container"
	diagnosticTargetNodeAnnotation            = "fugue.pro/diagnostic-target-node"
	diagnosticTargetNamespaceAnnotation       = "fugue.pro/diagnostic-target-namespace"
	diagnosticDurationAnnotation              = "fugue.pro/diagnostic-duration-seconds"
	diagnosticFrequencyAnnotation             = "fugue.pro/diagnostic-frequency-hz"
	diagnosticDefaultDuration                 = 60
	diagnosticMinDuration                     = 5
	diagnosticMaxDuration                     = 120
	diagnosticDefaultFrequency                = 19
	diagnosticMaxFrequency                    = 99
	diagnosticRetentionSeconds          int32 = 3600
	diagnosticMaxActivePerApp                 = 1
	diagnosticMaxActiveGlobal                 = 4
	diagnosticMaxReportBytes                  = 8 << 20
)

type diagnosticSessionBackend interface {
	RunnerImage(context.Context) (string, error)
	SessionNamespace() string
	CreateJob(context.Context, string, batchv1.Job) (batchv1.Job, error)
	GetJob(context.Context, string, string) (batchv1.Job, error)
	ListJobs(context.Context, string, string) ([]batchv1.Job, error)
	DeleteJob(context.Context, string, string) error
	ListPods(context.Context, string, string) ([]kubePodInfo, error)
	ReadPodLogs(context.Context, string, string, string) (string, error)
}

type kubeDiagnosticSessionBackend struct {
	cluster          *clusterNodeClient
	logs             *kubeLogsClient
	controlNamespace string
}

type diagnosticSession struct {
	ID              string     `json:"id"`
	AppID           string     `json:"app_id"`
	Kind            string     `json:"kind"`
	Status          string     `json:"status"`
	TargetPod       string     `json:"target_pod"`
	TargetContainer string     `json:"target_container"`
	TargetNode      string     `json:"target_node"`
	DurationSeconds int        `json:"duration_seconds"`
	FrequencyHz     int        `json:"frequency_hz"`
	CreatedAt       time.Time  `json:"created_at"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	FailureReason   string     `json:"failure_reason,omitempty"`
}

type diagnosticStartRequest struct {
	Kind            string `json:"kind"`
	DurationSeconds int    `json:"duration_seconds"`
	FrequencyHz     int    `json:"frequency_hz"`
	Pod             string `json:"pod,omitempty"`
	Container       string `json:"container,omitempty"`
}

type diagnosticTarget struct {
	Namespace   string
	PodName     string
	Container   string
	ContainerID string
	NodeName    string
}

func (s *Server) handleStartAppDiagnosticSession(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform administrator access required")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	var req diagnosticStartRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := normalizeDiagnosticStartRequest(&req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	backend, err := s.newDiagnosticSessionBackend()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	active, err := backend.ListJobs(r.Context(), backend.SessionNamespace(), diagnosticManagedByLabel+"="+diagnosticManagedByValue)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "list active diagnostic sessions: "+err.Error())
		return
	}
	globalActive, appActive := countActiveDiagnosticJobs(active, app.ID)
	if globalActive >= diagnosticMaxActiveGlobal || appActive >= diagnosticMaxActivePerApp {
		httpx.WriteError(w, http.StatusConflict, "diagnostic session concurrency limit reached")
		return
	}
	target, err := s.resolveDiagnosticTarget(r.Context(), app, req)
	if err != nil {
		httpx.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	runnerImage, err := backend.RunnerImage(r.Context())
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "resolve diagnostic runner image: "+err.Error())
		return
	}
	sessionID := model.DNS1035Label(model.NewID("diagnostic"), "diagnostic")
	sessionNamespace := backend.SessionNamespace()
	created, err := backend.CreateJob(r.Context(), sessionNamespace, buildDiagnosticJob(app, sessionID, sessionNamespace, target, req, runnerImage))
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "create diagnostic session: "+err.Error())
		return
	}
	session := diagnosticSessionFromJob(created)
	s.appendAudit(principal, "app.diagnostics.start", "diagnostic_session", session.ID, app.TenantID, map[string]string{
		"app_id": app.ID, "kind": req.Kind, "duration_seconds": strconv.Itoa(req.DurationSeconds),
		"frequency_hz": strconv.Itoa(req.FrequencyHz), "target_pod": target.PodName,
	})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"session": session})
}

func (s *Server) handleListAppDiagnosticSessions(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !canReadAppDiagnostics(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.observability.read scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	backend, err := s.newDiagnosticSessionBackend()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	jobs, err := backend.ListJobs(r.Context(), backend.SessionNamespace(), diagnosticManagedByLabel+"="+diagnosticManagedByValue+","+diagnosticAppIDLabel+"="+app.ID)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	sessions := make([]diagnosticSession, 0, len(jobs))
	for _, job := range jobs {
		sessions = append(sessions, diagnosticSessionFromJob(job))
	}
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].CreatedAt.After(sessions[j].CreatedAt) })
	s.appendAudit(principal, "app.diagnostics.list", "app", app.ID, app.TenantID, nil)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"sessions": sessions})
}

func (s *Server) handleGetAppDiagnosticSession(w http.ResponseWriter, r *http.Request) {
	s.handleGetAppDiagnosticSessionValue(w, r, false)
}

func (s *Server) handleGetAppDiagnosticReport(w http.ResponseWriter, r *http.Request) {
	s.handleGetAppDiagnosticSessionValue(w, r, true)
}

func (s *Server) handleGetAppDiagnosticSessionValue(w http.ResponseWriter, r *http.Request, includeReport bool) {
	principal := mustPrincipal(r)
	if !canReadAppDiagnostics(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.observability.read scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	sessionID := strings.TrimSpace(r.PathValue("session_id"))
	if !validDiagnosticSessionID(sessionID) {
		httpx.WriteError(w, http.StatusBadRequest, "invalid diagnostic session id")
		return
	}
	backend, err := s.newDiagnosticSessionBackend()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	namespace := backend.SessionNamespace()
	job, err := backend.GetJob(r.Context(), namespace, sessionID)
	if err != nil {
		writeDiagnosticBackendError(w, err)
		return
	}
	if job.Labels[diagnosticAppIDLabel] != app.ID || job.Labels[diagnosticManagedByLabel] != diagnosticManagedByValue {
		httpx.WriteError(w, http.StatusNotFound, "diagnostic session not found")
		return
	}
	session := diagnosticSessionFromJob(job)
	response := map[string]any{"session": session}
	if includeReport {
		if session.Status == "queued" || session.Status == "running" {
			httpx.WriteError(w, http.StatusConflict, "diagnostic report is not ready")
			return
		}
		pods, listErr := backend.ListPods(r.Context(), namespace, "job-name="+sessionID)
		if listErr != nil || len(pods) == 0 {
			httpx.WriteError(w, http.StatusServiceUnavailable, "diagnostic report pod is unavailable")
			return
		}
		logs, logErr := backend.ReadPodLogs(r.Context(), namespace, pods[0].Metadata.Name, "diagnostic-agent")
		if logErr != nil {
			httpx.WriteError(w, http.StatusServiceUnavailable, "read diagnostic report: "+logErr.Error())
			return
		}
		report, err := decodeDiagnosticReport(logs)
		if err != nil {
			httpx.WriteError(w, http.StatusServiceUnavailable, "diagnostic report is invalid: "+err.Error())
			return
		}
		response["report"] = report
	}
	action := "app.diagnostics.read"
	if includeReport {
		action = "app.diagnostics.report.read"
	}
	s.appendAudit(principal, action, "diagnostic_session", sessionID, app.TenantID, map[string]string{"app_id": app.ID})
	httpx.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) handleCancelAppDiagnosticSession(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform administrator access required")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	sessionID := strings.TrimSpace(r.PathValue("session_id"))
	if !validDiagnosticSessionID(sessionID) {
		httpx.WriteError(w, http.StatusBadRequest, "invalid diagnostic session id")
		return
	}
	backend, err := s.newDiagnosticSessionBackend()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	namespace := backend.SessionNamespace()
	job, err := backend.GetJob(r.Context(), namespace, sessionID)
	if err != nil {
		writeDiagnosticBackendError(w, err)
		return
	}
	if job.Labels[diagnosticAppIDLabel] != app.ID || job.Labels[diagnosticManagedByLabel] != diagnosticManagedByValue {
		httpx.WriteError(w, http.StatusNotFound, "diagnostic session not found")
		return
	}
	if err := backend.DeleteJob(r.Context(), namespace, sessionID); err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	s.appendAudit(principal, "app.diagnostics.cancel", "diagnostic_session", sessionID, app.TenantID, map[string]string{"app_id": app.ID})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"session": diagnosticSessionFromJob(job), "canceled": true})
}

func canReadAppDiagnostics(principal model.Principal) bool {
	return principal.IsPlatformAdmin() || principal.HasScope("app.observability.read")
}

func normalizeDiagnosticStartRequest(req *diagnosticStartRequest) error {
	req.Kind = strings.TrimSpace(strings.ToLower(req.Kind))
	if req.Kind == "" {
		req.Kind = "cpu-profile"
	}
	if req.Kind != "cpu-profile" {
		return fmt.Errorf("unsupported diagnostic kind %q", req.Kind)
	}
	if req.DurationSeconds == 0 {
		req.DurationSeconds = diagnosticDefaultDuration
	}
	if req.DurationSeconds < diagnosticMinDuration || req.DurationSeconds > diagnosticMaxDuration {
		return fmt.Errorf("duration_seconds must be between %d and %d", diagnosticMinDuration, diagnosticMaxDuration)
	}
	if req.FrequencyHz == 0 {
		req.FrequencyHz = diagnosticDefaultFrequency
	}
	if req.FrequencyHz < 1 || req.FrequencyHz > diagnosticMaxFrequency {
		return fmt.Errorf("frequency_hz must be between 1 and %d", diagnosticMaxFrequency)
	}
	req.Pod = strings.TrimSpace(req.Pod)
	req.Container = strings.TrimSpace(req.Container)
	return nil
}

func (s *Server) resolveDiagnosticTarget(ctx context.Context, app model.App, req diagnosticStartRequest) (diagnosticTarget, error) {
	namespace := runtime.NamespaceForTenant(app.TenantID)
	selector, defaultContainer, err := runtimeLogTarget(app, "app")
	if err != nil {
		return diagnosticTarget{}, err
	}
	client, err := s.newLogsClient(namespace)
	if err != nil {
		return diagnosticTarget{}, err
	}
	pods, err := client.listPodsBySelector(ctx, namespace, selector)
	if err != nil {
		return diagnosticTarget{}, err
	}
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].Metadata.CreationTimestamp.After(pods[j].Metadata.CreationTimestamp)
	})
	container := firstNonEmptyString(req.Container, defaultContainer)
	for _, pod := range pods {
		if req.Pod != "" && pod.Metadata.Name != req.Pod {
			continue
		}
		if !strings.EqualFold(pod.Status.Phase, "Running") || pod.Spec.NodeName == "" {
			continue
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == container && status.Ready && strings.TrimSpace(status.ContainerID) != "" {
				return diagnosticTarget{Namespace: namespace, PodName: pod.Metadata.Name, Container: container, ContainerID: status.ContainerID, NodeName: pod.Spec.NodeName}, nil
			}
		}
	}
	return diagnosticTarget{}, errors.New("no ready target pod with a running container was found")
}

func buildDiagnosticJob(app model.App, sessionID, sessionNamespace string, target diagnosticTarget, req diagnosticStartRequest, image string) batchv1.Job {
	zero := int32(0)
	activeDeadline := int64(req.DurationSeconds + 45)
	privileged := false
	readOnly := true
	allowPrivilegeEscalation := false
	runAsUser := int64(0)
	automount := false
	hostPathDirectory := corev1.HostPathDirectory
	return batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sessionID,
			Namespace: sessionNamespace,
			Labels: map[string]string{
				diagnosticManagedByLabel: diagnosticManagedByValue,
				diagnosticAppIDLabel:     app.ID,
				diagnosticSessionIDLabel: sessionID,
				diagnosticKindLabel:      req.Kind,
			},
			Annotations: map[string]string{
				diagnosticTargetPodAnnotation:       target.PodName,
				diagnosticTargetContainerAnnotation: target.Container,
				diagnosticTargetNodeAnnotation:      target.NodeName,
				diagnosticTargetNamespaceAnnotation: target.Namespace,
				diagnosticDurationAnnotation:        strconv.Itoa(req.DurationSeconds),
				diagnosticFrequencyAnnotation:       strconv.Itoa(req.FrequencyHz),
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &zero,
			ActiveDeadlineSeconds:   &activeDeadline,
			TTLSecondsAfterFinished: ptrInt32Diagnostic(diagnosticRetentionSeconds),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					diagnosticManagedByLabel: diagnosticManagedByValue,
					diagnosticAppIDLabel:     app.ID,
					diagnosticSessionIDLabel: sessionID,
				}},
				Spec: corev1.PodSpec{
					NodeName:                      target.NodeName,
					HostPID:                       true,
					AutomountServiceAccountToken:  &automount,
					EnableServiceLinks:            &automount,
					RestartPolicy:                 corev1.RestartPolicyNever,
					TerminationGracePeriodSeconds: ptrInt64Diagnostic(5),
					Containers: []corev1.Container{{
						Name:            "diagnostic-agent",
						Image:           image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{"/usr/local/bin/fugue-diagnostic-agent"},
						Args:            []string{"--kind", req.Kind, "--duration", strconv.Itoa(req.DurationSeconds), "--frequency", strconv.Itoa(req.FrequencyHz), "--container-id", target.ContainerID},
						SecurityContext: &corev1.SecurityContext{
							Privileged:               &privileged,
							RunAsUser:                &runAsUser,
							ReadOnlyRootFilesystem:   &readOnly,
							AllowPrivilegeEscalation: &allowPrivilegeEscalation,
							Capabilities: &corev1.Capabilities{
								Drop: []corev1.Capability{"ALL"},
								Add:  []corev1.Capability{"PERFMON", "SYS_PTRACE"},
							},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
							Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("256Mi")},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "host-proc", MountPath: "/host/proc", ReadOnly: true},
							{Name: "host-cgroup", MountPath: "/sys/fs/cgroup", ReadOnly: true},
							{Name: "scratch", MountPath: "/tmp"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "host-proc", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/proc", Type: &hostPathDirectory}}},
						{Name: "host-cgroup", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/sys/fs/cgroup", Type: &hostPathDirectory}}},
						{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resourceQuantityPtr("32Mi")}}},
					},
				},
			},
		},
	}
}

func diagnosticSessionFromJob(job batchv1.Job) diagnosticSession {
	status := "queued"
	failure := ""
	if job.Status.Active > 0 {
		status = "running"
	}
	if job.Status.Succeeded > 0 {
		status = "succeeded"
	}
	if job.Status.Failed > 0 {
		status = "failed"
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			status = "failed"
			failure = firstNonEmptyString(condition.Message, condition.Reason)
		}
	}
	created := job.CreationTimestamp.Time.UTC()
	var expires *time.Time
	if !created.IsZero() {
		value := created.Add(time.Duration(diagnosticRetentionSeconds) * time.Second)
		expires = &value
	}
	return diagnosticSession{
		ID:              job.Name,
		AppID:           job.Labels[diagnosticAppIDLabel],
		Kind:            job.Labels[diagnosticKindLabel],
		Status:          status,
		TargetPod:       job.Annotations[diagnosticTargetPodAnnotation],
		TargetContainer: job.Annotations[diagnosticTargetContainerAnnotation],
		TargetNode:      job.Annotations[diagnosticTargetNodeAnnotation],
		DurationSeconds: parseDiagnosticInt(job.Annotations[diagnosticDurationAnnotation]),
		FrequencyHz:     parseDiagnosticInt(job.Annotations[diagnosticFrequencyAnnotation]),
		CreatedAt:       created,
		StartedAt:       timePtrDiagnostic(job.Status.StartTime),
		FinishedAt:      timePtrDiagnostic(job.Status.CompletionTime),
		ExpiresAt:       expires,
		FailureReason:   failure,
	}
}

func countActiveDiagnosticJobs(jobs []batchv1.Job, appID string) (int, int) {
	global, app := 0, 0
	for _, job := range jobs {
		if job.Status.Succeeded > 0 || job.Status.Failed > 0 {
			continue
		}
		global++
		if job.Labels[diagnosticAppIDLabel] == appID {
			app++
		}
	}
	return global, app
}

func validDiagnosticSessionID(value string) bool {
	return value != "" && len(value) <= 63 && model.DNS1035Label(value, "invalid") == value && strings.HasPrefix(value, "diagnostic-")
}

func writeDiagnosticBackendError(w http.ResponseWriter, err error) {
	var statusErr *kubeStatusError
	if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound {
		httpx.WriteError(w, http.StatusNotFound, "diagnostic session not found")
		return
	}
	httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
}

func (s *Server) newDiagnosticSessionBackend() (diagnosticSessionBackend, error) {
	if s.diagnosticSessionBackend != nil {
		return s.diagnosticSessionBackend, nil
	}
	cluster, err := s.requireClusterNodeClient()
	if err != nil {
		return nil, err
	}
	logs, err := newKubeLogsClient("")
	if err != nil {
		return nil, err
	}
	controlNamespace := strings.TrimSpace(s.controlPlaneNamespace)
	if controlNamespace == "" {
		controlNamespace, err = kubeNamespace()
		if err != nil {
			return nil, err
		}
	}
	return &kubeDiagnosticSessionBackend{cluster: cluster, logs: logs, controlNamespace: controlNamespace}, nil
}

func (b *kubeDiagnosticSessionBackend) RunnerImage(ctx context.Context) (string, error) {
	namespace := strings.TrimSpace(b.controlNamespace)
	if namespace == "" {
		var err error
		namespace, err = kubeNamespace()
		if err != nil {
			return "", err
		}
	}
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}
	var pod corev1.Pod
	path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods/" + url.PathEscape(hostname)
	if err := b.cluster.doJSON(ctx, http.MethodGet, path, &pod); err != nil {
		return "", err
	}
	for _, container := range pod.Spec.Containers {
		image := strings.TrimSpace(container.Image)
		if container.Name == "api" && image != "" && strings.Contains(image, "@sha256:") {
			return image, nil
		}
	}
	return "", errors.New("current control-plane image is unavailable or not digest-addressed")
}

func (b *kubeDiagnosticSessionBackend) SessionNamespace() string {
	return strings.TrimSpace(b.controlNamespace)
}

func (b *kubeDiagnosticSessionBackend) CreateJob(ctx context.Context, namespace string, job batchv1.Job) (batchv1.Job, error) {
	var created batchv1.Job
	err := b.cluster.doJSONWithBody(ctx, http.MethodPost, "/apis/batch/v1/namespaces/"+url.PathEscape(namespace)+"/jobs", job, &created)
	return created, err
}

func (b *kubeDiagnosticSessionBackend) GetJob(ctx context.Context, namespace, name string) (batchv1.Job, error) {
	var job batchv1.Job
	err := b.cluster.doJSON(ctx, http.MethodGet, "/apis/batch/v1/namespaces/"+url.PathEscape(namespace)+"/jobs/"+url.PathEscape(name), &job)
	return job, err
}

func (b *kubeDiagnosticSessionBackend) ListJobs(ctx context.Context, namespace, selector string) ([]batchv1.Job, error) {
	query := url.Values{}
	query.Set("labelSelector", selector)
	path := "/apis/batch/v1/jobs"
	if namespace != "" {
		path = "/apis/batch/v1/namespaces/" + url.PathEscape(namespace) + "/jobs"
	}
	var list batchv1.JobList
	if err := b.cluster.doJSON(ctx, http.MethodGet, path+"?"+query.Encode(), &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (b *kubeDiagnosticSessionBackend) DeleteJob(ctx context.Context, namespace, name string) error {
	policy := metav1.DeletePropagationBackground
	return b.cluster.doJSONWithBody(ctx, http.MethodDelete, "/apis/batch/v1/namespaces/"+url.PathEscape(namespace)+"/jobs/"+url.PathEscape(name), metav1.DeleteOptions{PropagationPolicy: &policy}, nil)
}

func (b *kubeDiagnosticSessionBackend) ListPods(ctx context.Context, namespace, selector string) ([]kubePodInfo, error) {
	return b.logs.listPodsBySelector(ctx, namespace, selector)
}

func (b *kubeDiagnosticSessionBackend) ReadPodLogs(ctx context.Context, namespace, pod, container string) (string, error) {
	return b.logs.readPodLogs(ctx, namespace, pod, kubeLogOptions{Container: container})
}

func parseDiagnosticInt(value string) int {
	result, _ := strconv.Atoi(strings.TrimSpace(value))
	return result
}

func ptrInt32Diagnostic(value int32) *int32 { return &value }
func ptrInt64Diagnostic(value int64) *int64 { return &value }

func timePtrDiagnostic(value *metav1.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func resourceQuantityPtr(value string) *resource.Quantity {
	result := resource.MustParse(value)
	return &result
}

func decodeDiagnosticReport(logs string) (any, error) {
	if len(logs) > diagnosticMaxReportBytes {
		return nil, fmt.Errorf("report exceeds %d bytes", diagnosticMaxReportBytes)
	}
	trimmed := strings.TrimSpace(logs)
	if trimmed == "" {
		return nil, errors.New("report is empty")
	}
	var report any
	if err := json.Unmarshal([]byte(trimmed), &report); err == nil {
		return report, nil
	}
	// Be tolerant of a runtime that prefixes log lines, while still accepting
	// only a complete JSON report emitted by the diagnostic agent.
	lines := strings.Split(trimmed, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		candidate := strings.TrimSpace(lines[i])
		if candidate == "" {
			continue
		}
		if err := json.Unmarshal([]byte(candidate), &report); err == nil {
			return report, nil
		}
	}
	return nil, errors.New("report is not valid JSON")
}

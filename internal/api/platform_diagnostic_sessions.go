package api

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/livediagnostics"
	"fugue/internal/model"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

const diagnosticMaxActivePerTarget = 1

type platformDiagnosticTargetRequest struct {
	Type        livediagnostics.TargetType `json:"type"`
	Component   string                     `json:"component,omitempty"`
	Namespace   string                     `json:"namespace,omitempty"`
	Pod         string                     `json:"pod,omitempty"`
	Container   string                     `json:"container,omitempty"`
	Node        string                     `json:"node,omitempty"`
	ProcessName string                     `json:"process_name,omitempty"`
}

type platformDiagnosticStartRequest struct {
	Target                     platformDiagnosticTargetRequest `json:"target"`
	Kind                       livediagnostics.ProbeKind       `json:"kind"`
	DurationSeconds            int                             `json:"duration_seconds"`
	FrequencyHz                int                             `json:"frequency_hz"`
	SampleIntervalMilliseconds int                             `json:"sample_interval_milliseconds,omitempty"`
}

type platformDiagnosticSession struct {
	ID                         string                    `json:"id"`
	Kind                       livediagnostics.ProbeKind `json:"kind"`
	Status                     string                    `json:"status"`
	Target                     livediagnostics.Target    `json:"target"`
	ControlPath                string                    `json:"control_path"`
	DurationSeconds            int                       `json:"duration_seconds"`
	FrequencyHz                int                       `json:"frequency_hz"`
	SampleIntervalMilliseconds int                       `json:"sample_interval_milliseconds"`
	CreatedAt                  time.Time                 `json:"created_at"`
	StartedAt                  *time.Time                `json:"started_at,omitempty"`
	FinishedAt                 *time.Time                `json:"finished_at,omitempty"`
	ExpiresAt                  *time.Time                `json:"expires_at,omitempty"`
	FailureReason              string                    `json:"failure_reason,omitempty"`
}

func (s *Server) handleStartPlatformDiagnosticSession(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	var request platformDiagnosticStartRequest
	if err := httpx.DecodeJSON(r, &request); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	probe := livediagnostics.StartRequest{
		Kind: request.Kind, DurationSeconds: request.DurationSeconds, FrequencyHz: request.FrequencyHz,
		SampleIntervalMilliseconds: request.SampleIntervalMilliseconds,
	}
	if err := probe.Normalize(); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	backend, err := s.newDiagnosticSessionBackend()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	active, err := backend.ListJobs(r.Context(), backend.SessionNamespace(), livediagnostics.ManagedByLabel+"="+livediagnostics.ManagedByValue)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "list active diagnostic sessions: "+err.Error())
		return
	}
	if countActiveDiagnosticJobsTotal(active) >= livediagnostics.MaxActiveGlobal {
		httpx.WriteError(w, http.StatusConflict, "diagnostic session concurrency limit reached")
		return
	}
	target, err := s.resolvePlatformDiagnosticTarget(r.Context(), backend, request.Target)
	if err != nil {
		httpx.WriteError(w, http.StatusConflict, err.Error())
		return
	}
	if countActivePlatformDiagnosticTargetJobs(active, target) >= diagnosticMaxActivePerTarget {
		httpx.WriteError(w, http.StatusConflict, "diagnostic target already has an active session")
		return
	}
	runnerImage, err := backend.RunnerImage(r.Context())
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "resolve diagnostic runner image: "+err.Error())
		return
	}
	sessionID := model.DNS1035Label(model.NewID("diagnostic"), "diagnostic")
	job, err := livediagnostics.BuildJob(target, sessionID, backend.SessionNamespace(), runnerImage, "api", probe)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, "build diagnostic session: "+err.Error())
		return
	}
	created, err := backend.CreateJob(r.Context(), backend.SessionNamespace(), job)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "create diagnostic session: "+err.Error())
		return
	}
	session := platformDiagnosticSessionFromJob(created)
	s.appendAudit(principal, "platform.diagnostics.start", "diagnostic_session", session.ID, principal.TenantID, map[string]string{
		"kind": string(probe.Kind), "target_type": string(target.Type), "target_node": target.Node,
		"target_pod": target.Pod, "target_component": target.Component, "target_process": target.ProcessName,
	})
	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{"session": session})
}

func (s *Server) handleListPlatformDiagnosticSessions(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	backend, err := s.newDiagnosticSessionBackend()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	jobs, err := backend.ListJobs(r.Context(), backend.SessionNamespace(), livediagnostics.ManagedByLabel+"="+livediagnostics.ManagedByValue)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "list diagnostic sessions: "+err.Error())
		return
	}
	sessions := make([]platformDiagnosticSession, 0, len(jobs))
	for _, job := range jobs {
		if platformDiagnosticJob(job) {
			sessions = append(sessions, platformDiagnosticSessionFromJob(job))
		}
	}
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].CreatedAt.After(sessions[j].CreatedAt) })
	s.appendAudit(principal, "platform.diagnostics.list", "cluster", "diagnostic_sessions", principal.TenantID, nil)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"sessions": sessions})
}

func (s *Server) handleGetPlatformDiagnosticSession(w http.ResponseWriter, r *http.Request) {
	s.handleGetPlatformDiagnosticSessionValue(w, r, false)
}

func (s *Server) handleGetPlatformDiagnosticReport(w http.ResponseWriter, r *http.Request) {
	s.handleGetPlatformDiagnosticSessionValue(w, r, true)
}

func (s *Server) handleGetPlatformDiagnosticSessionValue(w http.ResponseWriter, r *http.Request, includeReport bool) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
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
	job, err := backend.GetJob(r.Context(), backend.SessionNamespace(), sessionID)
	if err != nil {
		writeDiagnosticBackendError(w, err)
		return
	}
	if !platformDiagnosticJob(job) {
		httpx.WriteError(w, http.StatusNotFound, "diagnostic session not found")
		return
	}
	session := platformDiagnosticSessionFromJob(job)
	response := map[string]any{"session": session}
	if includeReport {
		if session.Status == "queued" || session.Status == "running" {
			httpx.WriteError(w, http.StatusConflict, "diagnostic report is not ready")
			return
		}
		pods, listErr := backend.ListPods(r.Context(), backend.SessionNamespace(), "job-name="+sessionID)
		if listErr != nil || len(pods) == 0 {
			httpx.WriteError(w, http.StatusServiceUnavailable, "diagnostic report pod is unavailable")
			return
		}
		logs, logErr := backend.ReadPodLogs(r.Context(), backend.SessionNamespace(), pods[0].Metadata.Name, livediagnostics.DiagnosticAgentContainer)
		if logErr != nil {
			httpx.WriteError(w, http.StatusServiceUnavailable, "read diagnostic report: "+logErr.Error())
			return
		}
		report, decodeErr := decodeDiagnosticReport(logs)
		if decodeErr != nil {
			httpx.WriteError(w, http.StatusServiceUnavailable, "diagnostic report is invalid: "+decodeErr.Error())
			return
		}
		response["report"] = report
	}
	action := "platform.diagnostics.read"
	if includeReport {
		action = "platform.diagnostics.report.read"
	}
	s.appendAudit(principal, action, "diagnostic_session", sessionID, principal.TenantID, nil)
	httpx.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) handleCancelPlatformDiagnosticSession(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
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
	job, err := backend.GetJob(r.Context(), backend.SessionNamespace(), sessionID)
	if err != nil {
		writeDiagnosticBackendError(w, err)
		return
	}
	if !platformDiagnosticJob(job) {
		httpx.WriteError(w, http.StatusNotFound, "diagnostic session not found")
		return
	}
	if err := backend.DeleteJob(r.Context(), backend.SessionNamespace(), sessionID); err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	s.appendAudit(principal, "platform.diagnostics.cancel", "diagnostic_session", sessionID, principal.TenantID, nil)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"session": platformDiagnosticSessionFromJob(job), "canceled": true})
}

func (s *Server) resolvePlatformDiagnosticTarget(ctx context.Context, backend diagnosticSessionBackend, request platformDiagnosticTargetRequest) (livediagnostics.Target, error) {
	request.Type = livediagnostics.TargetType(strings.ToLower(strings.TrimSpace(string(request.Type))))
	request.Component = strings.TrimSpace(request.Component)
	request.Namespace = strings.TrimSpace(request.Namespace)
	request.Pod = strings.TrimSpace(request.Pod)
	request.Container = strings.TrimSpace(request.Container)
	request.Node = strings.TrimSpace(request.Node)
	request.ProcessName = strings.TrimSpace(request.ProcessName)
	switch request.Type {
	case livediagnostics.TargetPlatformComponent:
		return s.resolvePlatformComponentDiagnosticTarget(ctx, backend, request)
	case livediagnostics.TargetNodeProcess:
		return resolveNodeProcessDiagnosticTarget(ctx, backend, request)
	default:
		return livediagnostics.Target{}, errors.New("target.type must be platform_component or node_process")
	}
}

func (s *Server) resolvePlatformComponentDiagnosticTarget(ctx context.Context, backend diagnosticSessionBackend, request platformDiagnosticTargetRequest) (livediagnostics.Target, error) {
	if request.Component == "" || model.DNS1035Label(request.Component, "invalid") != request.Component {
		return livediagnostics.Target{}, errors.New("target.component must be a valid component label")
	}
	namespace := request.Namespace
	if namespace == "" {
		namespace = strings.TrimSpace(s.controlPlaneNamespace)
	}
	if namespace == "" || namespace != strings.TrimSpace(s.controlPlaneNamespace) && namespace != "kube-system" {
		return livediagnostics.Target{}, errors.New("platform component diagnostics are limited to the Fugue and kube-system namespaces")
	}
	releaseInstance := strings.TrimSpace(s.controlPlaneReleaseInstance)
	if releaseInstance == "" {
		return livediagnostics.Target{}, errors.New("control-plane release instance is unavailable")
	}
	selector := "app.kubernetes.io/instance=" + releaseInstance + ",app.kubernetes.io/component=" + request.Component
	pods, err := backend.ListPods(ctx, namespace, selector)
	if err != nil {
		return livediagnostics.Target{}, err
	}
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].Metadata.CreationTimestamp.After(pods[j].Metadata.CreationTimestamp)
	})
	for _, pod := range pods {
		if request.Pod != "" && request.Pod != pod.Metadata.Name {
			continue
		}
		if pod.Metadata.UID == "" || pod.Spec.NodeName == "" || !strings.EqualFold(pod.Status.Phase, "Running") {
			continue
		}
		container := request.Container
		if container == "" && len(pod.Spec.Containers) == 1 {
			container = pod.Spec.Containers[0].Name
		}
		if container == "" {
			return livediagnostics.Target{}, errors.New("target.container is required for a multi-container platform pod")
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != container || !status.Ready || strings.TrimSpace(status.ContainerID) == "" {
				continue
			}
			target := livediagnostics.Target{
				Type: livediagnostics.TargetPlatformComponent, Component: request.Component, Namespace: namespace,
				Pod: pod.Metadata.Name, PodUID: pod.Metadata.UID, Container: container, ContainerID: status.ContainerID,
				Node: pod.Spec.NodeName, ImageDigest: firstNonEmptyString(status.ImageID, status.Image),
			}
			return target, target.ValidateResolved()
		}
	}
	return livediagnostics.Target{}, errors.New("no ready Fugue platform component target was found")
}

func resolveNodeProcessDiagnosticTarget(ctx context.Context, backend diagnosticSessionBackend, request platformDiagnosticTargetRequest) (livediagnostics.Target, error) {
	if request.Node == "" {
		return livediagnostics.Target{}, errors.New("target.node is required")
	}
	processName, err := livediagnostics.NormalizeNodeProcessName(request.ProcessName)
	if err != nil {
		return livediagnostics.Target{}, err
	}
	node, err := backend.GetNode(ctx, request.Node)
	if err != nil {
		return livediagnostics.Target{}, err
	}
	if node.Name != request.Node || !diagnosticNodeReady(node) {
		return livediagnostics.Target{}, errors.New("target node is not Ready")
	}
	target := livediagnostics.Target{Type: livediagnostics.TargetNodeProcess, Node: request.Node, ProcessName: processName}
	return target, target.ValidateResolved()
}

func diagnosticNodeReady(node corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func platformDiagnosticJob(job batchv1.Job) bool {
	if job.Labels[livediagnostics.ManagedByLabel] != livediagnostics.ManagedByValue {
		return false
	}
	targetType := livediagnostics.TargetType(job.Labels[livediagnostics.TargetTypeLabel])
	return targetType == livediagnostics.TargetPlatformComponent || targetType == livediagnostics.TargetNodeProcess
}

func platformDiagnosticSessionFromJob(job batchv1.Job) platformDiagnosticSession {
	base := diagnosticSessionFromJob(job)
	created := base.CreatedAt
	target := livediagnostics.Target{
		Type: livediagnostics.TargetType(job.Labels[livediagnostics.TargetTypeLabel]), AppID: job.Labels[livediagnostics.AppIDLabel],
		Component: job.Annotations[livediagnostics.TargetComponentAnnotation], Namespace: job.Annotations[livediagnostics.TargetNamespaceAnnotation],
		Pod: job.Annotations[livediagnostics.TargetPodAnnotation], PodUID: job.Annotations[livediagnostics.TargetPodUIDAnnotation],
		Container: job.Annotations[livediagnostics.TargetContainerAnnotation], Node: job.Annotations[livediagnostics.TargetNodeAnnotation],
		ProcessName: job.Annotations[livediagnostics.TargetProcessAnnotation], ImageDigest: job.Annotations[livediagnostics.TargetImageAnnotation],
	}
	return platformDiagnosticSession{
		ID: base.ID, Kind: livediagnostics.ProbeKind(base.Kind), Status: base.Status, Target: target,
		ControlPath: job.Labels[livediagnostics.ControlPathLabel], DurationSeconds: base.DurationSeconds, FrequencyHz: base.FrequencyHz,
		SampleIntervalMilliseconds: parseDiagnosticInt(job.Annotations[livediagnostics.SampleIntervalAnnotation]),
		CreatedAt:                  created, StartedAt: base.StartedAt, FinishedAt: base.FinishedAt, ExpiresAt: base.ExpiresAt, FailureReason: base.FailureReason,
	}
}

func countActiveDiagnosticJobsTotal(jobs []batchv1.Job) int {
	count := 0
	for _, job := range jobs {
		if job.Status.Succeeded == 0 && job.Status.Failed == 0 {
			count++
		}
	}
	return count
}

func countActivePlatformDiagnosticTargetJobs(jobs []batchv1.Job, target livediagnostics.Target) int {
	count := 0
	for _, job := range jobs {
		if job.Status.Succeeded > 0 || job.Status.Failed > 0 {
			continue
		}
		if target.Type == livediagnostics.TargetNodeProcess {
			if job.Labels[livediagnostics.TargetTypeLabel] == string(target.Type) && job.Annotations[livediagnostics.TargetNodeAnnotation] == target.Node && job.Annotations[livediagnostics.TargetProcessAnnotation] == target.ProcessName {
				count++
			}
			continue
		}
		if job.Labels[livediagnostics.TargetTypeLabel] == string(target.Type) && job.Annotations[livediagnostics.TargetPodUIDAnnotation] == target.PodUID && job.Annotations[livediagnostics.TargetContainerAnnotation] == target.Container {
			count++
		}
	}
	return count
}

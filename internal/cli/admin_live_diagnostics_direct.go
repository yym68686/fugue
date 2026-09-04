package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
	"fugue/internal/model"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const directDiagnosticMaxReportBytes = 8 << 20

type directPlatformDiagnosticClient struct {
	client          kubernetes.Interface
	controlNS       string
	releaseInstance string
}

func newDirectPlatformDiagnosticClient(opts platformDiagnosticCommandOptions) (*directPlatformDiagnosticClient, error) {
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	if value := strings.TrimSpace(opts.kubeconfig); value != "" {
		loading.ExplicitPath = value
	}
	overrides := &clientcmd.ConfigOverrides{}
	if value := strings.TrimSpace(opts.kubeContext); value != "" {
		overrides.CurrentContext = value
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes client configuration: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes diagnostic client: %w", err)
	}
	controlNS := strings.TrimSpace(opts.controlNS)
	if controlNS == "" {
		return nil, errors.New("control-namespace is required for direct Kubernetes diagnostics")
	}
	releaseInstance := strings.TrimSpace(opts.releaseInstance)
	if releaseInstance == "" {
		return nil, errors.New("release-instance is required for direct Kubernetes diagnostics")
	}
	return &directPlatformDiagnosticClient{client: client, controlNS: controlNS, releaseInstance: releaseInstance}, nil
}

func (c *directPlatformDiagnosticClient) Start(ctx context.Context, request platformDiagnosticStartRequest) (platformDiagnosticSessionResponse, error) {
	probe := livediagnostics.StartRequest{
		Kind: request.Kind, DurationSeconds: request.DurationSeconds, FrequencyHz: request.FrequencyHz,
		SampleIntervalMilliseconds: request.SampleIntervalMilliseconds,
	}
	if err := probe.Normalize(); err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	jobs, err := c.client.BatchV1().Jobs(c.controlNS).List(ctx, metav1.ListOptions{LabelSelector: livediagnostics.ManagedByLabel + "=" + livediagnostics.ManagedByValue})
	if err != nil {
		return platformDiagnosticSessionResponse{}, fmt.Errorf("list active diagnostic sessions: %w", err)
	}
	if directActiveDiagnosticCount(jobs.Items) >= livediagnostics.MaxActiveGlobal {
		return platformDiagnosticSessionResponse{}, errors.New("diagnostic session concurrency limit reached")
	}
	target, err := c.resolveTarget(ctx, request.Target)
	if err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	if directActiveTargetCount(jobs.Items, target) > 0 {
		return platformDiagnosticSessionResponse{}, errors.New("diagnostic target already has an active session")
	}
	runnerImage, err := c.runnerImage(ctx)
	if err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	sessionID := model.DNS1035Label(model.NewID("diagnostic"), "diagnostic")
	job, err := livediagnostics.BuildJob(target, sessionID, c.controlNS, runnerImage, "direct-kubernetes", probe)
	if err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	created, err := c.client.BatchV1().Jobs(c.controlNS).Create(ctx, &job, metav1.CreateOptions{})
	if err != nil {
		return platformDiagnosticSessionResponse{}, fmt.Errorf("create diagnostic session: %w", err)
	}
	return platformDiagnosticSessionResponse{Session: directPlatformDiagnosticSession(*created)}, nil
}

func (c *directPlatformDiagnosticClient) List(ctx context.Context) (platformDiagnosticSessionListResponse, error) {
	jobs, err := c.client.BatchV1().Jobs(c.controlNS).List(ctx, metav1.ListOptions{LabelSelector: livediagnostics.ManagedByLabel + "=" + livediagnostics.ManagedByValue})
	if err != nil {
		return platformDiagnosticSessionListResponse{}, fmt.Errorf("list diagnostic sessions: %w", err)
	}
	response := platformDiagnosticSessionListResponse{Sessions: make([]platformDiagnosticSession, 0, len(jobs.Items))}
	for _, job := range jobs.Items {
		if directPlatformDiagnosticJob(job) {
			response.Sessions = append(response.Sessions, directPlatformDiagnosticSession(job))
		}
	}
	return response, nil
}

func (c *directPlatformDiagnosticClient) Get(ctx context.Context, sessionID string) (platformDiagnosticSessionResponse, error) {
	job, err := c.getJob(ctx, sessionID)
	if err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	return platformDiagnosticSessionResponse{Session: directPlatformDiagnosticSession(job)}, nil
}

func (c *directPlatformDiagnosticClient) Report(ctx context.Context, sessionID string) (platformDiagnosticReportResponse, error) {
	job, err := c.getJob(ctx, sessionID)
	if err != nil {
		return platformDiagnosticReportResponse{}, err
	}
	session := directPlatformDiagnosticSession(job)
	if session.Status == "queued" || session.Status == "running" {
		return platformDiagnosticReportResponse{}, errors.New("diagnostic report is not ready")
	}
	pods, err := c.client.CoreV1().Pods(c.controlNS).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + session.ID})
	if err != nil || len(pods.Items) == 0 {
		if err != nil {
			return platformDiagnosticReportResponse{}, fmt.Errorf("list diagnostic report pods: %w", err)
		}
		return platformDiagnosticReportResponse{}, errors.New("diagnostic report pod is unavailable")
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].CreationTimestamp.After(pods.Items[j].CreationTimestamp.Time)
	})
	stream, err := c.client.CoreV1().Pods(c.controlNS).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{Container: livediagnostics.DiagnosticAgentContainer}).Stream(ctx)
	if err != nil {
		return platformDiagnosticReportResponse{}, fmt.Errorf("read diagnostic report: %w", err)
	}
	defer stream.Close()
	body, err := io.ReadAll(io.LimitReader(stream, directDiagnosticMaxReportBytes+1))
	if err != nil {
		return platformDiagnosticReportResponse{}, fmt.Errorf("read diagnostic report: %w", err)
	}
	report, err := decodeDirectDiagnosticReport(body)
	if err != nil {
		return platformDiagnosticReportResponse{}, err
	}
	return platformDiagnosticReportResponse{Session: session, Report: report}, nil
}

func (c *directPlatformDiagnosticClient) Cancel(ctx context.Context, sessionID string) (platformDiagnosticSessionCancelResponse, error) {
	job, err := c.getJob(ctx, sessionID)
	if err != nil {
		return platformDiagnosticSessionCancelResponse{}, err
	}
	policy := metav1.DeletePropagationBackground
	if err := c.client.BatchV1().Jobs(c.controlNS).Delete(ctx, job.Name, metav1.DeleteOptions{PropagationPolicy: &policy}); err != nil {
		return platformDiagnosticSessionCancelResponse{}, fmt.Errorf("delete diagnostic session: %w", err)
	}
	return platformDiagnosticSessionCancelResponse{Session: directPlatformDiagnosticSession(job), Canceled: true}, nil
}

func (c *directPlatformDiagnosticClient) getJob(ctx context.Context, sessionID string) (batchv1.Job, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || len(sessionID) > 63 || model.DNS1035Label(sessionID, "invalid") != sessionID || !strings.HasPrefix(sessionID, "diagnostic-") {
		return batchv1.Job{}, errors.New("invalid diagnostic session id")
	}
	job, err := c.client.BatchV1().Jobs(c.controlNS).Get(ctx, sessionID, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return batchv1.Job{}, errors.New("diagnostic session not found")
	}
	if err != nil {
		return batchv1.Job{}, fmt.Errorf("get diagnostic session: %w", err)
	}
	if !directPlatformDiagnosticJob(*job) {
		return batchv1.Job{}, errors.New("diagnostic session not found")
	}
	return *job, nil
}

func (c *directPlatformDiagnosticClient) runnerImage(ctx context.Context) (string, error) {
	deploymentName := c.releaseInstance + "-fugue-api"
	deployment, err := c.client.AppsV1().Deployments(c.controlNS).Get(ctx, deploymentName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("resolve independent diagnostic runner: %w", err)
	}
	for _, container := range deployment.Spec.Template.Spec.Containers {
		image := strings.TrimSpace(container.Image)
		if container.Name == "api" && strings.Contains(image, "@sha256:") {
			return image, nil
		}
	}
	return "", errors.New("diagnostic runner does not use a digest-addressed image")
}

func (c *directPlatformDiagnosticClient) resolveTarget(ctx context.Context, request platformDiagnosticTargetRequest) (livediagnostics.Target, error) {
	request.Type = livediagnostics.TargetType(strings.ToLower(strings.TrimSpace(string(request.Type))))
	switch request.Type {
	case livediagnostics.TargetPlatformComponent:
		return c.resolveComponentTarget(ctx, request)
	case livediagnostics.TargetNodeProcess:
		return c.resolveNodeProcessTarget(ctx, request)
	default:
		return livediagnostics.Target{}, errors.New("target-type must be platform_component or node_process")
	}
}

func (c *directPlatformDiagnosticClient) resolveComponentTarget(ctx context.Context, request platformDiagnosticTargetRequest) (livediagnostics.Target, error) {
	component := strings.TrimSpace(request.Component)
	if component == "" || model.DNS1035Label(component, "invalid") != component {
		return livediagnostics.Target{}, errors.New("component must be a valid component label")
	}
	namespace := strings.TrimSpace(request.Namespace)
	if namespace == "" {
		namespace = c.controlNS
	}
	if namespace != c.controlNS && namespace != "kube-system" {
		return livediagnostics.Target{}, errors.New("platform component diagnostics are limited to the Fugue and kube-system namespaces")
	}
	selector := "app.kubernetes.io/instance=" + c.releaseInstance + ",app.kubernetes.io/component=" + component
	pods, err := c.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return livediagnostics.Target{}, fmt.Errorf("list platform component pods: %w", err)
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].CreationTimestamp.After(pods.Items[j].CreationTimestamp.Time)
	})
	for _, pod := range pods.Items {
		if value := strings.TrimSpace(request.Pod); value != "" && pod.Name != value {
			continue
		}
		if pod.UID == "" || pod.Spec.NodeName == "" || pod.Status.Phase != corev1.PodRunning {
			continue
		}
		containerName := strings.TrimSpace(request.Container)
		if containerName == "" && len(pod.Spec.Containers) == 1 {
			containerName = pod.Spec.Containers[0].Name
		}
		if containerName == "" {
			return livediagnostics.Target{}, errors.New("container is required for a multi-container platform Pod")
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != containerName || !status.Ready || strings.TrimSpace(status.ContainerID) == "" {
				continue
			}
			target := livediagnostics.Target{
				Type: livediagnostics.TargetPlatformComponent, Component: component, Namespace: namespace,
				Pod: pod.Name, PodUID: string(pod.UID), Container: containerName, ContainerID: status.ContainerID,
				Node: pod.Spec.NodeName, ImageDigest: firstNonEmptyTrimmed(status.ImageID, status.Image),
			}
			return target, target.ValidateResolved()
		}
	}
	return livediagnostics.Target{}, errors.New("no ready Fugue platform component target was found")
}

func (c *directPlatformDiagnosticClient) resolveNodeProcessTarget(ctx context.Context, request platformDiagnosticTargetRequest) (livediagnostics.Target, error) {
	nodeName := strings.TrimSpace(request.Node)
	if nodeName == "" {
		return livediagnostics.Target{}, errors.New("node is required for a node_process target")
	}
	processName, err := livediagnostics.NormalizeNodeProcessName(request.ProcessName)
	if err != nil {
		return livediagnostics.Target{}, err
	}
	node, err := c.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return livediagnostics.Target{}, fmt.Errorf("get target node: %w", err)
	}
	ready := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
			ready = true
			break
		}
	}
	if !ready {
		return livediagnostics.Target{}, errors.New("target node is not Ready")
	}
	target := livediagnostics.Target{Type: livediagnostics.TargetNodeProcess, Node: nodeName, ProcessName: processName}
	return target, target.ValidateResolved()
}

func directPlatformDiagnosticJob(job batchv1.Job) bool {
	if job.Labels[livediagnostics.ManagedByLabel] != livediagnostics.ManagedByValue {
		return false
	}
	targetType := livediagnostics.TargetType(job.Labels[livediagnostics.TargetTypeLabel])
	return targetType == livediagnostics.TargetPlatformComponent || targetType == livediagnostics.TargetNodeProcess
}

func directPlatformDiagnosticSession(job batchv1.Job) platformDiagnosticSession {
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
			failure = firstNonEmptyTrimmed(condition.Message, condition.Reason)
		}
	}
	created := job.CreationTimestamp.Time.UTC()
	var expires *time.Time
	if !created.IsZero() {
		value := created.Add(time.Duration(livediagnostics.RetentionSeconds) * time.Second)
		expires = &value
	}
	target := livediagnostics.Target{
		Type: livediagnostics.TargetType(job.Labels[livediagnostics.TargetTypeLabel]), AppID: job.Labels[livediagnostics.AppIDLabel],
		Component: job.Annotations[livediagnostics.TargetComponentAnnotation], Namespace: job.Annotations[livediagnostics.TargetNamespaceAnnotation],
		Pod: job.Annotations[livediagnostics.TargetPodAnnotation], PodUID: job.Annotations[livediagnostics.TargetPodUIDAnnotation],
		Container: job.Annotations[livediagnostics.TargetContainerAnnotation], Node: job.Annotations[livediagnostics.TargetNodeAnnotation],
		ProcessName: job.Annotations[livediagnostics.TargetProcessAnnotation], ImageDigest: job.Annotations[livediagnostics.TargetImageAnnotation],
	}
	return platformDiagnosticSession{
		ID: job.Name, Kind: livediagnostics.ProbeKind(job.Labels[livediagnostics.KindLabel]), Status: status, Target: target,
		ControlPath: job.Labels[livediagnostics.ControlPathLabel], DurationSeconds: directDiagnosticInt(job.Annotations[livediagnostics.DurationAnnotation]),
		FrequencyHz:                directDiagnosticInt(job.Annotations[livediagnostics.FrequencyAnnotation]),
		SampleIntervalMilliseconds: directDiagnosticInt(job.Annotations[livediagnostics.SampleIntervalAnnotation]),
		CreatedAt:                  created, StartedAt: directDiagnosticTime(job.Status.StartTime), FinishedAt: directDiagnosticTime(job.Status.CompletionTime),
		ExpiresAt: expires, FailureReason: failure,
	}
}

func directActiveDiagnosticCount(jobs []batchv1.Job) int {
	count := 0
	for _, job := range jobs {
		if job.Status.Succeeded == 0 && job.Status.Failed == 0 {
			count++
		}
	}
	return count
}

func directActiveTargetCount(jobs []batchv1.Job, target livediagnostics.Target) int {
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

func directDiagnosticInt(value string) int {
	result, _ := strconv.Atoi(strings.TrimSpace(value))
	return result
}

func directDiagnosticTime(value *metav1.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := value.Time.UTC()
	return &result
}

func decodeDirectDiagnosticReport(body []byte) (map[string]any, error) {
	if len(body) > directDiagnosticMaxReportBytes {
		return nil, fmt.Errorf("diagnostic report exceeds %d bytes", directDiagnosticMaxReportBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	var report map[string]any
	if err := decoder.Decode(&report); err != nil {
		return nil, fmt.Errorf("decode diagnostic report: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("diagnostic report contains trailing data")
	}
	return report, nil
}

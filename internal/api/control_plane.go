package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

const (
	controlPlaneComponentAPI        = "api"
	controlPlaneComponentController = "controller"

	controlPlaneStatusReady    = "ready"
	controlPlaneStatusRolling  = "rolling"
	controlPlaneStatusMixed    = "mixed"
	controlPlaneStatusDegraded = "degraded"
	controlPlaneStatusMissing  = "missing"
)

type kubeDeploymentList struct {
	Items []kubeDeployment `json:"items"`
}

type kubeDeployment struct {
	Metadata struct {
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		Replicas *int32 `json:"replicas,omitempty"`
		Template struct {
			Spec struct {
				Containers []kubeContainer `json:"containers"`
			} `json:"spec"`
		} `json:"template"`
	} `json:"spec"`
	Status struct {
		ReadyReplicas     int32 `json:"readyReplicas,omitempty"`
		UpdatedReplicas   int32 `json:"updatedReplicas,omitempty"`
		AvailableReplicas int32 `json:"availableReplicas,omitempty"`
	} `json:"status"`
}

type kubeContainer struct {
	Name  string `json:"name"`
	Image string `json:"image"`
}

type githubWorkflowRunsResponse struct {
	WorkflowRuns []githubWorkflowRun `json:"workflow_runs"`
}

type githubWorkflowRun struct {
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	RunNumber  int    `json:"run_number"`
	Event      string `json:"event"`
	HeadBranch string `json:"head_branch"`
	HeadSHA    string `json:"head_sha"`
	HTMLURL    string `json:"html_url"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
}

func (s *Server) handleGetControlPlaneStatus(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}

	namespace := strings.TrimSpace(s.controlPlaneNamespace)
	if namespace == "" {
		httpx.WriteError(w, http.StatusServiceUnavailable, "control plane namespace is not configured")
		return
	}

	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	controlPlane, err := client.readControlPlaneStatus(
		r.Context(),
		namespace,
		strings.TrimSpace(s.controlPlaneReleaseInstance),
	)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if workflow := s.readControlPlaneWorkflowRun(r.Context()); workflow != nil {
		controlPlane.DeployWorkflow = workflow
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{"control_plane": controlPlane})
}

func (c *clusterNodeClient) readControlPlaneStatus(
	ctx context.Context,
	namespace string,
	releaseInstance string,
) (model.ControlPlaneStatus, error) {
	deployments, err := c.listDeployments(ctx, namespace)
	if err != nil {
		return model.ControlPlaneStatus{}, fmt.Errorf("list control plane deployments: %w", err)
	}

	apiDeployment := findControlPlaneDeployment(deployments, controlPlaneComponentAPI, releaseInstance)
	controllerDeployment := findControlPlaneDeployment(deployments, controlPlaneComponentController, releaseInstance)
	if releaseInstance == "" {
		releaseInstance = readControlPlaneReleaseInstance(apiDeployment, controllerDeployment)
	}

	apiPods, err := c.listControlPlaneComponentPods(ctx, namespace, controlPlaneComponentAPI, releaseInstance)
	if err != nil {
		apiPods = nil
	}
	controllerPods, err := c.listControlPlaneComponentPods(ctx, namespace, controlPlaneComponentController, releaseInstance)
	if err != nil {
		controllerPods = nil
	}
	warnings, warningErr := c.readControlPlaneWarnings(ctx, namespace, releaseInstance)
	if warningErr != nil {
		warnings = append(warnings, model.ControlPlaneWarning{
			Severity:  "warning",
			Namespace: namespace,
			Name:      "control-plane-pod-scan",
			Status:    "unavailable",
			Message:   warningErr.Error(),
		})
	}

	components := []model.ControlPlaneComponent{
		buildControlPlaneComponent(controlPlaneComponentAPI, apiDeployment, apiPods),
		buildControlPlaneComponent(controlPlaneComponentController, controllerDeployment, controllerPods),
	}
	version := readCommonControlPlaneVersion(components)
	liveVersion := readCommonObservedControlPlaneVersion(components)

	return model.ControlPlaneStatus{
		Namespace:       namespace,
		ReleaseInstance: releaseInstance,
		Version:         version,
		LiveVersion:     liveVersion,
		Status:          readControlPlaneStatus(components, version),
		ObservedAt:      time.Now().UTC(),
		Components:      components,
		Warnings:        warnings,
	}, nil
}

func (c *clusterNodeClient) listDeployments(ctx context.Context, namespace string) ([]kubeDeployment, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}

	var deploymentList kubeDeploymentList
	apiPath := "/apis/apps/v1/namespaces/" + url.PathEscape(namespace) + "/deployments"
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &deploymentList); err != nil {
		return nil, err
	}
	return deploymentList.Items, nil
}

func (c *clusterNodeClient) listControlPlaneComponentPods(
	ctx context.Context,
	namespace string,
	component string,
	releaseInstance string,
) ([]model.ControlPlanePod, error) {
	namespace = strings.TrimSpace(namespace)
	component = strings.TrimSpace(component)
	if namespace == "" || component == "" {
		return nil, fmt.Errorf("namespace and component are required")
	}

	query := url.Values{}
	selector := []string{"app.kubernetes.io/component=" + component}
	if releaseInstance = strings.TrimSpace(releaseInstance); releaseInstance != "" {
		selector = append(selector, "app.kubernetes.io/instance="+releaseInstance)
	}
	query.Set("labelSelector", strings.Join(selector, ","))

	var podList kubePodList
	apiPath := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods?" + query.Encode()
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &podList); err != nil {
		return nil, err
	}

	pods := make([]model.ControlPlanePod, 0, len(podList.Items))
	for _, pod := range podList.Items {
		phase := strings.TrimSpace(pod.Status.Phase)
		if strings.EqualFold(phase, "Succeeded") || strings.EqualFold(phase, "Failed") {
			continue
		}
		pods = append(pods, controlPlanePodFromKube(component, pod))
	}
	sort.Slice(pods, func(i, j int) bool {
		left := controlPlanePodTimestamp(pods[i])
		right := controlPlanePodTimestamp(pods[j])
		if !left.Equal(right) {
			return left.After(right)
		}
		return strings.TrimSpace(pods[i].Name) < strings.TrimSpace(pods[j].Name)
	})
	return pods, nil
}

func (c *clusterNodeClient) readControlPlaneWarnings(ctx context.Context, namespace string, releaseInstance string) ([]model.ControlPlaneWarning, error) {
	pods, err := c.listControlPlaneNamespacePods(ctx, namespace)
	if err != nil {
		return nil, err
	}
	warnings := make([]model.ControlPlaneWarning, 0)
	for _, pod := range pods {
		if !controlPlanePodMatchesRelease(pod, releaseInstance) {
			continue
		}
		if controlPlanePodIsCoreComponent(pod) {
			continue
		}
		phase := strings.TrimSpace(pod.Status.Phase)
		if strings.EqualFold(phase, "Succeeded") || strings.EqualFold(phase, "Failed") {
			continue
		}
		if strings.EqualFold(phase, "Running") && allKubePodContainersReady(pod) {
			continue
		}
		warnings = append(warnings, controlPlaneWarningFromPod(namespace, pod))
	}
	sort.Slice(warnings, func(i, j int) bool {
		if warnings[i].Severity != warnings[j].Severity {
			return warnings[i].Severity < warnings[j].Severity
		}
		return warnings[i].Name < warnings[j].Name
	})
	return warnings, nil
}

func (c *clusterNodeClient) listControlPlaneNamespacePods(ctx context.Context, namespace string) ([]kubePodInfo, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}
	var podList kubePodList
	apiPath := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods"
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &podList); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

func controlPlanePodMatchesRelease(pod kubePodInfo, releaseInstance string) bool {
	releaseInstance = strings.TrimSpace(releaseInstance)
	if releaseInstance == "" {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(pod.Metadata.Labels["app.kubernetes.io/instance"]), releaseInstance)
}

func controlPlanePodIsCoreComponent(pod kubePodInfo) bool {
	component := strings.TrimSpace(pod.Metadata.Labels["app.kubernetes.io/component"])
	return strings.EqualFold(component, controlPlaneComponentAPI) ||
		strings.EqualFold(component, controlPlaneComponentController)
}

func allKubePodContainersReady(pod kubePodInfo) bool {
	statuses := append([]kubeContainerStatus(nil), pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	if len(statuses) == 0 {
		return false
	}
	for _, status := range statuses {
		if !status.Ready {
			return false
		}
	}
	return true
}

func controlPlaneWarningFromPod(namespace string, pod kubePodInfo) model.ControlPlaneWarning {
	kind := "Pod"
	name := strings.TrimSpace(pod.Metadata.Name)
	if len(pod.Metadata.OwnerReferences) > 0 {
		if ownerKind := strings.TrimSpace(pod.Metadata.OwnerReferences[0].Kind); ownerKind != "" {
			kind = ownerKind
		}
		if ownerName := strings.TrimSpace(pod.Metadata.OwnerReferences[0].Name); ownerName != "" {
			name = ownerName
		}
	}
	reason, message := controlPlanePodWarningReason(pod)
	return model.ControlPlaneWarning{
		Severity:  "warning",
		Namespace: firstNonEmptyString(strings.TrimSpace(pod.Metadata.Namespace), strings.TrimSpace(namespace)),
		Kind:      kind,
		Name:      name,
		Status:    strings.TrimSpace(pod.Status.Phase),
		Reason:    reason,
		Message:   message,
	}
}

func controlPlanePodWarningReason(pod kubePodInfo) (string, string) {
	if reason := strings.TrimSpace(pod.Status.Reason); reason != "" {
		return reason, strings.TrimSpace(pod.Status.Message)
	}
	statuses := append([]kubeContainerStatus(nil), pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	for _, status := range statuses {
		if status.Ready {
			continue
		}
		if status.State.Waiting != nil {
			return strings.TrimSpace(status.State.Waiting.Reason), strings.TrimSpace(status.State.Waiting.Message)
		}
		if status.State.Terminated != nil {
			return strings.TrimSpace(status.State.Terminated.Reason), strings.TrimSpace(status.State.Terminated.Message)
		}
	}
	return "", strings.TrimSpace(pod.Status.Message)
}

func findControlPlaneDeployment(
	deployments []kubeDeployment,
	component string,
	releaseInstance string,
) *kubeDeployment {
	for index := range deployments {
		deployment := &deployments[index]
		if !deploymentMatchesControlPlaneComponent(deployment, component) {
			continue
		}
		if releaseInstance != "" && readDeploymentReleaseInstance(deployment) != releaseInstance {
			continue
		}
		return deployment
	}

	if releaseInstance != "" {
		return nil
	}

	for index := range deployments {
		deployment := &deployments[index]
		if deploymentMatchesControlPlaneComponent(deployment, component) {
			return deployment
		}
	}
	return nil
}

func deploymentMatchesControlPlaneComponent(deployment *kubeDeployment, component string) bool {
	if deployment == nil {
		return false
	}
	if strings.EqualFold(
		strings.TrimSpace(deployment.Metadata.Labels["app.kubernetes.io/component"]),
		component,
	) {
		return true
	}
	return strings.HasSuffix(strings.TrimSpace(deployment.Metadata.Name), "-"+component)
}

func readDeploymentReleaseInstance(deployment *kubeDeployment) string {
	if deployment == nil {
		return ""
	}
	return strings.TrimSpace(deployment.Metadata.Labels["app.kubernetes.io/instance"])
}

func readControlPlaneReleaseInstance(
	apiDeployment *kubeDeployment,
	controllerDeployment *kubeDeployment,
) string {
	if releaseInstance := readDeploymentReleaseInstance(apiDeployment); releaseInstance != "" {
		return releaseInstance
	}
	return readDeploymentReleaseInstance(controllerDeployment)
}

func buildControlPlaneComponent(component string, deployment *kubeDeployment, pods []model.ControlPlanePod) model.ControlPlaneComponent {
	if deployment == nil {
		return model.ControlPlaneComponent{
			Component: component,
			Status:    controlPlaneStatusMissing,
		}
	}

	desiredReplicas := 1
	if deployment.Spec.Replicas != nil {
		desiredReplicas = int(*deployment.Spec.Replicas)
	}

	image := readControlPlaneImage(deployment, component)
	imageRepository, imageTag := splitImageReference(image)
	controlPlaneComponent := model.ControlPlaneComponent{
		Component:         component,
		DeploymentName:    strings.TrimSpace(deployment.Metadata.Name),
		Image:             image,
		ImageRepository:   imageRepository,
		ImageTag:          imageTag,
		ObservedImageTags: observedControlPlaneImageTags(pods),
		ObservedPods:      append([]model.ControlPlanePod(nil), pods...),
		DesiredReplicas:   desiredReplicas,
		ReadyReplicas:     int(deployment.Status.ReadyReplicas),
		UpdatedReplicas:   int(deployment.Status.UpdatedReplicas),
		AvailableReplicas: int(deployment.Status.AvailableReplicas),
	}
	controlPlaneComponent.Status = readControlPlaneComponentStatus(controlPlaneComponent)
	return controlPlaneComponent
}

func readControlPlaneImage(deployment *kubeDeployment, component string) string {
	if deployment == nil {
		return ""
	}

	for _, container := range deployment.Spec.Template.Spec.Containers {
		if strings.EqualFold(strings.TrimSpace(container.Name), component) {
			return strings.TrimSpace(container.Image)
		}
	}
	if len(deployment.Spec.Template.Spec.Containers) == 0 {
		return ""
	}
	return strings.TrimSpace(deployment.Spec.Template.Spec.Containers[0].Image)
}

func splitImageReference(image string) (string, string) {
	image = strings.TrimSpace(image)
	if image == "" {
		return "", ""
	}
	if atIndex := strings.Index(image, "@"); atIndex >= 0 {
		image = image[:atIndex]
	}

	lastSlash := strings.LastIndex(image, "/")
	lastColon := strings.LastIndex(image, ":")
	if lastColon > lastSlash {
		return image[:lastColon], image[lastColon+1:]
	}
	return image, ""
}

func readControlPlaneComponentStatus(component model.ControlPlaneComponent) string {
	if strings.TrimSpace(component.DeploymentName) == "" {
		return controlPlaneStatusMissing
	}
	if component.DesiredReplicas <= 0 {
		return controlPlaneStatusDegraded
	}
	if component.ReadyReplicas >= component.DesiredReplicas &&
		component.UpdatedReplicas >= component.DesiredReplicas &&
		component.AvailableReplicas >= component.DesiredReplicas {
		return controlPlaneStatusReady
	}
	if component.ReadyReplicas > 0 ||
		component.UpdatedReplicas > 0 ||
		component.AvailableReplicas > 0 {
		return controlPlaneStatusRolling
	}
	return controlPlaneStatusDegraded
}

func readCommonControlPlaneVersion(components []model.ControlPlaneComponent) string {
	version := ""
	for _, component := range components {
		tag := strings.TrimSpace(component.ImageTag)
		if tag == "" {
			return ""
		}
		if version == "" {
			version = tag
			continue
		}
		if version != tag {
			return ""
		}
	}
	return version
}

func readCommonObservedControlPlaneVersion(components []model.ControlPlaneComponent) string {
	version := ""
	for _, component := range components {
		if len(component.ObservedImageTags) != 1 {
			return ""
		}
		tag := strings.TrimSpace(component.ObservedImageTags[0])
		if tag == "" {
			return ""
		}
		if version == "" {
			version = tag
			continue
		}
		if version != tag {
			return ""
		}
	}
	return version
}

func readControlPlaneStatus(components []model.ControlPlaneComponent, commonVersion string) string {
	hasRolling := false
	allReady := len(components) > 0

	for _, component := range components {
		switch component.Status {
		case controlPlaneStatusMissing, controlPlaneStatusDegraded:
			return controlPlaneStatusDegraded
		case controlPlaneStatusRolling:
			hasRolling = true
			allReady = false
		case controlPlaneStatusReady:
		default:
			allReady = false
		}
	}

	if commonVersion == "" {
		if allReady {
			return controlPlaneStatusMixed
		}
		return controlPlaneStatusRolling
	}
	if hasRolling {
		return controlPlaneStatusRolling
	}
	if allReady {
		return controlPlaneStatusReady
	}
	return controlPlaneStatusDegraded
}

func controlPlanePodFromKube(component string, pod kubePodInfo) model.ControlPlanePod {
	image := readControlPlanePodImage(component, pod)
	imageRepository, imageTag := splitImageReference(image)
	startTime := pod.Status.StartTime
	return model.ControlPlanePod{
		Name:            strings.TrimSpace(pod.Metadata.Name),
		NodeName:        strings.TrimSpace(pod.Spec.NodeName),
		Phase:           strings.TrimSpace(pod.Status.Phase),
		Ready:           readControlPlanePodReady(component, pod),
		Image:           image,
		ImageRepository: imageRepository,
		ImageTag:        imageTag,
		StartTime:       startTime,
	}
}

func readControlPlanePodImage(component string, pod kubePodInfo) string {
	component = strings.TrimSpace(component)
	for _, container := range pod.Spec.Containers {
		if strings.EqualFold(strings.TrimSpace(container.Name), component) {
			return strings.TrimSpace(container.Image)
		}
	}
	if len(pod.Spec.Containers) == 0 {
		return ""
	}
	return strings.TrimSpace(pod.Spec.Containers[0].Image)
}

func readControlPlanePodReady(component string, pod kubePodInfo) bool {
	component = strings.TrimSpace(component)
	for _, status := range pod.Status.ContainerStatuses {
		if component == "" || strings.EqualFold(strings.TrimSpace(status.Name), component) {
			return status.Ready
		}
	}
	return false
}

func observedControlPlaneImageTags(pods []model.ControlPlanePod) []string {
	if len(pods) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(pods))
	out := make([]string, 0, len(pods))
	for _, pod := range pods {
		tag := strings.TrimSpace(pod.ImageTag)
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
	}
	sort.Strings(out)
	return out
}

func controlPlanePodTimestamp(pod model.ControlPlanePod) time.Time {
	if pod.StartTime != nil && !pod.StartTime.IsZero() {
		return pod.StartTime.UTC()
	}
	return time.Time{}
}

func (s *Server) readControlPlaneWorkflowRun(ctx context.Context) *model.ControlPlaneWorkflowRun {
	repository := strings.TrimSpace(s.controlPlaneGitHubRepository)
	if repository == "" {
		return nil
	}

	run := &model.ControlPlaneWorkflowRun{
		Repository: repository,
		Workflow:   defaultControlPlaneWorkflow(s.controlPlaneGitHubWorkflow),
		ObservedAt: time.Now().UTC(),
	}

	apiURL, err := s.controlPlaneWorkflowRunsURL(repository, run.Workflow)
	if err != nil {
		run.Status = "unavailable"
		run.Error = err.Error()
		return run
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		run.Status = "unavailable"
		run.Error = err.Error()
		return run
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "fugue-api")
	if token := strings.TrimSpace(s.controlPlaneGitHubToken); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := s.controlPlaneHTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		run.Status = "unavailable"
		run.Error = err.Error()
		return run
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		run.Status = "unavailable"
		run.Error = fmt.Sprintf("github actions api failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		return run
	}

	var payload githubWorkflowRunsResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		run.Status = "unavailable"
		run.Error = fmt.Sprintf("decode github actions response: %v", err)
		return run
	}
	if len(payload.WorkflowRuns) == 0 {
		run.Status = "unknown"
		run.Error = "no workflow runs found"
		return run
	}

	latest := payload.WorkflowRuns[0]
	run.Status = strings.TrimSpace(latest.Status)
	run.Conclusion = strings.TrimSpace(latest.Conclusion)
	run.RunNumber = latest.RunNumber
	run.Event = strings.TrimSpace(latest.Event)
	run.HeadBranch = strings.TrimSpace(latest.HeadBranch)
	run.HeadSHA = strings.TrimSpace(latest.HeadSHA)
	run.HTMLURL = strings.TrimSpace(latest.HTMLURL)
	run.CreatedAt = parseOptionalRFC3339(latest.CreatedAt)
	run.UpdatedAt = parseOptionalRFC3339(latest.UpdatedAt)
	return run
}

func (s *Server) controlPlaneWorkflowRunsURL(repository, workflow string) (string, error) {
	parts := strings.Split(strings.TrimSpace(repository), "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", fmt.Errorf("control plane github repository must be owner/repo")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(s.controlPlaneGitHubAPIURL), "/")
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	query := url.Values{}
	query.Set("per_page", "1")
	return fmt.Sprintf(
		"%s/repos/%s/%s/actions/workflows/%s/runs?%s",
		baseURL,
		url.PathEscape(parts[0]),
		url.PathEscape(parts[1]),
		url.PathEscape(strings.TrimSpace(workflow)),
		query.Encode(),
	), nil
}

func defaultControlPlaneWorkflow(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "deploy-control-plane.yml"
	}
	return value
}

func parseOptionalRFC3339(raw string) *time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil
	}
	copy := parsed.UTC()
	return &copy
}

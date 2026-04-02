package api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
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

	components := []model.ControlPlaneComponent{
		buildControlPlaneComponent(controlPlaneComponentAPI, apiDeployment),
		buildControlPlaneComponent(controlPlaneComponentController, controllerDeployment),
	}
	version := readCommonControlPlaneVersion(components)

	return model.ControlPlaneStatus{
		Namespace:       namespace,
		ReleaseInstance: releaseInstance,
		Version:         version,
		Status:          readControlPlaneStatus(components, version),
		ObservedAt:      time.Now().UTC(),
		Components:      components,
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

func buildControlPlaneComponent(component string, deployment *kubeDeployment) model.ControlPlaneComponent {
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

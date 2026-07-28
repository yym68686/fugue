package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
)

type agentPreflightRefusalError struct {
	cause error
}

func (e *agentPreflightRefusalError) Error() string {
	if e == nil || e.cause == nil {
		return "agent operation refused before apply"
	}
	return e.cause.Error()
}

func (e *agentPreflightRefusalError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func permanentAgentPreflightRefusal(err error) error {
	if err == nil {
		return nil
	}
	return &agentPreflightRefusalError{cause: err}
}

func isPermanentAgentPreflightRefusal(err error) bool {
	var refusal *agentPreflightRefusalError
	return errors.As(err, &refusal)
}

func (s *AgentService) prepareAgentTaskRollout(ctx context.Context, operationType string, current, desired model.App) (model.App, error) {
	if !s.Config.ApplyWithKubectl || (operationType != model.OperationTypeDeploy && operationType != model.OperationTypeMigrate && operationType != model.OperationTypeScale) {
		prepared, err := agentZeroDowntimeRolloutWithRenderer(operationType, current, desired, s.Renderer)
		return prepared, permanentAgentPreflightRefusal(err)
	}
	live, found, err := s.readAgentLiveDeployment(ctx, current)
	if err != nil {
		return model.App{}, err
	}
	if !found {
		if agentAppHasLiveServiceToProtect(current) {
			return model.App{}, permanentAgentPreflightRefusal(fmt.Errorf("zero-downtime %s refused: live deployment is missing for a previously serving app", strings.TrimSpace(operationType)))
		}
		prepared, err := agentZeroDowntimeRolloutWithRenderer(operationType, current, desired, s.Renderer)
		return prepared, permanentAgentPreflightRefusal(err)
	}

	liveReplicas := agentNestedInt(live, "spec", "replicas")
	if liveReplicas < 0 {
		liveReplicas = 1
	}
	if liveReplicas <= 0 &&
		agentNestedInt(live, "status", "replicas") <= 0 &&
		agentNestedInt(live, "status", "readyReplicas") <= 0 &&
		agentNestedInt(live, "status", "availableReplicas") <= 0 {
		stoppedCurrent := current
		stoppedCurrent.Status = model.AppStatus{Phase: "disabled"}
		stoppedCurrent.Spec.Replicas = 0
		prepared, err := agentZeroDowntimeRolloutWithRenderer(operationType, stoppedCurrent, desired, s.Renderer)
		return prepared, permanentAgentPreflightRefusal(err)
	}
	observedCurrent := current
	observedCurrent.Status.CurrentReplicas = maxAgentInt(1, agentNestedInt(live, "status", "readyReplicas"))
	observedCurrent.Status.CurrentRuntimeID = current.Spec.RuntimeID
	observedCurrent.Status.Phase = "deployed"
	liveKey := strings.TrimSpace(agentNestedString(live, "metadata", "annotations", FugueAnnotationReleaseKey))
	if liveKey == "" {
		return model.App{}, permanentAgentPreflightRefusal(fmt.Errorf("zero-downtime %s refused: live deployment has no %s identity annotation", strings.TrimSpace(operationType), FugueAnnotationReleaseKey))
	}
	desiredDeployment := agentRenderedDeployment(desired, s.Renderer)
	desiredKey := agentDeploymentReleaseKey(desiredDeployment)
	auxiliaryTemplateChanged := agentDeploymentAuxiliaryTemplateChanged(live, desiredDeployment)
	if desired.Spec.Replicas > 0 && model.AppHasClusterService(desired.Spec) && !agentDeploymentStrategyIsSafe(desiredDeployment) {
		return model.App{}, permanentAgentPreflightRefusal(fmt.Errorf("zero-downtime %s refused: rendered serving workload is not an online RollingUpdate", strings.TrimSpace(operationType)))
	}
	if operationType != model.OperationTypeScale && desired.Spec.Replicas > 0 && !model.AppHasClusterService(desired.Spec) {
		return model.App{}, permanentAgentPreflightRefusal(fmt.Errorf("zero-downtime %s refused: requested reconciliation removes the live cluster service", strings.TrimSpace(operationType)))
	}
	workloadChange := liveKey != desiredKey ||
		desired.Spec.Replicas != liveReplicas ||
		auxiliaryTemplateChanged ||
		!agentLiveStrategyMatchesDesired(live, desiredDeployment)
	if desired.Spec.Replicas > 0 && workloadChange {
		if !agentLiveDeploymentReady(live, liveReplicas) {
			return model.App{}, fmt.Errorf("zero-downtime %s refused: live deployment is not fully ready", strings.TrimSpace(operationType))
		}
		ready, err := s.readAgentLiveServiceEndpoint(ctx, observedCurrent)
		if err != nil {
			return model.App{}, err
		}
		if !ready {
			return model.App{}, fmt.Errorf("zero-downtime %s refused: live service has no ready endpoint", strings.TrimSpace(operationType))
		}
	}
	prepared, err := agentZeroDowntimeRolloutWithRendererAndObservedTemplate(
		operationType,
		observedCurrent,
		desired,
		s.Renderer,
		liveKey,
		auxiliaryTemplateChanged,
	)
	return prepared, permanentAgentPreflightRefusal(err)
}

func (s *AgentService) readAgentLiveDeployment(ctx context.Context, app model.App) (map[string]any, bool, error) {
	runner := s.CommandRunner
	if runner == nil {
		runner = defaultCommandRunner
	}
	readCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	output, err := runner(
		readCtx,
		"kubectl",
		"get", "deployment", RuntimeAppResourceName(app),
		"--namespace", NamespaceForTenant(app.TenantID),
		"--output", "json",
		"--ignore-not-found=true",
	)
	if err != nil {
		return nil, false, fmt.Errorf("read live deployment before zero-downtime apply: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if strings.TrimSpace(string(output)) == "" {
		return nil, false, nil
	}
	var deployment map[string]any
	if err := json.Unmarshal(output, &deployment); err != nil {
		return nil, false, fmt.Errorf("decode live deployment before zero-downtime apply: %w", err)
	}
	return deployment, true, nil
}

func (s *AgentService) readAgentLiveServiceEndpoint(ctx context.Context, app model.App) (bool, error) {
	runner := s.CommandRunner
	if runner == nil {
		runner = defaultCommandRunner
	}
	readCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	output, err := runner(
		readCtx,
		"kubectl",
		"get", "endpoints", RuntimeAppServiceName(app),
		"--namespace", NamespaceForTenant(app.TenantID),
		"--output", "json",
		"--ignore-not-found=true",
	)
	if err != nil {
		return false, fmt.Errorf("read live service endpoints before zero-downtime apply: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if strings.TrimSpace(string(output)) == "" {
		return false, nil
	}
	var endpoints struct {
		Subsets []struct {
			Addresses []struct {
				IP string `json:"ip"`
			} `json:"addresses"`
		} `json:"subsets"`
	}
	if err := json.Unmarshal(output, &endpoints); err != nil {
		return false, fmt.Errorf("decode live service endpoints before zero-downtime apply: %w", err)
	}
	for _, subset := range endpoints.Subsets {
		for _, address := range subset.Addresses {
			if strings.TrimSpace(address.IP) != "" {
				return true, nil
			}
		}
	}
	return false, nil
}

func agentLiveDeploymentReady(deployment map[string]any, desiredReplicas int) bool {
	if desiredReplicas <= 0 {
		return false
	}
	generation := agentNestedInt(deployment, "metadata", "generation")
	observedGeneration := agentNestedInt(deployment, "status", "observedGeneration")
	return observedGeneration >= generation &&
		agentNestedInt(deployment, "status", "updatedReplicas") >= desiredReplicas &&
		agentNestedInt(deployment, "status", "readyReplicas") >= desiredReplicas &&
		agentNestedInt(deployment, "status", "availableReplicas") >= desiredReplicas &&
		agentNestedInt(deployment, "status", "replicas") <= desiredReplicas &&
		agentNestedInt(deployment, "status", "unavailableReplicas") <= 0
}

func agentLiveStrategyMatchesDesired(live, desired map[string]any) bool {
	liveType := agentNestedString(live, "spec", "strategy", "type")
	desiredType := agentDeploymentStrategy(desired)
	if !strings.EqualFold(liveType, desiredType) {
		return false
	}
	if !strings.EqualFold(liveType, "RollingUpdate") {
		return true
	}
	return fmt.Sprint(agentNestedValue(live, "spec", "strategy", "rollingUpdate", "maxUnavailable")) == fmt.Sprint(agentNestedValue(desired, "spec", "strategy", "rollingUpdate", "maxUnavailable")) &&
		fmt.Sprint(agentNestedValue(live, "spec", "strategy", "rollingUpdate", "maxSurge")) == fmt.Sprint(agentNestedValue(desired, "spec", "strategy", "rollingUpdate", "maxSurge"))
}

type agentDeploymentAuxiliaryTemplateFingerprint struct {
	DrainAnnotations              map[string]string
	DrainAgent                    map[string]any
	ContainerLifecycles           map[string]any
	TerminationGracePeriodSeconds string
	ShareProcessNamespace         string
}

func agentDeploymentAuxiliaryTemplateChanged(live, desired map[string]any) bool {
	return !reflect.DeepEqual(
		agentDeploymentAuxiliaryTemplateFingerprintFor(live),
		agentDeploymentAuxiliaryTemplateFingerprintFor(desired),
	)
}

func agentDeploymentAuxiliaryTemplateFingerprintFor(deployment map[string]any) agentDeploymentAuxiliaryTemplateFingerprint {
	fingerprint := agentDeploymentAuxiliaryTemplateFingerprint{
		TerminationGracePeriodSeconds: agentAuxiliaryScalar(agentNestedValue(deployment, "spec", "template", "spec", "terminationGracePeriodSeconds")),
		ShareProcessNamespace:         agentAuxiliaryScalar(agentNestedValue(deployment, "spec", "template", "spec", "shareProcessNamespace")),
	}
	annotations, _ := agentNestedValue(deployment, "spec", "template", "metadata", "annotations").(map[string]any)
	if annotations == nil {
		if stringAnnotations, ok := agentNestedValue(deployment, "spec", "template", "metadata", "annotations").(map[string]string); ok {
			annotations = make(map[string]any, len(stringAnnotations))
			for key, value := range stringAnnotations {
				annotations[key] = value
			}
		}
	}
	for _, key := range []string{
		"fugue.io/drain-mode",
		"fugue.io/drain-timeout-seconds",
		"fugue.io/drain-quiet-period-seconds",
		"fugue.io/drain-agent-port",
		"fugue.io/termination-grace-min-seconds",
	} {
		if value := strings.TrimSpace(fmt.Sprint(annotations[key])); value != "" && value != "<nil>" {
			if fingerprint.DrainAnnotations == nil {
				fingerprint.DrainAnnotations = map[string]string{}
			}
			fingerprint.DrainAnnotations[key] = value
		}
	}
	for _, container := range agentObjectSlice(agentNestedValue(deployment, "spec", "template", "spec", "initContainers")) {
		if strings.TrimSpace(fmt.Sprint(container["name"])) != "fugue-drain-agent" {
			continue
		}
		fingerprint.DrainAgent = map[string]any{
			"name":          container["name"],
			"image":         container["image"],
			"resources":     container["resources"],
			"restartPolicy": container["restartPolicy"],
		}
		break
	}
	for _, container := range agentObjectSlice(agentNestedValue(deployment, "spec", "template", "spec", "containers")) {
		lifecycle, ok := container["lifecycle"]
		if !ok || lifecycle == nil {
			continue
		}
		if fingerprint.ContainerLifecycles == nil {
			fingerprint.ContainerLifecycles = map[string]any{}
		}
		fingerprint.ContainerLifecycles[strings.TrimSpace(fmt.Sprint(container["name"]))] = agentCanonicalAuxiliaryValue(lifecycle)
	}
	return fingerprint
}

func agentCanonicalAuxiliaryValue(value any) any {
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var normalized any
	if err := json.Unmarshal(data, &normalized); err != nil {
		return value
	}
	return normalized
}

func agentAuxiliaryScalar(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func agentObjectSlice(value any) []map[string]any {
	switch objects := value.(type) {
	case []map[string]any:
		return objects
	case []any:
		out := make([]map[string]any, 0, len(objects))
		for _, object := range objects {
			if values, ok := object.(map[string]any); ok {
				out = append(out, values)
			}
		}
		return out
	default:
		return nil
	}
}

func agentNestedValue(root map[string]any, path ...string) any {
	var current any = root
	for _, part := range path {
		values, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = values[part]
	}
	return current
}

func agentNestedString(root map[string]any, path ...string) string {
	value, _ := agentNestedValue(root, path...).(string)
	return strings.TrimSpace(value)
}

func agentNestedInt(root map[string]any, path ...string) int {
	switch value := agentNestedValue(root, path...).(type) {
	case int:
		return value
	case int32:
		return int(value)
	case int64:
		return int(value)
	case float64:
		return int(value)
	case json.Number:
		parsed, err := value.Int64()
		if err == nil {
			return int(parsed)
		}
	}
	return -1
}

func maxAgentInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

// PrepareAgentOperationApp is the control-plane preflight and the agent's
// local second line of defence. It returns the transiently prepared desired
// app (including an online rollout intent when one is validated).
func PrepareAgentOperationApp(op model.Operation, current model.App) (model.App, error) {
	return prepareAgentOperationAppWithRenderer(op, current, Renderer{})
}

func prepareAgentOperationAppWithObservedRelease(op model.Operation, current model.App, renderer Renderer, observedReleaseKey string) (model.App, error) {
	desired := current
	switch op.Type {
	case model.OperationTypeDeploy:
		if op.DesiredSpec == nil {
			return model.App{}, fmt.Errorf("deploy task missing desired spec")
		}
		desired.Spec = *op.DesiredSpec
	case model.OperationTypeScale:
		if op.DesiredReplicas == nil {
			return model.App{}, fmt.Errorf("scale task missing desired replicas")
		}
		desired.Spec.Replicas = *op.DesiredReplicas
	case model.OperationTypeMigrate:
		if op.DesiredSpec != nil {
			desired.Spec = *op.DesiredSpec
		} else if strings.TrimSpace(op.TargetRuntimeID) != "" {
			desired.Spec.RuntimeID = op.TargetRuntimeID
		} else {
			return model.App{}, fmt.Errorf("migrate task missing target runtime")
		}
	default:
		return desired, nil
	}
	return agentZeroDowntimeRolloutWithRendererAndObserved(op.Type, current, desired, renderer, observedReleaseKey)
}

func prepareAgentOperationAppWithRenderer(op model.Operation, current model.App, renderer Renderer) (model.App, error) {
	desired := current
	switch op.Type {
	case model.OperationTypeDeploy:
		if op.DesiredSpec == nil {
			return model.App{}, fmt.Errorf("deploy task missing desired spec")
		}
		desired.Spec = *op.DesiredSpec
	case model.OperationTypeScale:
		if op.DesiredReplicas == nil {
			return model.App{}, fmt.Errorf("scale task missing desired replicas")
		}
		desired.Spec.Replicas = *op.DesiredReplicas
	case model.OperationTypeMigrate:
		if op.DesiredSpec != nil {
			desired.Spec = *op.DesiredSpec
		} else if strings.TrimSpace(op.TargetRuntimeID) != "" {
			desired.Spec.RuntimeID = op.TargetRuntimeID
		} else {
			return model.App{}, fmt.Errorf("migrate task missing target runtime")
		}
	default:
		return desired, nil
	}
	return agentZeroDowntimeRolloutWithRenderer(op.Type, current, desired, renderer)
}

// agentZeroDowntimeRollout prepares a task for the external-runtime path. The
// controller has a richer Kubernetes preflight, but an external agent must
// still refuse a serving workload change that would require a destructive
// storage rollout. Returning the prepared app keeps the transient rollout
// intent out of the persisted AppSpec while ensuring the rendered manifest has
// the online drain contract.
func agentZeroDowntimeRollout(operationType string, current, desired model.App) (model.App, error) {
	return agentZeroDowntimeRolloutWithRenderer(operationType, current, desired, Renderer{})
}

func agentZeroDowntimeRolloutWithRenderer(operationType string, current, desired model.App, renderer Renderer) (model.App, error) {
	return agentZeroDowntimeRolloutWithRendererAndObserved(operationType, current, desired, renderer, "")
}

func agentZeroDowntimeRolloutWithRendererAndObserved(operationType string, current, desired model.App, renderer Renderer, observedReleaseKey string) (model.App, error) {
	return agentZeroDowntimeRolloutWithRendererAndObservedTemplate(operationType, current, desired, renderer, observedReleaseKey, false)
}

func agentZeroDowntimeRolloutWithRendererAndObservedTemplate(
	operationType string,
	current, desired model.App,
	renderer Renderer,
	observedReleaseKey string,
	auxiliaryTemplateChanged bool,
) (model.App, error) {
	if operationType != model.OperationTypeDeploy &&
		operationType != model.OperationTypeMigrate &&
		operationType != model.OperationTypeScale {
		return desired, nil
	}

	serving := agentAppHasLiveServiceToProtect(current)
	if operationType == model.OperationTypeScale && desired.Spec.Replicas <= 0 {
		return desired, nil
	}
	if desired.Spec.Replicas > 1 && !agentAppSupportsConcurrentStorage(desired.Spec) {
		return model.App{}, fmt.Errorf("zero-downtime %s refused: %s", strings.TrimSpace(operationType), agentStorageTopologyReason(desired.Spec))
	}
	if !serving {
		return desired, nil
	}
	if desired.Spec.Replicas <= 0 || !model.AppHasClusterService(desired.Spec) {
		return model.App{}, fmt.Errorf("zero-downtime %s refused: stop the service explicitly before removing its serving workload", strings.TrimSpace(operationType))
	}
	if operationType == model.OperationTypeMigrate {
		return model.App{}, fmt.Errorf("zero-downtime migrate refused: an external runtime handoff has no validated online plan; stop the service first")
	}

	// Scaling changes replica count but not the pod template. A scale-up of an
	// unsupported single-writer volume can nevertheless create a second pod and
	// contend for the claim, so fail closed when the requested count exceeds one.
	if operationType == model.OperationTypeScale && !auxiliaryTemplateChanged {
		return desired, nil
	}

	currentDeployment := agentRenderedDeployment(current, renderer)
	desiredDeployment := agentRenderedDeployment(desired, renderer)
	currentKey := agentDeploymentReleaseKey(currentDeployment)
	desiredKey := agentDeploymentReleaseKey(desiredDeployment)
	if observed := strings.TrimSpace(observedReleaseKey); observed != "" {
		if observed != currentKey && observed != desiredKey {
			return model.App{}, fmt.Errorf("zero-downtime deploy refused: live deployment release identity matches neither current nor desired snapshot")
		}
		if observed == desiredKey && !auxiliaryTemplateChanged {
			if !agentDeploymentStrategyIsSafe(desiredDeployment) {
				return model.App{}, fmt.Errorf("zero-downtime deploy refused: rendered serving workload is not an online RollingUpdate")
			}
			return desired, nil
		}
	} else if !auxiliaryTemplateChanged && reflect.DeepEqual(agentDeploymentTemplate(currentDeployment), agentDeploymentTemplate(desiredDeployment)) {
		// A policy/strategy-only reconciliation does not replace a pod.
		return desired, nil
	}

	if !agentAppSupportsConcurrentStorage(desired.Spec) {
		return model.App{}, fmt.Errorf("zero-downtime deploy refused: %s", agentStorageTopologyReason(desired.Spec))
	}
	if strings.TrimSpace(desired.Spec.RolloutIntent) == "" {
		desired.Spec.RolloutIntent = model.AppRolloutIntentOnlineRestart
	}
	desiredDeployment = agentRenderedDeployment(desired, renderer)
	annotations := agentDeploymentAnnotations(desiredDeployment)
	if !agentDeploymentStrategyIsSafe(desiredDeployment) ||
		strings.EqualFold(annotations["fugue.io/downtime-class"], "downtime-required") ||
		strings.EqualFold(annotations["fugue.io/rollout-mode"], "isolated-singleton") {
		return model.App{}, fmt.Errorf("zero-downtime deploy refused: rendered serving workload is not an online RollingUpdate")
	}
	return desired, nil
}

func agentDeploymentStrategyIsSafe(deployment map[string]any) bool {
	if !strings.EqualFold(agentDeploymentStrategy(deployment), "RollingUpdate") {
		return false
	}
	spec, _ := deployment["spec"].(map[string]any)
	strategy, _ := spec["strategy"].(map[string]any)
	rolling, _ := strategy["rollingUpdate"].(map[string]any)
	if fmt.Sprint(rolling["maxUnavailable"]) != "0" {
		return false
	}
	surge := fmt.Sprint(rolling["maxSurge"])
	return surge != "" && surge != "0" && surge != "<nil>"
}

func agentAppHasLiveServiceToProtect(app model.App) bool {
	if app.Spec.Replicas <= 0 || !model.AppHasClusterService(app.Spec) {
		return false
	}
	if app.Status.CurrentReplicas > 0 ||
		strings.TrimSpace(app.Status.CurrentRuntimeID) != "" ||
		app.Status.CurrentReleaseStartedAt != nil ||
		app.Status.CurrentReleaseReadyAt != nil {
		return true
	}
	// A non-initial lifecycle phase is durable evidence that the app existed
	// before this task even when an agent received a stale status projection.
	switch strings.ToLower(strings.TrimSpace(app.Status.Phase)) {
	case "", "created", "importing", "pending", "deploying", "scaling", "migrating", "failing-over", "disabling", "disabled", "deleting", "deleted", "failed", "error":
		return false
	default:
		return true
	}
}

func agentAppSupportsConcurrentStorage(spec model.AppSpec) bool {
	if spec.Workspace != nil {
		// The external agent renderer has no live-pod node discovery or pinning;
		// never claim a same-node RWO rollout is safe without that proof.
		return false
	}
	if spec.PersistentStorage == nil {
		return true
	}
	return model.AppPersistentStorageSpecUsesSharedProjectRWX(spec.PersistentStorage)
}

func agentStorageTopologyReason(spec model.AppSpec) string {
	if spec.Workspace != nil {
		return model.AppStorageClassSameNodeOnlineMountUnsupportedSummary(spec.Workspace.StorageClassName)
	}
	if spec.PersistentStorage != nil {
		return model.AppStorageClassSameNodeOnlineMountUnsupportedSummary(spec.PersistentStorage.StorageClassName)
	}
	return "the service topology cannot run an old and replacement pod concurrently"
}

func agentRenderedDeployment(app model.App, renderer Renderer) map[string]any {
	app = renderer.PrepareApp(app)
	for _, object := range buildAppObjectsWithPlacementsAndOptions(app, SchedulingConstraints{}, nil, renderer.renderOptions()) {
		if kind, _ := object["kind"].(string); kind == "Deployment" {
			return object
		}
	}
	return nil
}

func agentDeploymentTemplate(deployment map[string]any) any {
	if deployment == nil {
		return nil
	}
	spec, _ := deployment["spec"].(map[string]any)
	return spec["template"]
}

func agentDeploymentStrategy(deployment map[string]any) string {
	if deployment == nil {
		return ""
	}
	spec, _ := deployment["spec"].(map[string]any)
	strategy, _ := spec["strategy"].(map[string]any)
	value, _ := strategy["type"].(string)
	return strings.TrimSpace(value)
}

func agentDeploymentAnnotations(deployment map[string]any) map[string]string {
	if deployment == nil {
		return nil
	}
	metadata, _ := deployment["metadata"].(map[string]any)
	switch annotations := metadata["annotations"].(type) {
	case map[string]string:
		return annotations
	case map[string]any:
		out := make(map[string]string, len(annotations))
		for key, value := range annotations {
			if text, ok := value.(string); ok {
				out[key] = text
			}
		}
		return out
	default:
		return nil
	}
}

func agentDeploymentReleaseKey(deployment map[string]any) string {
	return strings.TrimSpace(agentDeploymentAnnotations(deployment)[FugueAnnotationReleaseKey])
}

package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type zeroDowntimeRolloutGuardDecision struct {
	Refused                  bool
	Reason                   string
	PolicyMode               string
	RequirementSource        string
	RolloutIntent            string
	Strategy                 string
	DowntimeClass            string
	RolloutMode              string
	PreviousReleaseKey       string
	DesiredReleaseKey        string
	PodTemplateChanged       bool
	AuxiliaryTemplateChanged bool
}

func (s *Service) refuseNonZeroDowntimeRolloutIfNeeded(
	ctx context.Context,
	op model.Operation,
	currentApp, desiredApp model.App,
	scheduling runtime.SchedulingConstraints,
) error {
	return s.refuseNonZeroDowntimeRolloutWithScheduling(ctx, op, currentApp, desiredApp, scheduling, scheduling, "")
}

func (s *Service) refuseNonZeroDowntimeRolloutWithScheduling(
	ctx context.Context,
	op model.Operation,
	currentApp, desiredApp model.App,
	currentScheduling, desiredScheduling runtime.SchedulingConstraints,
	observedReleaseKey string,
) error {
	return s.refuseNonZeroDowntimeRolloutWithSchedulingAndTemplateEvidence(
		ctx,
		op,
		currentApp,
		desiredApp,
		currentScheduling,
		desiredScheduling,
		observedReleaseKey,
		false,
	)
}

func (s *Service) refuseNonZeroDowntimeRolloutWithSchedulingAndTemplateEvidence(
	ctx context.Context,
	op model.Operation,
	currentApp, desiredApp model.App,
	currentScheduling, desiredScheduling runtime.SchedulingConstraints,
	observedReleaseKey string,
	auxiliaryTemplateChanged bool,
) error {
	decision := s.zeroDowntimeRolloutGuardDecisionWithSchedulingAndTemplateEvidence(
		op,
		currentApp,
		desiredApp,
		currentScheduling,
		desiredScheduling,
		observedReleaseKey,
		auxiliaryTemplateChanged,
	)
	if !decision.Refused {
		return nil
	}

	message := fmt.Sprintf("zero-downtime %s refused: %s", strings.TrimSpace(op.Type), decision.Reason)
	attrs := map[string]any{
		"decision":                   "zero_downtime_required",
		"operation_id":               strings.TrimSpace(op.ID),
		"operation_type":             strings.TrimSpace(op.Type),
		"policy_mode":                decision.PolicyMode,
		"requirement_source":         decision.RequirementSource,
		"rollout_intent":             decision.RolloutIntent,
		"strategy":                   decision.Strategy,
		"downtime_class":             decision.DowntimeClass,
		"rollout_mode":               decision.RolloutMode,
		"reason":                     decision.Reason,
		"previous_release_key":       decision.PreviousReleaseKey,
		"desired_release_key":        decision.DesiredReleaseKey,
		"pod_template_changed":       decision.PodTemplateChanged,
		"auxiliary_template_changed": decision.AuxiliaryTemplateChanged,
		"cluster_service":            model.AppHasClusterService(desiredApp.Spec),
		"desired_replicas":           desiredApp.Spec.Replicas,
		"storage_class":              zeroDowntimeAppStorageClass(desiredApp.Spec),
		"storage_mode":               zeroDowntimeAppStorageMode(desiredApp.Spec),
	}
	s.logOperationAppEvent("blocked", "warning", op, desiredApp, message, attrs)
	s.logControllerAppEvent(ctx, "zero_downtime_rollout_decision", "warning", desiredApp, message, attrs)
	return fmt.Errorf("%s", message)
}

// prepareManagedAppReconcileRollout applies the same fail-closed contract to
// a background ManagedApp reconciliation. Reconciliation normally has no
// Operation object to guard, yet a stored desired snapshot can still differ
// from the live serving snapshot. Derive the transient online intent before
// any Kubernetes object is written and refuse unsupported durable topologies.
func (s *Service) prepareManagedAppReconcileRollout(
	ctx context.Context,
	managed runtime.ManagedAppObject,
	desired model.App,
	scheduling runtime.SchedulingConstraints,
) (model.App, error) {
	return s.prepareManagedAppReconcileRolloutWithEvidence(ctx, nil, "", managed, desired, "", scheduling)
}

func (s *Service) prepareManagedAppReconcileRolloutWithEvidence(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	managed runtime.ManagedAppObject,
	desired model.App,
	operationType string,
	desiredScheduling runtime.SchedulingConstraints,
) (model.App, error) {
	current := runtime.AppFromManagedApp(managed)
	// ManagedApp is a runtime-fact projection and carries the image/build
	// source that produced the live workload, but it predates the split between
	// immutable origin intent and the latest build artifact. Rehydrate that
	// missing origin dimension from durable intent before classifying the live
	// transition. Otherwise a resource-only repair after a failed rollout is
	// misclassified as an unplanned restart and permanently blocked.
	backfillManagedAppSource(&current, desired)
	if client != nil {
		return s.prepareManagedAppRolloutFromLiveState(ctx, client, namespace, managed, current, desired, operationType, desiredScheduling)
	}
	return s.prepareManagedAppRolloutFromStatus(ctx, managed, current, desired, operationType, desiredScheduling)
}

func (s *Service) prepareManagedAppRolloutFromStatus(
	ctx context.Context,
	managed runtime.ManagedAppObject,
	current, desired model.App,
	operationType string,
	desiredScheduling runtime.SchedulingConstraints,
) (model.App, error) {
	if !managedAppStatusShowsServing(managed.Status) {
		return desired, nil
	}
	current.Status.CurrentReplicas = managed.Status.ReadyReplicas
	current.Status.CurrentRuntimeID = current.Spec.RuntimeID
	current.Status.Phase = "deployed"
	desired.Spec.RolloutIntent = s.managedRolloutIntentForDesiredState(current, desired, managed.Spec.Scheduling, desiredScheduling)
	opType := strings.TrimSpace(operationType)
	if opType == "" {
		opType = inferredManagedReconcileOperationType(current, desired)
	}
	opType = managedRolloutGuardOperationType(opType)
	op := model.Operation{Type: opType, DesiredSpec: &desired.Spec}
	if err := s.refuseNonZeroDowntimeRolloutWithScheduling(ctx, op, current, desired, managed.Spec.Scheduling, desiredScheduling, ""); err != nil {
		return model.App{}, err
	}
	return desired, nil
}

func inferredManagedReconcileOperationType(current, desired model.App) string {
	if desired.Spec.Replicas != current.Spec.Replicas && managedAppSpecsEqualExceptReplicas(current.Spec, desired.Spec) {
		return model.OperationTypeScale
	}
	return model.OperationTypeDeploy
}

func managedAppSpecsEqualExceptReplicas(current, desired model.AppSpec) bool {
	current.Replicas = 0
	desired.Replicas = 0
	current.RolloutIntent = ""
	desired.RolloutIntent = ""
	return reflect.DeepEqual(current, desired)
}

func managedRolloutIntentForDesiredState(
	current, desired model.App,
	currentScheduling, desiredScheduling runtime.SchedulingConstraints,
) string {
	if intent := strings.TrimSpace(rolloutIntentForManagedDesiredState(current, desired)); intent != "" {
		return intent
	}
	if intent := strings.TrimSpace(desired.Spec.RolloutIntent); intent != "" {
		return intent
	}
	// RolloutIntent is transient reconciliation safety state, not a user-facing
	// switch. Preserve the live snapshot's validated intent when the durable
	// desired state and scheduling are otherwise unchanged apart from replica
	// count. Clearing it here changes drain lifecycle fields without changing
	// the release key; reusing it for a different desired state would weaken the
	// online-plan guard.
	if !managedAppDesiredStateEqualExceptReplicasAndRolloutIntent(current, desired) ||
		!managedSchedulingConstraintsEqual(currentScheduling, desiredScheduling) {
		return ""
	}
	return strings.TrimSpace(current.Spec.RolloutIntent)
}

// managedRolloutIntentForDesiredState extends the ordinary AppSpec classifier
// with deployment-level evidence for managed PostgreSQL state. Older
// ManagedApp snapshots can contain mixed-case PostgreSQL service names even
// after the durable store has been repaired. Canonicalizing that generated
// hostname changes the app container environment and therefore needs an
// explicit online rollout intent. Database placement fields, on the other
// hand, do not change the app Deployment; while a database transition is in
// progress, retain an already validated intent when the rendered workload is
// otherwise identical.
func (s *Service) managedRolloutIntentForDesiredState(
	current, desired model.App,
	currentScheduling, desiredScheduling runtime.SchedulingConstraints,
) string {
	if intent := managedRolloutIntentForDesiredState(current, desired, currentScheduling, desiredScheduling); intent != "" {
		return intent
	}
	if s == nil || !appSupportsOnlineRolloutIntent(desired) ||
		!managedSchedulingConstraintsEqual(currentScheduling, desiredScheduling) {
		return ""
	}

	currentWithoutIntent := managedAppWithoutRolloutIntent(current)
	desiredWithoutIntent := managedAppWithoutRolloutIntent(desired)
	canonicalCurrent, currentChanged := normalizeManagedPostgresDeploymentNames(currentWithoutIntent)
	canonicalDesired, desiredChanged := normalizeManagedPostgresDeploymentNames(desiredWithoutIntent)
	if !managedAppNonPostgresWorkloadInputsEqual(canonicalCurrent, canonicalDesired) {
		return ""
	}

	canonicalCurrentKey := strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(canonicalCurrent), currentScheduling))
	canonicalDesiredKey := strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(canonicalDesired), desiredScheduling))
	if canonicalCurrentKey == "" || canonicalCurrentKey != canonicalDesiredKey {
		return ""
	}

	currentKey := strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(currentWithoutIntent), currentScheduling))
	desiredKey := strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(desiredWithoutIntent), desiredScheduling))
	if currentChanged && !desiredChanged && currentKey != desiredKey {
		return model.AppRolloutIntentOnlineEnvironmentUpdate
	}
	if appHasOnlineRolloutIntent(current) && currentKey == desiredKey {
		return strings.TrimSpace(current.Spec.RolloutIntent)
	}
	return ""
}

func managedAppWithoutRolloutIntent(app model.App) model.App {
	out := app
	if spec := cloneControllerAppSpec(&app.Spec); spec != nil {
		spec.RolloutIntent = ""
		out.Spec = *spec
	}
	return out
}

func managedAppNonPostgresWorkloadInputsEqual(current, desired model.App) bool {
	if !reflect.DeepEqual(comparableManagedAppDeploymentSpec(current.Spec), comparableManagedAppDeploymentSpec(desired.Spec)) {
		return false
	}
	if !reflect.DeepEqual(model.AppOriginSource(current), model.AppOriginSource(desired)) ||
		!reflect.DeepEqual(model.AppBuildSource(current), model.AppBuildSource(desired)) {
		return false
	}
	if !reflect.DeepEqual(current.Route, desired.Route) ||
		!reflect.DeepEqual(comparableManagedAppServiceBindings(current.Bindings), comparableManagedAppServiceBindings(desired.Bindings)) {
		return false
	}
	return reflect.DeepEqual(
		managedBackingServiceDeploymentInputs(current.BackingServices),
		managedBackingServiceDeploymentInputs(desired.BackingServices),
	)
}

func comparableManagedAppDeploymentSpec(spec model.AppSpec) model.AppSpec {
	normalized := comparableRestartSpec(spec)
	if normalized.Postgres != nil {
		normalized.Postgres = managedPostgresDeploymentConnectionInput(normalized.Postgres)
	}
	return normalized
}

type managedBackingServiceDeploymentInput struct {
	ID          string
	TenantID    string
	ProjectID   string
	OwnerAppID  string
	Name        string
	Type        string
	Provisioner string
	Postgres    *model.AppPostgresSpec
}

func managedBackingServiceDeploymentInputs(services []model.BackingService) []managedBackingServiceDeploymentInput {
	if len(services) == 0 {
		return nil
	}
	out := make([]managedBackingServiceDeploymentInput, len(services))
	for index, service := range services {
		out[index] = managedBackingServiceDeploymentInput{
			ID:          strings.TrimSpace(service.ID),
			TenantID:    strings.TrimSpace(service.TenantID),
			ProjectID:   strings.TrimSpace(service.ProjectID),
			OwnerAppID:  strings.TrimSpace(service.OwnerAppID),
			Name:        strings.TrimSpace(service.Name),
			Type:        strings.TrimSpace(service.Type),
			Provisioner: strings.TrimSpace(service.Provisioner),
			Postgres:    managedPostgresDeploymentConnectionInput(service.Spec.Postgres),
		}
	}
	return out
}

func managedPostgresDeploymentConnectionInput(postgres *model.AppPostgresSpec) *model.AppPostgresSpec {
	if postgres == nil {
		return nil
	}
	return &model.AppPostgresSpec{
		Database:    strings.TrimSpace(postgres.Database),
		User:        strings.TrimSpace(postgres.User),
		Password:    postgres.Password,
		ServiceName: strings.TrimSpace(postgres.ServiceName),
	}
}

func normalizeManagedPostgresDeploymentNames(app model.App) (model.App, bool) {
	out := app
	out.BackingServices = cloneControllerBackingServices(app.BackingServices)
	out.Bindings = cloneControllerServiceBindings(app.Bindings)
	changed := false

	if out.Spec.Postgres != nil {
		postgres := *model.CloneAppPostgresSpec(out.Spec.Postgres)
		canonical := model.NormalizePostgresServiceName(postgres.ServiceName, "")
		if canonical != "" && canonical != postgres.ServiceName {
			postgres.ServiceName = canonical
			out.Spec.Postgres = &postgres
			changed = true
		}
	}

	type postgresName struct {
		oldName string
		newName string
	}
	serviceNames := make(map[string]postgresName)
	for index := range out.BackingServices {
		service := &out.BackingServices[index]
		if !strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) ||
			!strings.EqualFold(strings.TrimSpace(service.Provisioner), model.BackingServiceProvisionerManaged) ||
			service.Spec.Postgres == nil {
			continue
		}
		postgres := *model.CloneAppPostgresSpec(service.Spec.Postgres)
		oldName := strings.TrimSpace(postgres.ServiceName)
		if oldName == "" {
			continue
		}
		newName := model.NormalizePostgresServiceName(oldName, "")
		serviceNames[strings.TrimSpace(service.ID)] = postgresName{oldName: oldName, newName: newName}
		if newName != postgres.ServiceName {
			postgres.ServiceName = newName
			service.Spec.Postgres = &postgres
			changed = true
		}
	}

	for index := range out.Bindings {
		binding := &out.Bindings[index]
		names, ok := serviceNames[strings.TrimSpace(binding.ServiceID)]
		if !ok || len(binding.Env) == 0 {
			continue
		}
		host := strings.TrimSpace(binding.Env["DB_HOST"])
		nextHost := ""
		switch {
		case strings.EqualFold(host, names.oldName):
			nextHost = names.newName
		case strings.EqualFold(host, names.oldName+"-rw"):
			nextHost = model.PostgresRWServiceName(names.newName)
		default:
			continue
		}
		if nextHost != "" && nextHost != binding.Env["DB_HOST"] {
			binding.Env["DB_HOST"] = nextHost
			changed = true
		}
	}
	return out, changed
}

func managedAppDesiredStateEqualExceptReplicasAndRolloutIntent(current, desired model.App) bool {
	currentSnapshot := comparableManagedAppRolloutSnapshot(current)
	desiredSnapshot := comparableManagedAppRolloutSnapshot(desired)
	currentSnapshot.Spec.Replicas = 0
	desiredSnapshot.Spec.Replicas = 0
	return reflect.DeepEqual(currentSnapshot, desiredSnapshot) &&
		reflect.DeepEqual(model.AppOriginSource(current), model.AppOriginSource(desired)) &&
		reflect.DeepEqual(model.AppBuildSource(current), model.AppBuildSource(desired))
}

func managedSchedulingConstraintsEqual(current, desired runtime.SchedulingConstraints) bool {
	if len(current.NodeSelector) == 0 {
		current.NodeSelector = nil
	}
	if len(desired.NodeSelector) == 0 {
		desired.NodeSelector = nil
	}
	if len(current.Tolerations) == 0 {
		current.Tolerations = nil
	}
	if len(desired.Tolerations) == 0 {
		desired.Tolerations = nil
	}
	return reflect.DeepEqual(current, desired)
}

func managedRolloutGuardOperationType(operationType string) string {
	switch strings.TrimSpace(operationType) {
	case model.OperationTypeScale:
		return model.OperationTypeScale
	case model.OperationTypeMigrate:
		return model.OperationTypeMigrate
	default:
		return model.OperationTypeDeploy
	}
}

func (s *Service) prepareManagedAppRolloutFromLiveState(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	managed runtime.ManagedAppObject,
	current, desired model.App,
	operationType string,
	desiredScheduling runtime.SchedulingConstraints,
) (model.App, error) {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = runtime.NamespaceForTenant(current.TenantID)
	}
	deployment, found, err := client.getDeployment(ctx, namespace, runtime.RuntimeAppResourceName(current))
	if err != nil {
		return model.App{}, fmt.Errorf("read live deployment before zero-downtime reconcile: %w", err)
	}
	statusServing := managedAppStatusShowsServing(managed.Status)
	if !found {
		if statusServing {
			return model.App{}, fmt.Errorf("live deployment is missing while managed app status records a serving release")
		}
		return desired, nil
	}

	liveReplicas := 1
	if deployment.Spec.Replicas != nil {
		liveReplicas = *deployment.Spec.Replicas
	}
	liveWorkload := liveReplicas > 0 || deployment.Status.Replicas > 0 || deployment.Status.ReadyReplicas > 0 || statusServing
	if !liveWorkload {
		return desired, nil
	}
	liveKey := strings.TrimSpace(deployment.Metadata.Annotations[runtime.FugueAnnotationReleaseKey])
	if liveKey == "" {
		return model.App{}, fmt.Errorf("live deployment has no %s identity annotation", runtime.FugueAnnotationReleaseKey)
	}

	currentScheduling := managed.Spec.Scheduling
	currentKey := strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(current), currentScheduling))
	desiredKey := strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(desired), desiredScheduling))
	servingLKGKey := strings.TrimSpace(managed.Status.CurrentReleaseKey)
	var recoveredOperation *model.Operation
	if liveKey != currentKey && liveKey != desiredKey {
		recovered, ok, err := s.recoverManagedAppPendingDeploySnapshot(ctx, managed, current, liveKey)
		if err != nil {
			return model.App{}, err
		}
		if ok {
			current = recovered.App
			currentScheduling = recovered.Scheduling
			recoveredOperation = &recovered.Operation
		}
	}
	if current.Spec.Replicas <= 0 {
		current.Spec.Replicas = liveReplicas
	}
	current.Status.CurrentReplicas = deployment.Status.ReadyReplicas
	if current.Status.CurrentReplicas <= 0 {
		current.Status.CurrentReplicas = maxInt(deployment.Status.AvailableReplicas, deployment.Status.Replicas)
	}
	current.Status.CurrentRuntimeID = current.Spec.RuntimeID
	current.Status.Phase = "deployed"
	desired.Status = current.Status
	desired.Spec.RolloutIntent = s.managedRolloutIntentForDesiredState(current, desired, currentScheduling, desiredScheduling)
	if recoveredOperation != nil && strings.TrimSpace(desired.Spec.RolloutIntent) == "" {
		recoveryRollout := *recoveredOperation
		recoveryRollout.DesiredSpec = &desired.Spec
		desired.Spec.RolloutIntent = rolloutIntentForManagedOperation(recoveryRollout, current, desired)
	}
	opType := strings.TrimSpace(operationType)
	if opType == "" {
		opType = inferredManagedReconcileOperationType(current, desired)
	}
	opType = managedRolloutGuardOperationType(opType)
	op := model.Operation{Type: opType, DesiredSpec: &desired.Spec}
	if recoveredOperation != nil {
		op.RequestedByType = recoveredOperation.RequestedByType
		op.RequestedByID = recoveredOperation.RequestedByID
	}

	currentKey = strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(current), currentScheduling))
	desiredKey = strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(desired), desiredScheduling))
	if liveKey != currentKey && liveKey != desiredKey {
		return model.App{}, fmt.Errorf("live deployment release key %q matches neither current snapshot %q nor desired snapshot %q", liveKey, currentKey, desiredKey)
	}

	desiredDeployment := s.zeroDowntimeManagedAppDeployment(desired, desiredScheduling)
	zeroDowntimeRequired := zeroDowntimeRequirementSourceForOperation(current, desired) != ""
	if zeroDowntimeRequired && desired.Spec.Replicas > 0 && model.AppHasClusterService(desired.Spec) && !zeroDowntimeDeploymentStrategyIsSafe(desiredDeployment) {
		return model.App{}, fmt.Errorf("rendered serving workload is not an explicit safe RollingUpdate")
	}
	expectedDeployment, expectedDeploymentFound := s.expectedManagedAppDeployment(desired, desiredScheduling)
	if desired.Spec.Replicas > 0 && !expectedDeploymentFound {
		return model.App{}, fmt.Errorf("rendered serving workload has no deployment for live template preflight")
	}
	auxiliaryTemplateChanged := expectedDeploymentFound && managedDeploymentAuxiliaryTemplateChanged(deployment, expectedDeployment)
	// Drain helpers, termination grace, and process namespace are intentionally
	// excluded from the release key. A stale helper template from a previous
	// controller version must not turn an otherwise identical serving release
	// into an online replacement request. Only an explicit online/continuity
	// rollout may require those auxiliary fields to match before replacing a
	// workload; the reconciler preserves the live template for the no-op case.
	guardAuxiliaryTemplateChanged := auxiliaryTemplateChanged && managedAppAuxiliaryTemplateChangeRequiresGuard(desired)
	desiredReplicas := desired.Spec.Replicas
	workloadChange := liveKey != desiredKey ||
		!liveDeploymentStrategyMatchesDesired(deployment, desiredDeployment) ||
		guardAuxiliaryTemplateChanged ||
		desiredReplicas != liveReplicas
	unavailableRecovery := false
	readyDesiredCohortRecovery := false
	if desiredReplicas > 0 && workloadChange {
		readyEndpoint, err := liveManagedAppHasReadyEndpoint(ctx, client, namespace, current)
		if err != nil {
			return model.App{}, err
		}
		deploymentReady := managedDeploymentStatusReady(deployment, liveReplicas)
		failedCandidateRecovery := false
		if !deploymentReady && liveKey != desiredKey && readyEndpoint && model.AppHasClusterService(desired.Spec) {
			readyDesiredCohortRecovery, err = s.managedAppReadyReleaseCohort(
				ctx, client, namespace, desired, desiredScheduling, desiredReplicas,
			)
			if err != nil {
				return model.App{}, err
			}
			if !readyDesiredCohortRecovery && recoveredOperation != nil && servingLKGKey != "" &&
				servingLKGKey != liveKey && servingLKGKey != desiredKey &&
				strings.TrimSpace(managed.Status.CurrentReleaseReadyAt) != "" {
				readyLKG, readyErr := s.managedAppReadyReleaseKeyCohort(
					ctx, client, namespace, desired, servingLKGKey, desiredReplicas,
				)
				if readyErr != nil {
					return model.App{}, readyErr
				}
				failedCandidateRecovery = managedAppAllowsFailedCandidateReplacement(
					deployment, current.Spec, desired.Spec, readyEndpoint, readyLKG, desiredReplicas,
				)
			}
		}
		if !readyDesiredCohortRecovery {
			unavailableRecovery = managedAppAllowsUnavailableRecovery(
				deployment,
				current.Spec,
				desired.Spec,
				readyEndpoint,
			)
			if !deploymentReady && !unavailableRecovery && !failedCandidateRecovery {
				return model.App{}, fmt.Errorf("live deployment is not fully ready before an online replacement")
			}
			if !readyEndpoint && !unavailableRecovery && !failedCandidateRecovery {
				return model.App{}, fmt.Errorf("live service has no ready endpoint before an online replacement")
			}
		}
	}
	if desiredReplicas > 0 && model.AppHasClusterService(current.Spec) && !model.AppHasClusterService(desired.Spec) {
		return model.App{}, fmt.Errorf("requested reconciliation would remove a live cluster service")
	}
	if desiredReplicas > 0 && !model.AppHasClusterService(desired.Spec) {
		if ready, err := liveManagedAppHasReadyEndpoint(ctx, client, namespace, current); err != nil {
			return model.App{}, err
		} else if ready {
			return model.App{}, fmt.Errorf("requested reconciliation would remove a live cluster service")
		}
	}
	if readyDesiredCohortRecovery {
		return desired, nil
	}

	if !unavailableRecovery {
		if err := s.refuseNonZeroDowntimeRolloutWithSchedulingAndTemplateEvidence(
			ctx,
			op,
			current,
			desired,
			currentScheduling,
			desiredScheduling,
			liveKey,
			guardAuxiliaryTemplateChanged,
		); err != nil {
			return model.App{}, err
		}
	}
	decision := s.zeroDowntimeRolloutGuardDecisionWithSchedulingAndTemplateEvidence(
		op,
		current,
		desired,
		currentScheduling,
		desiredScheduling,
		liveKey,
		guardAuxiliaryTemplateChanged,
	)
	if decision.PodTemplateChanged && zeroDowntimeRequiresSameNodePin(desired.Spec) {
		nodeName, ok := s.currentReadyAppNodeForOnlineRolloutWithClient(ctx, client, current)
		if !ok || strings.TrimSpace(desiredScheduling.NodeSelector[kubeHostnameLabelKey]) != nodeName {
			return model.App{}, fmt.Errorf("the online RWO rollout is not pinned to the single current ready node")
		}
	}
	if decision.PodTemplateChanged {
		if !expectedDeploymentFound {
			return model.App{}, fmt.Errorf("rendered serving workload has no deployment for capacity preflight")
		}
		if message, err := zeroDowntimeRolloutCapacityBlockMessage(ctx, client, expectedDeployment, desired.Spec.Replicas); err != nil {
			return model.App{}, err
		} else if strings.TrimSpace(message) != "" {
			return model.App{}, fmt.Errorf("%s", message)
		}
	}
	return desired, nil
}

func (s *Service) managedAppReadyReleaseCohort(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	desired model.App,
	scheduling runtime.SchedulingConstraints,
	desiredReplicas int,
) (bool, error) {
	if client == nil || desiredReplicas <= 0 {
		return false, nil
	}
	releaseKey := strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(desired), scheduling))
	return s.managedAppReadyReleaseKeyCohort(ctx, client, namespace, desired, releaseKey, desiredReplicas)
}

func (s *Service) managedAppReadyReleaseKeyCohort(
	ctx context.Context,
	client *kubeClient,
	namespace string,
	app model.App,
	releaseKey string,
	desiredReplicas int,
) (bool, error) {
	if client == nil || desiredReplicas <= 0 {
		return false, nil
	}
	releaseKey = strings.TrimSpace(releaseKey)
	if releaseKey == "" {
		return false, nil
	}
	pods, err := client.listPodsBySelector(ctx, namespace, managedAppPodLabelSelector(app))
	if err != nil {
		return false, fmt.Errorf("list app pods while proving previous ready release: %w", err)
	}
	ready := 0
	for _, pod := range pods {
		if strings.TrimSpace(pod.Metadata.DeletionTimestamp) != "" ||
			!strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Running") ||
			!kubePodReady(pod) ||
			strings.TrimSpace(pod.Metadata.Annotations[runtime.FugueAnnotationReleaseKey]) != releaseKey {
			continue
		}
		ready++
	}
	return ready >= desiredReplicas, nil
}

// managedAppAllowsFailedCandidateReplacement handles a distinct online
// recovery state: a controller-authored candidate owns the Deployment
// template and has terminally failed, while an exact previous LKG cohort is
// still Ready behind the Service. The caller proves both release identities
// and the LKG pod cohort before entering this predicate.
func managedAppAllowsFailedCandidateReplacement(
	deployment kubeDeployment,
	current, desired model.AppSpec,
	readyEndpoint, readyLKG bool,
	desiredReplicas int,
) bool {
	if desiredReplicas <= 0 || !readyEndpoint || !readyLKG ||
		deployment.Status.ObservedGeneration < deployment.Metadata.Generation ||
		deployment.Status.ReadyReplicas < desiredReplicas ||
		deployment.Status.AvailableReplicas < desiredReplicas ||
		deployment.Status.Replicas <= desiredReplicas ||
		deployment.Status.UpdatedReplicas == 0 ||
		deployment.Status.UnavailableReplicas == 0 ||
		!hasDeploymentFailureCondition(deployment.Status.Conditions) {
		return false
	}
	return controllerAppSupportsConcurrentStorage(current) &&
		controllerAppSupportsConcurrentStorage(desired)
}

func managedAppAuxiliaryTemplateChangeRequiresGuard(app model.App) bool {
	return model.AppZeroDowntimeEnabled(app.Spec) || appHasOnlineRolloutIntent(app)
}

// recoverManagedAppPendingDeploySnapshot closes the narrow crash window where
// a deploy applied and became Ready, but the operation failed before its
// desired snapshot was committed to the store. The ManagedApp status normally
// retains the controller-authored pending release identity while background
// reconcile restores the older durable spec. Older status writes (and a few
// pre-fix failure paths), however, promoted that identity directly to
// CurrentReleaseKey and cleared PendingReleaseKey before the operation was
// durably completed. When that happens, a later deploy must still be able to
// use the actual serving snapshot as its zero-downtime baseline instead of
// treating the controller-authored, operation-proven release as arbitrary
// live drift.
//
// Recovery is deliberately fail-closed. The recovery key must match the live
// Deployment, belong to a completed failed deploy (and, for an active deploy,
// predate it), fall inside that operation's time interval, and be reproduced
// exactly from the operation's full desired spec/source state. Background
// reconciliation may enter this path only for the promoted current release
// form, and only when the complete failed-operation proof below is available.
// Unverifiable/manual identities remain fail-closed.
type managedAppRecoveredDeploySnapshot struct {
	App        model.App
	Operation  model.Operation
	Scheduling runtime.SchedulingConstraints
}

func (s *Service) recoverManagedAppPendingDeploySnapshot(
	ctx context.Context,
	managed runtime.ManagedAppObject,
	current model.App,
	liveKey string,
) (managedAppRecoveredDeploySnapshot, bool, error) {
	liveKey = strings.TrimSpace(liveKey)
	pendingKey := strings.TrimSpace(managed.Status.PendingReleaseKey)
	pendingStartedAt := parseManagedAppStatusTimestamp(managed.Status.PendingReleaseStartedAt)
	recoveryKey := pendingKey
	recoveryStartedAt := pendingStartedAt
	// A completed rollout can be represented by CurrentReleaseKey without a
	// pending key when the status publish raced operation completion. Accept
	// that form only with both controller-authored release timestamps and the
	// durable serving evidence; the operation-history proof below remains
	// mandatory. This does not broaden recovery to arbitrary live annotations.
	if recoveryKey == "" {
		recoveryKey = strings.TrimSpace(managed.Status.CurrentReleaseKey)
		recoveryStartedAt = parseManagedAppStatusTimestamp(managed.Status.CurrentReleaseStartedAt)
		if recoveryKey == "" || recoveryStartedAt == nil ||
			parseManagedAppStatusTimestamp(managed.Status.CurrentReleaseReadyAt) == nil ||
			!managedAppStatusShowsServing(managed.Status) {
			return managedAppRecoveredDeploySnapshot{}, false, nil
		}
	}
	apply := managedAppApplySourceFromContext(ctx)
	if liveKey == "" || recoveryKey != liveKey || recoveryStartedAt == nil ||
		managed.Status.ObservedGeneration < managed.Metadata.Generation ||
		s == nil || s.Store == nil {
		return managedAppRecoveredDeploySnapshot{}, false, nil
	}

	var active *model.Operation
	if apply.Source == managedAppApplySourceOperation {
		if apply.OperationID == "" {
			return managedAppRecoveredDeploySnapshot{}, false, nil
		}
		candidate, err := s.Store.GetOperation(apply.OperationID)
		if err != nil {
			return managedAppRecoveredDeploySnapshot{}, false, fmt.Errorf("read active deploy while recovering pending live release: %w", err)
		}
		if candidate.Type != model.OperationTypeDeploy || candidate.Status != model.OperationStatusRunning ||
			candidate.AppID != current.ID || candidate.TenantID != current.TenantID {
			return managedAppRecoveredDeploySnapshot{}, false, nil
		}
		active = &candidate
	} else {
		// Background reconciliation may run after the operation that produced a
		// serving release has already failed/finished. A failed operation can
		// leave its controller-authored key in PendingReleaseKey even though the
		// ManagedApp is already in Error. That representation is recoverable only
		// through the exact failed-operation proof below; all other phases and
		// unproven pending identities remain fail-closed.
		phase := strings.TrimSpace(managed.Status.Phase)
		if (pendingKey != "" && !strings.EqualFold(phase, runtime.ManagedAppPhaseError)) ||
			(pendingKey == "" && !strings.EqualFold(phase, runtime.ManagedAppPhaseReady)) {
			return managedAppRecoveredDeploySnapshot{}, false, nil
		}
	}

	operations, err := s.Store.ListOperationsByApp(current.TenantID, true, current.ID)
	if err != nil {
		return managedAppRecoveredDeploySnapshot{}, false, fmt.Errorf("list deploy history while recovering pending live release: %w", err)
	}

	var recovered managedAppRecoveredDeploySnapshot
	var recoveredAt time.Time
	for _, candidate := range operations {
		if (active != nil && candidate.ID == active.ID) || candidate.Type != model.OperationTypeDeploy ||
			candidate.Status != model.OperationStatusFailed || candidate.DesiredSpec == nil ||
			candidate.CompletedAt == nil || (active != nil && !candidate.CompletedAt.Before(active.CreatedAt)) {
			continue
		}
		attemptStartedAt := candidate.CreatedAt
		if candidate.StartedAt != nil {
			attemptStartedAt = *candidate.StartedAt
		}
		if recoveryStartedAt.Before(attemptStartedAt) || recoveryStartedAt.After(*candidate.CompletedAt) {
			continue
		}

		snapshot := current
		snapshot.Spec = *cloneControllerAppSpec(candidate.DesiredSpec)
		buildSource := model.AppBuildSource(snapshot)
		if candidate.DesiredSource != nil {
			buildSource = model.CloneAppSource(candidate.DesiredSource)
		}
		originSource := model.AppOriginSource(snapshot)
		if candidate.DesiredOriginSource != nil {
			originSource = model.CloneAppSource(candidate.DesiredOriginSource)
		}
		model.SetAppSourceState(&snapshot, originSource, buildSource)
		if normalized, changed := s.normalizeManagedAppRuntimeImageRefs(snapshot); changed {
			snapshot = normalized
		}
		snapshot = s.appWithResolvedLaunchOverride(ctx, snapshot)
		snapshot.Spec.RolloutIntent = rolloutIntentForManagedOperation(candidate, current, snapshot)
		snapshot = s.Renderer.PrepareApp(snapshot)
		candidateScheduling := managed.Spec.Scheduling
		// A failed deploy created by a runtime move is rendered with the
		// operation target's scheduling constraints. The ManagedApp status may
		// already have been restored to the durable source scheduling, so using
		// it here would make an otherwise exact candidate proof impossible. Read
		// the target runtime constraints from the operation snapshot; retain the
		// managed scheduling only for same-runtime/legacy operations where no
		// separate target proof is needed.
		if targetRuntimeID := strings.TrimSpace(snapshot.Spec.RuntimeID); targetRuntimeID != "" &&
			targetRuntimeID != strings.TrimSpace(current.Spec.RuntimeID) {
			targetScheduling, schedulingErr := s.managedSchedulingConstraints(targetRuntimeID)
			if schedulingErr != nil {
				return managedAppRecoveredDeploySnapshot{}, false, fmt.Errorf(
					"resolve failed pending release runtime scheduling for app %s operation %s runtime %s: %w",
					current.ID, candidate.ID, targetRuntimeID, schedulingErr,
				)
			}
			candidateScheduling = targetScheduling
		}
		candidateKey := strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(snapshot, candidateScheduling))
		if candidateKey != liveKey {
			continue
		}
		if recovered.App.ID == "" || candidate.CompletedAt.After(recoveredAt) {
			recovered = managedAppRecoveredDeploySnapshot{
				App:        snapshot,
				Operation:  candidate,
				Scheduling: candidateScheduling,
			}
			recoveredAt = *candidate.CompletedAt
		}
	}
	if recovered.App.ID == "" {
		return managedAppRecoveredDeploySnapshot{}, false, nil
	}
	return recovered, true, nil
}

// managedAppAllowsUnavailableRecovery breaks a deadlock where a terminally
// failed stateless Deployment cannot accept the image or configuration change
// that would repair it because the online rollout guard first requires the old
// release to be healthy. Live Kubernetes evidence remains authoritative: the
// Deployment must be fully observed, have no ready or available replicas, have
// no ready Service endpoint, and report a failed rollout condition. Workloads
// whose current or desired state cannot mount concurrently remain fail-closed.
func managedAppAllowsUnavailableRecovery(
	deployment kubeDeployment,
	current, desired model.AppSpec,
	readyEndpoint bool,
) bool {
	if readyEndpoint ||
		deployment.Status.ObservedGeneration < deployment.Metadata.Generation ||
		deployment.Status.ReadyReplicas > 0 ||
		deployment.Status.AvailableReplicas > 0 ||
		!hasDeploymentFailureCondition(deployment.Status.Conditions) {
		return false
	}
	return controllerAppSupportsConcurrentStorage(current) &&
		controllerAppSupportsConcurrentStorage(desired)
}

func liveDeploymentStrategyMatchesDesired(live kubeDeployment, desired map[string]any) bool {
	if !strings.EqualFold(strings.TrimSpace(live.Spec.Strategy.Type), deploymentStrategyTypeFromObject(desired)) {
		return false
	}
	strategy := nestedObjectMap(desired, "spec", "strategy")
	rolling := nestedObjectMap(strategy, "rollingUpdate")
	if !strings.EqualFold(strings.TrimSpace(live.Spec.Strategy.Type), "RollingUpdate") {
		return true
	}
	return normalizedKubeIntOrString(live.Spec.Strategy.RollingUpdate.MaxUnavailable) == normalizedKubeIntOrString(rolling["maxUnavailable"]) &&
		normalizedKubeIntOrString(live.Spec.Strategy.RollingUpdate.MaxSurge) == normalizedKubeIntOrString(rolling["maxSurge"])
}

type managedDeploymentAuxiliaryTemplateFingerprint struct {
	DrainAnnotations              map[string]string
	DrainAgent                    *kubeContainerSpec
	ContainerLifecycles           map[string]map[string]any
	TerminationGracePeriodSeconds *int64
	ShareProcessNamespace         *bool
}

func managedDeploymentAuxiliaryTemplateChanged(live, desired kubeDeployment) bool {
	return !reflect.DeepEqual(
		managedDeploymentAuxiliaryTemplateFingerprintFor(live),
		managedDeploymentAuxiliaryTemplateFingerprintFor(desired),
	)
}

func managedDeploymentAuxiliaryTemplateFingerprintFor(deployment kubeDeployment) managedDeploymentAuxiliaryTemplateFingerprint {
	fingerprint := managedDeploymentAuxiliaryTemplateFingerprint{
		TerminationGracePeriodSeconds: deployment.Spec.Template.Spec.TerminationGracePeriodSeconds,
		ShareProcessNamespace:         deployment.Spec.Template.Spec.ShareProcessNamespace,
	}
	for _, key := range []string{
		"fugue.io/drain-mode",
		"fugue.io/drain-timeout-seconds",
		"fugue.io/drain-quiet-period-seconds",
		"fugue.io/drain-agent-port",
		"fugue.io/termination-grace-min-seconds",
	} {
		if value := strings.TrimSpace(deployment.Spec.Template.Metadata.Annotations[key]); value != "" {
			if fingerprint.DrainAnnotations == nil {
				fingerprint.DrainAnnotations = map[string]string{}
			}
			fingerprint.DrainAnnotations[key] = value
		}
	}
	for _, container := range deployment.Spec.Template.Spec.InitContainers {
		if strings.TrimSpace(container.Name) != "fugue-drain-agent" {
			continue
		}
		copy := container
		fingerprint.DrainAgent = &copy
		break
	}
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if len(container.Lifecycle) == 0 {
			continue
		}
		if fingerprint.ContainerLifecycles == nil {
			fingerprint.ContainerLifecycles = map[string]map[string]any{}
		}
		fingerprint.ContainerLifecycles[strings.TrimSpace(container.Name)] = container.Lifecycle
	}
	return fingerprint
}

func liveManagedAppHasReadyEndpoint(ctx context.Context, client *kubeClient, namespace string, app model.App) (bool, error) {
	serviceName := runtime.RuntimeAppServiceName(app)
	slices, err := client.listEndpointSlicesForService(ctx, namespace, serviceName)
	if err != nil {
		return false, fmt.Errorf("read live service endpoints before zero-downtime reconcile: %w", err)
	}
	ready, total := countReadyEndpointAddresses(slices)
	if len(slices) == 0 && total == 0 {
		endpoints, found, err := client.getEndpointsForService(ctx, namespace, serviceName)
		if err != nil {
			return false, fmt.Errorf("read live legacy service endpoints before zero-downtime reconcile: %w", err)
		}
		if found {
			ready, _ = countReadyLegacyEndpointAddresses(endpoints)
		}
	}
	return ready > 0, nil
}

func managedAppStatusShowsServing(status runtime.ManagedAppStatus) bool {
	if status.ReadyReplicas > 0 ||
		strings.TrimSpace(status.CurrentReleaseStartedAt) != "" ||
		strings.TrimSpace(status.CurrentReleaseReadyAt) != "" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(status.Phase)) {
	case runtime.ManagedAppPhaseReady:
		return true
	default:
		for _, condition := range status.Conditions {
			if strings.EqualFold(strings.TrimSpace(condition.Type), "ZeroDowntimeBlocked") &&
				strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
				return true
			}
		}
		return false
	}
}

func (s *Service) zeroDowntimeRolloutGuardDecision(
	op model.Operation,
	currentApp, desiredApp model.App,
	scheduling runtime.SchedulingConstraints,
) zeroDowntimeRolloutGuardDecision {
	return s.zeroDowntimeRolloutGuardDecisionWithScheduling(op, currentApp, desiredApp, scheduling, scheduling, "")
}

func (s *Service) zeroDowntimeRolloutGuardDecisionWithScheduling(
	op model.Operation,
	currentApp, desiredApp model.App,
	currentScheduling, desiredScheduling runtime.SchedulingConstraints,
	observedReleaseKey string,
) zeroDowntimeRolloutGuardDecision {
	return s.zeroDowntimeRolloutGuardDecisionWithSchedulingAndTemplateEvidence(
		op,
		currentApp,
		desiredApp,
		currentScheduling,
		desiredScheduling,
		observedReleaseKey,
		false,
	)
}

func (s *Service) zeroDowntimeRolloutGuardDecisionWithSchedulingAndTemplateEvidence(
	op model.Operation,
	currentApp, desiredApp model.App,
	currentScheduling, desiredScheduling runtime.SchedulingConstraints,
	observedReleaseKey string,
	auxiliaryTemplateChanged bool,
) zeroDowntimeRolloutGuardDecision {
	decision := zeroDowntimeRolloutGuardDecision{
		PolicyMode:               zeroDowntimePolicyMode(currentApp.Spec, desiredApp.Spec),
		RequirementSource:        zeroDowntimeRequirementSourceForOperation(currentApp, desiredApp),
		RolloutIntent:            strings.TrimSpace(desiredApp.Spec.RolloutIntent),
		AuxiliaryTemplateChanged: auxiliaryTemplateChanged,
	}
	if op.Type != model.OperationTypeDeploy && op.Type != model.OperationTypeMigrate && op.Type != model.OperationTypeScale {
		return decision
	}
	if desiredApp.Spec.Replicas > 1 && model.AppHasClusterService(desiredApp.Spec) && !controllerAppSupportsConcurrentStorage(desiredApp.Spec) {
		decision.Refused = true
		decision.RequirementSource = model.AppZeroDowntimeRequirementSourceServiceDefault
		decision.Reason = zeroDowntimeUnsupportedTopologyReason(desiredApp.Spec)
		return decision
	}
	if decision.RequirementSource == "" {
		return decision
	}
	if currentApp.Spec.Replicas <= 0 || !model.AppHasClusterService(currentApp.Spec) {
		return decision
	}
	if op.Type == model.OperationTypeScale {
		if desiredApp.Spec.Replicas <= 0 {
			// Scale-to-zero is the explicit maintenance stop operation.
			return decision
		}
		if desiredApp.Spec.Replicas > 1 && !controllerAppSupportsConcurrentStorage(desiredApp.Spec) {
			decision.Refused = true
			decision.Reason = zeroDowntimeUnsupportedTopologyReason(desiredApp.Spec)
			return decision
		}
	}
	if desiredApp.Spec.Replicas <= 0 {
		decision.Refused = true
		decision.Reason = "the requested restart removes every serving replica while zero downtime is enabled"
		return decision
	}
	if !model.AppHasClusterService(desiredApp.Spec) {
		decision.Refused = true
		decision.Reason = "the requested restart removes the cluster service while zero downtime is enabled"
		return decision
	}

	decision.PreviousReleaseKey = strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(currentApp), currentScheduling))
	decision.DesiredReleaseKey = strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(desiredApp), desiredScheduling))
	if observed := strings.TrimSpace(observedReleaseKey); observed != "" {
		if observed != decision.PreviousReleaseKey && observed != decision.DesiredReleaseKey {
			decision.Refused = true
			decision.PreviousReleaseKey = observed
			decision.Reason = "the live deployment release identity does not match either the current or desired snapshot"
			return decision
		}
		decision.PreviousReleaseKey = observed
	}
	desiredDeployment := s.zeroDowntimeManagedAppDeployment(desiredApp, desiredScheduling)
	decision.Strategy = deploymentStrategyTypeFromObject(desiredDeployment)
	desiredAnnotations := objectStringMapValue(nestedObjectValue(desiredDeployment, "metadata", "annotations"))
	decision.DowntimeClass = strings.TrimSpace(desiredAnnotations["fugue.io/downtime-class"])
	decision.RolloutMode = strings.TrimSpace(desiredAnnotations["fugue.io/rollout-mode"])
	if strings.TrimSpace(decision.Strategy) == "" {
		decision.Refused = true
		decision.Reason = "the rendered serving workload has no explicit deployment strategy while zero downtime is enabled"
		return decision
	}
	if strings.EqualFold(decision.Strategy, "Recreate") ||
		strings.EqualFold(decision.DowntimeClass, "downtime-required") ||
		strings.EqualFold(decision.RolloutMode, "isolated-singleton") {
		decision.Refused = true
		decision.Reason = "the rendered serving workload requires a Recreate rollout while zero downtime is enabled"
		return decision
	}
	if !zeroDowntimeDeploymentStrategyIsSafe(desiredDeployment) {
		decision.Refused = true
		decision.Reason = "the rendered serving workload is not an explicit safe RollingUpdate"
		return decision
	}
	if observed := strings.TrimSpace(observedReleaseKey); observed != "" {
		decision.PodTemplateChanged = observed != decision.DesiredReleaseKey || auxiliaryTemplateChanged
	} else {
		previousTemplate := s.zeroDowntimeManagedAppPodTemplate(currentApp, currentScheduling)
		desiredTemplate := nestedObjectValue(desiredDeployment, "spec", "template")
		decision.PodTemplateChanged = !reflect.DeepEqual(previousTemplate, desiredTemplate)
	}
	if !decision.PodTemplateChanged {
		return decision
	}
	if !appSupportsOnlineRolloutIntent(desiredApp) {
		decision.Refused = true
		decision.Reason = zeroDowntimeUnsupportedTopologyReason(desiredApp.Spec)
		return decision
	}
	if !appHasOnlineRolloutIntent(desiredApp) {
		decision.Refused = true
		decision.Reason = "the requested restart has no validated online rollout plan"
		return decision
	}
	if zeroDowntimeRequiresSameNodePin(desiredApp.Spec) &&
		strings.TrimSpace(desiredScheduling.NodeSelector[kubeHostnameLabelKey]) == "" {
		decision.Refused = true
		decision.Reason = "the online RWO rollout has no validated current-node placement"
		return decision
	}
	return decision
}

func zeroDowntimeDeploymentStrategyIsSafe(deployment map[string]any) bool {
	if !strings.EqualFold(deploymentStrategyTypeFromObject(deployment), "RollingUpdate") {
		return false
	}
	strategy := nestedObjectMap(deployment, "spec", "strategy")
	rolling := nestedObjectMap(strategy, "rollingUpdate")
	if normalizedKubeIntOrString(rolling["maxUnavailable"]) != "0" {
		return false
	}
	return kubeIntOrStringPositive(rolling["maxSurge"])
}

func zeroDowntimeRequiresSameNodePin(spec model.AppSpec) bool {
	if spec.Workspace != nil {
		return model.AppWorkspaceSpecSupportsSameNodeOnlineRollout(spec.Workspace)
	}
	return model.AppPersistentStorageSpecSupportsSameNodeOnlineRollout(spec.PersistentStorage)
}

func controllerAppSupportsConcurrentStorage(spec model.AppSpec) bool {
	if spec.Workspace != nil {
		return false
	}
	if spec.PersistentStorage == nil {
		return true
	}
	return model.AppPersistentStorageSpecUsesSharedProjectRWX(spec.PersistentStorage)
}

func zeroDowntimeRequirementSourceForOperation(currentApp, desiredApp model.App) string {
	if zeroDowntimePolicyMode(currentApp.Spec, desiredApp.Spec) != "" {
		return model.AppZeroDowntimeRequirementSourceServicePolicy
	}
	if managedAppHasLiveServiceToProtect(currentApp) || managedAppHasLiveServiceToProtect(desiredApp) {
		return model.AppZeroDowntimeRequirementSourceServiceDefault
	}
	return ""
}

func managedAppHasLiveServiceToProtect(app model.App) bool {
	if app.Spec.Replicas <= 0 || !model.AppHasClusterService(app.Spec) {
		return false
	}
	// CurrentReplicas can legitimately fall to zero while a previously serving
	// app is unhealthy or while status observation is catching up. Keep the
	// default fail-closed contract in those states. CurrentRuntimeID and release
	// timestamps are durable evidence that this is not an initial deployment.
	return app.Status.CurrentReplicas > 0 ||
		strings.TrimSpace(app.Status.CurrentRuntimeID) != "" ||
		app.Status.CurrentReleaseStartedAt != nil ||
		app.Status.CurrentReleaseReadyAt != nil
}

func (s *Service) zeroDowntimeManagedAppPodTemplate(app model.App, scheduling runtime.SchedulingConstraints) any {
	return nestedObjectValue(s.zeroDowntimeManagedAppDeployment(app, scheduling), "spec", "template")
}

func (s *Service) zeroDowntimeManagedAppDeployment(app model.App, scheduling runtime.SchedulingConstraints) map[string]any {
	prepared := s.Renderer.PrepareApp(app)
	objects := s.Renderer.BuildManagedAppChildObjects(prepared, scheduling, nil)
	return firstManagedAppDeploymentObject(objects, runtime.RuntimeAppResourceName(prepared))
}

func zeroDowntimePolicyMode(specs ...model.AppSpec) string {
	for index := len(specs) - 1; index >= 0; index-- {
		policy := model.NormalizeAppContinuityPolicy(specs[index].Continuity)
		if policy != nil && policy.ZeroDowntime != nil && policy.ZeroDowntime.Enabled {
			return strings.TrimSpace(policy.ZeroDowntime.Mode)
		}
	}
	return ""
}

func zeroDowntimeUnsupportedTopologyReason(spec model.AppSpec) string {
	if spec.Workspace != nil {
		return model.AppStorageClassSameNodeOnlineMountUnsupportedSummary(spec.Workspace.StorageClassName)
	}
	if spec.PersistentStorage != nil {
		if model.AppPersistentStorageSpecUsesSharedProjectRWX(spec.PersistentStorage) {
			return "shared RWX storage could not be validated for an online rollout"
		}
		return model.AppStorageClassSameNodeOnlineMountUnsupportedSummary(spec.PersistentStorage.StorageClassName)
	}
	return "the service topology cannot run an old and replacement pod concurrently"
}

func zeroDowntimeAppStorageClass(spec model.AppSpec) string {
	if spec.Workspace != nil {
		return strings.TrimSpace(spec.Workspace.StorageClassName)
	}
	if spec.PersistentStorage != nil {
		return strings.TrimSpace(spec.PersistentStorage.StorageClassName)
	}
	return ""
}

func zeroDowntimeAppStorageMode(spec model.AppSpec) string {
	if spec.Workspace != nil {
		return "workspace"
	}
	if spec.PersistentStorage != nil {
		return strings.TrimSpace(spec.PersistentStorage.Mode)
	}
	return "stateless"
}

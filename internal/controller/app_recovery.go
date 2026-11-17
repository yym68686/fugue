package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseflow"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const recoveryLabel = "fugue.pro/incident-recovery"

var recoveryDigest = regexp.MustCompile(`sha256:[a-f0-9]{64}$`)
var errRecoveryWaiting = errors.New("recovery is waiting for runtime convergence")

func recoveryObjectPath(namespace, resource, name string) string {
	prefix := "/api/v1/namespaces/"
	if resource == "deployments" || resource == "replicasets" {
		prefix = "/apis/apps/v1/namespaces/"
	}
	return prefix + url.PathEscape(namespace) + "/" + resource + "/" + url.PathEscape(name)
}

// Intercept before any ordinary reconciliation, auto resize, migration or
// retirement can touch a held workload. One bounded, restartable step per pass.
func (s *Service) reconcileAppRecovery(ctx context.Context, client *kubeClient, managed runtime.ManagedAppObject, app model.App) (bool, error) {
	records, err := s.Store.ListAppRecoveries(app.ID)
	if err != nil {
		return true, err
	}
	for _, r := range records {
		if !r.Active {
			continue
		}
		stepTimeout := 20 * time.Second
		if r.Phase == "probing" {
			stepTimeout = 65 * time.Second
		}
		ctx, cancel := context.WithTimeout(ctx, stepTimeout)
		defer cancel()
		next := r
		if r.CutoverAt == nil && r.Phase != "failed" && !r.ReleaseRequested && time.Now().After(r.Deadline) {
			next.Phase = "failed"
			next.Message = "recovery deadline exceeded; original traffic and investigation hold retained; release explicitly to resume normal reconciliation"
			_, err := s.Store.SaveAppRecovery(r, next, false)
			return true, err
		}
		if len(r.SourcePods) > 0 {
			next.EvidenceStatus, err = s.observeRecoveryEvidence(ctx, client, r)
			if err != nil {
				return true, err
			}
		}
		if r.ReleaseRequested || r.Phase == "retained" && r.RetentionUntil != nil && !time.Now().Before(*r.RetentionUntil) {
			next.ReleaseRequested = true
			if r.CutoverAt != nil && r.Phase == "publishing" {
				err = s.publishRecovery(ctx, client, app, records, &next)
			} else {
				next.Phase = "releasing"
				err = s.releaseRecoveryRuntime(ctx, client, app, &next)
			}
		} else {
			switch r.Phase {
			case "pending":
				err = s.captureRecoverySource(ctx, client, app, &next)
			case "preparing":
				err = s.prepareRecoveryReplacement(ctx, client, managed, app, &next)
			case "probing":
				err = s.probeRecoveryReplacement(ctx, client, app, &next)
				if ctx.Err() != nil {
					return true, ctx.Err()
				}
				if err == nil && len(next.ProbeResults) == len(r.Request.Probes) {
					_, err = s.Store.SaveAppRecovery(r, next, true)
					return true, err
				}
			case "publishing":
				err = s.publishRecovery(ctx, client, app, records, &next)
			case "retained", "failed":
				if !reflect.DeepEqual(next, r) {
					_, err = s.Store.SaveAppRecovery(r, next, false)
				}
				if err == nil && r.CutoverAt != nil && r.Phase == "retained" {
					err = s.syncManagedAppObservedStatus(ctx, client, r.Namespace, managed, app, nil, s.Renderer.ManagedAppReleaseKey(app, managed.Spec.Scheduling), false)
				}
				return true, err
			}
		}
		if err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return true, err
			}
			if r.CutoverAt == nil && time.Now().After(r.Deadline) && !r.ReleaseRequested {
				next.Phase = "failed"
				next.Message = "recovery deadline exceeded; source traffic and investigation hold retained; release explicitly to resume normal reconciliation"
			} else if !errors.Is(err, errRecoveryWaiting) {
				next.Message = "recovery step blocked; retained resources are unchanged (see controller diagnostics)"
			}
			if s.Logger != nil && !errors.Is(err, errRecoveryWaiting) {
				s.Logger.Printf("incident recovery app=%s recovery=%s phase=%s deferred: %v", app.ID, r.ID, r.Phase, err)
			}
		}
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		if !reflect.DeepEqual(next, r) {
			_, saveErr := s.Store.SaveAppRecovery(r, next, false)
			if saveErr != nil {
				return true, saveErr
			}
		}
		return true, nil
	}
	return false, nil
}

func (s *Service) captureRecoverySource(ctx context.Context, c *kubeClient, app model.App, r *model.AppRecoveryRecord) error {
	if model.AppSpecSHA256(app.Spec) != model.AppSpecSHA256(r.SourceSpec) {
		return fmt.Errorf("source specification changed")
	}
	r.Namespace = runtime.NamespaceForTenant(app.TenantID)
	r.SourceDeployment = runtime.RuntimeAppResourceName(app)
	r.SourceService = runtime.RuntimeAppServiceName(app)
	d, found, err := c.getRawObject(ctx, recoveryObjectPath(r.Namespace, "deployments", r.SourceDeployment))
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("source deployment missing")
	}
	if nestedObjectString(d, "metadata", "labels", runtime.FugueLabelAppID) != app.ID {
		return fmt.Errorf("source deployment ownership unavailable")
	}
	r.SourceDeploymentUID = nestedObjectString(d, "metadata", "uid")
	if r.SourceDeploymentUID == "" || nestedObjectString(d, "metadata", "deletionTimestamp") != "" {
		return fmt.Errorf("source deployment identity unavailable")
	}
	service, found, err := c.getRawObject(ctx, recoveryObjectPath(r.Namespace, "services", r.SourceService))
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("source service missing")
	}
	if nestedObjectString(service, "metadata", "labels", runtime.FugueLabelAppID) != app.ID || nestedObjectString(service, "metadata", "uid") == "" {
		return fmt.Errorf("source service ownership unavailable")
	}
	r.SourceServiceUID = nestedObjectString(service, "metadata", "uid")
	policy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err == nil {
		if policy.CandidateWeight != 0 || policy.CandidateReleaseID != "" {
			return fmt.Errorf("an independent release is active; resolve it before recovery")
		}
		release, e := s.Store.GetAppRelease(app.TenantID, true, policy.StableReleaseID)
		if e != nil {
			return e
		}
		if release.DeploymentName != "" && release.DeploymentName != r.SourceDeployment || release.ServiceName != "" && release.ServiceName != r.SourceService || release.RevisionWorkload != nil {
			return fmt.Errorf("stable traffic is not on the canonical source workload")
		}
		r.SourceTraffic = &policy
		r.SourceReleaseID = release.ID
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	pods, err := c.listPodsBySelector(ctx, r.Namespace, runtime.FugueLabelAppWorkload+"="+r.SourceDeployment)
	if err != nil {
		return err
	}
	if len(pods) != 1 {
		return fmt.Errorf("require exactly one non-terminating original Pod, observed %d", len(pods))
	}
	p := pods[0]
	if p.ObservedUID == "" || p.Metadata.DeletionTimestamp != "" || p.Spec.NodeName == "" {
		return fmt.Errorf("original Pod identity or execution state unknown")
	}
	var node struct {
		Status struct {
			Conditions []struct{ Type, Status string }
		}
	}
	if _, err := c.doJSON(ctx, http.MethodGet, "/api/v1/nodes/"+url.PathEscape(p.Spec.NodeName), nil, &node); err != nil {
		return err
	}
	ready := false
	for _, condition := range node.Status.Conditions {
		if condition.Type == "Ready" && condition.Status == "True" {
			ready = true
		}
	}
	if !ready {
		return fmt.Errorf("source node is not positively Ready; do not replace an unknown execution state")
	}
	sets, err := c.listReplicaSetsBySelector(ctx, r.Namespace, runtime.FugueLabelAppWorkload+"="+r.SourceDeployment)
	if err != nil {
		return err
	}
	r.SourceReplicaSets = map[string]string{}
	for _, rs := range sets {
		raw, found, e := c.getRawObject(ctx, recoveryObjectPath(r.Namespace, "replicasets", rs.Metadata.Name))
		if e != nil {
			return e
		}
		if !found {
			return errRecoveryWaiting
		}
		owned := false
		for _, o := range mapSlice(nestedObjectValue(raw, "metadata", "ownerReferences")) {
			if objectStringField(o, "uid") == r.SourceDeploymentUID && objectStringField(o, "kind") == "Deployment" {
				owned = true
			}
		}
		if !owned {
			return fmt.Errorf("unexpected ReplicaSet ownership")
		}
		r.SourceReplicaSets[rs.Metadata.Name] = nestedObjectString(raw, "metadata", "uid")
	}
	if len(r.SourceReplicaSets) == 0 {
		return fmt.Errorf("source ReplicaSet ownership is unavailable")
	}
	for _, o := range p.ObservedOwnerReferences {
		if o.Controller && r.SourceReplicaSets[o.Name] != o.UID {
			return fmt.Errorf("original Pod has unexpected controller")
		}
	}
	identity := model.AppRecoveryPod{Name: p.Metadata.Name, UID: p.ObservedUID, Node: p.Spec.NodeName, Containers: []model.AppRecoveryContainer{}}
	for _, cs := range append(p.Status.InitContainerStatuses, p.Status.ContainerStatuses...) {
		if cs.ContainerID != "" {
			identity.Containers = append(identity.Containers, model.AppRecoveryContainer{Name: cs.Name, ID: cs.ContainerID, RestartCount: cs.RestartCount})
		}
	}
	containers := mapSlice(nestedObjectValue(d, "spec", "template", "spec", "containers"))
	if len(containers) == 0 || len(identity.Containers) == 0 {
		return fmt.Errorf("source container identity unavailable")
	}
	main := objectStringField(containers[0], "name")
	digest := ""
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Name == main {
			digest = recoveryDigest.FindString(cs.ImageID)
		}
	}
	if digest == "" {
		return fmt.Errorf("running business container image digest unavailable")
	}
	image := objectStringField(containers[0], "image")
	if i := strings.Index(image, "@"); i >= 0 {
		image = image[:i]
	}
	if i := strings.LastIndex(image, ":"); i > strings.LastIndex(image, "/") {
		image = image[:i]
	}
	if image == "" {
		return fmt.Errorf("source image repository unavailable")
	}
	r.ImageRef = image + "@" + digest
	r.SourcePods = []model.AppRecoveryPod{identity}
	r.EvidenceStatus = "intact"
	data, _ := json.Marshal(r.SourceSpec)
	var spec model.AppSpec
	if err := json.Unmarshal(data, &spec); err != nil {
		return err
	}
	spec.Image = r.ImageRef
	spec.WorkloadGeneration = strings.TrimPrefix(r.ID, "recovery_")
	spec.EphemeralPaths = model.RecoveryEphemeralPaths(spec, r.Request.EphemeralPaths)
	if err := model.ValidateRecoveryPaths(spec.EphemeralPaths, spec); err != nil {
		return err
	}
	replacement := app
	replacement.Spec = spec
	if spec.Workspace != nil {
		r.SourcePVC = runtime.WorkspacePVCName(app)
		spec.Workspace.StoragePath = ""
		spec.Workspace.ResetToken = ""
		r.ReplacementPVC = runtime.WorkspacePVCName(replacement)
	} else if spec.PersistentStorage != nil {
		if model.AppPersistentStorageSpecUsesSharedProjectRWX(spec.PersistentStorage) {
			return fmt.Errorf("shared project volumes require an explicit independent storage migration before recovery")
		}
		r.SourcePVC = runtime.PersistentStoragePVCName(app, *spec.PersistentStorage)
		spec.PersistentStorage.ClaimName = runtime.WorkspacePVCName(replacement)
		if spec.PersistentStorage.ClaimName == r.SourcePVC {
			return fmt.Errorf("replacement PVC identity collided")
		}
		spec.PersistentStorage.StoragePath = ""
		spec.PersistentStorage.ResetToken = ""
		r.ReplacementPVC = spec.PersistentStorage.ClaimName
	}
	r.ReplacementSpec = &spec
	replacement.Spec = spec
	r.ReplacementDeployment = runtime.RuntimeAppResourceName(replacement)
	r.ReplacementService = runtime.RuntimeAppServiceName(replacement)
	r.Phase = "preparing"
	r.Message = "captured exact source identity; preparing independent replacement"
	return nil
}

func (s *Service) prepareRecoveryReplacement(ctx context.Context, c *kubeClient, managed runtime.ManagedAppObject, app model.App, r *model.AppRecoveryRecord) error {
	if err := freezeRecoverySource(ctx, c, *r); err != nil {
		return err
	}
	return s.applyRecoveryReplacement(ctx, c, managed, app, r)
}

func freezeRecoverySource(ctx context.Context, c *kubeClient, r model.AppRecoveryRecord) error {
	path := recoveryObjectPath(r.Namespace, "deployments", r.SourceDeployment)
	d, found, err := c.getRawObject(ctx, path)
	if err != nil {
		return err
	}
	if !found || nestedObjectString(d, "metadata", "uid") != r.SourceDeploymentUID {
		return fmt.Errorf("source deployment identity changed")
	}
	if paused, _ := nestedObjectValue(d, "spec", "paused").(bool); !paused {
		if err := c.patchAppWorkloadObject(ctx, path, d, map[string]any{"spec": map[string]any{"paused": true}}); err != nil {
			return err
		}
		return errRecoveryWaiting
	}
	sets, err := c.listReplicaSetsBySelector(ctx, r.Namespace, runtime.FugueLabelAppWorkload+"="+r.SourceDeployment)
	if err != nil {
		return err
	}
	for _, rs := range sets {
		uid := r.SourceReplicaSets[rs.Metadata.Name]
		if uid == "" {
			return fmt.Errorf("unexpected ReplicaSet appeared after capture")
		}
		if err := deleteRecoveryObject(ctx, c, recoveryObjectPath(r.Namespace, "replicasets", rs.Metadata.Name), uid, "Orphan"); err != nil {
			return err
		}
	}
	if len(sets) > 0 {
		return errRecoveryWaiting
	}
	for _, identity := range r.SourcePods {
		p, found, e := c.getPod(ctx, r.Namespace, identity.Name)
		if e != nil {
			return e
		}
		if !found || p.ObservedUID != identity.UID {
			return fmt.Errorf("original Pod was lost before isolation")
		}
		if len(p.ObservedOwnerReferences) > 0 {
			return errRecoveryWaiting
		}
	}
	// A paused empty Deployment cannot replace the now orphaned evidence Pods.
	if err := c.patchAppWorkloadObject(ctx, path, d, map[string]any{"spec": map[string]any{"paused": true, "replicas": 0}}); err != nil {
		return err
	}
	return nil
}

func (s *Service) applyRecoveryReplacement(ctx context.Context, c *kubeClient, managed runtime.ManagedAppObject, app model.App, r *model.AppRecoveryRecord) error {
	if r.ReplacementSpec == nil {
		return fmt.Errorf("replacement snapshot missing")
	}
	replacement := app
	replacement.Spec = *r.ReplacementSpec
	replacement = s.Renderer.PrepareApp(s.appWithResolvedLaunchOverride(ctx, replacement))
	scheduling, err := s.managedSchedulingConstraintsForApp(ctx, replacement)
	if err != nil {
		return err
	}
	if err := s.preflightManagedAppZeroDowntimeRolloutCapacity(ctx, replacement, scheduling); err != nil {
		return err
	}
	placements, err := s.managedPostgresPlacements(ctx, replacement)
	if err != nil {
		return err
	}
	objects := s.Renderer.BuildManagedAppChildObjectsWithPlacements(replacement, scheduling, placements, runtime.ManagedAppOwnerReference(managed))
	filtered, err := recoveryReplacementObjects(objects, *r)
	if err != nil {
		return err
	}
	if err := s.validateAppStoragePlacement(ctx, c, replacement, scheduling, filtered); err != nil {
		return err
	}
	if err := createRecoveryObjects(ctx, c, r.Namespace, r.ID, filtered); err != nil {
		return err
	}
	d, found, err := c.getRawObject(ctx, recoveryObjectPath(r.Namespace, "deployments", r.ReplacementDeployment))
	if err != nil {
		return err
	}
	if !found || nestedObjectString(d, "metadata", "labels", recoveryLabel) != r.ID {
		return fmt.Errorf("replacement ownership unavailable")
	}
	r.ReplacementDeploymentUID = nestedObjectString(d, "metadata", "uid")
	r.Phase = "probing"
	r.Message = "source frozen; isolated replacement created; waiting for readiness and business probes"
	return nil
}

// Create without adopting or overwriting a colliding object. Retried steps may
// reuse only objects bearing this exact recovery identity. POST also closes
// the get/create race: an intervening creator causes a conflict, never a patch.
func createRecoveryObjects(ctx context.Context, c *kubeClient, namespace, recoveryID string, objects []map[string]any) error {
	sort.SliceStable(objects, func(i, j int) bool { return kubeApplyPhase(objects[i]) < kubeApplyPhase(objects[j]) })
	for _, object := range objects {
		path, err := runtime.ObjectAPIPath(namespace, object)
		if err != nil {
			return err
		}
		existing, found, err := c.getRawObject(ctx, path)
		if err != nil {
			return err
		}
		if found {
			if nestedObjectString(existing, "metadata", "labels", recoveryLabel) != recoveryID || nestedObjectString(existing, "metadata", "deletionTimestamp") != "" {
				return fmt.Errorf("recovery object name is already owned or terminating")
			}
			continue
		}
		collection := path[:strings.LastIndex(path, "/")]
		if _, err := c.doJSON(ctx, http.MethodPost, collection, object, nil); err != nil {
			return err
		}
	}
	return nil
}

func recoveryReplacementObjects(objects []map[string]any, r model.AppRecoveryRecord) ([]map[string]any, error) {
	// Renderers use both map[string]string and map[string]any. Normalize before
	// filtering so backing-service labels cannot accidentally be discarded.
	raw, err := json.Marshal(objects)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &objects); err != nil {
		return nil, err
	}
	filtered := []map[string]any{}
	for _, o := range objects {
		labels := objectMapField(objectMapField(o, "metadata"), "labels")
		if objectStringField(labels, runtime.FugueLabelBackingServiceType) != "" || objectStringField(labels, runtime.FugueLabelBackingServiceID) != "" {
			continue
		}
		// Compose aliases still select the source until the business gate passes.
		if objectStringField(o, "kind") == "Service" && nestedObjectString(o, "metadata", "name") != r.ReplacementService {
			continue
		}
		if labels == nil {
			labels = map[string]any{}
			objectMapField(o, "metadata")["labels"] = labels
		}
		labels[recoveryLabel] = r.ID
		filtered = append(filtered, o)
	}
	return filtered, nil
}

func (s *Service) recoveryReplacementReady(ctx context.Context, c *kubeClient, app model.App, r model.AppRecoveryRecord) error {
	d, found, err := c.getDeployment(ctx, r.Namespace, r.ReplacementDeployment)
	if err != nil {
		return err
	}
	if !found || d.Metadata.UID != r.ReplacementDeploymentUID {
		return fmt.Errorf("replacement deployment identity changed")
	}
	if d.Status.ObservedGeneration < d.Metadata.Generation || d.Status.ReadyReplicas != 1 || d.Status.AvailableReplicas != 1 {
		return errRecoveryWaiting
	}
	pods, err := c.listPodsBySelector(ctx, r.Namespace, runtime.FugueLabelAppWorkload+"="+r.ReplacementDeployment)
	if err != nil {
		return err
	}
	if len(pods) != 1 || pods[0].Metadata.DeletionTimestamp != "" {
		return errRecoveryWaiting
	}
	if r.CutoverAt == nil && r.ReplacementPodUID != "" && pods[0].ObservedUID != r.ReplacementPodUID {
		return fmt.Errorf("replacement Pod changed after business verification began")
	}
	if len(d.Spec.Template.Spec.Containers) == 0 || d.Spec.Template.Spec.Containers[0].Image != r.ImageRef {
		return fmt.Errorf("replacement image no longer matches captured digest")
	}
	main := d.Spec.Template.Spec.Containers[0].Name
	matched := false
	for _, cs := range pods[0].Status.ContainerStatuses {
		if cs.Name == main && cs.Ready && recoveryDigest.FindString(cs.ImageID) == recoveryDigest.FindString(r.ImageRef) {
			matched = true
		}
	}
	if !matched {
		return errRecoveryWaiting
	}
	return recoveryServiceTargetsPod(ctx, c, r.Namespace, r.ReplacementService, pods[0].ObservedUID)
}

func recoveryServiceTargetsPod(ctx context.Context, c *kubeClient, ns, name, uid string) error {
	raw, found, err := c.getRawObject(ctx, recoveryObjectPath(ns, "endpoints", name))
	if err != nil {
		return err
	}
	if !found || uid == "" {
		return errRecoveryWaiting
	}
	count := 0
	for _, subset := range mapSlice(raw["subsets"]) {
		for _, address := range mapSlice(subset["addresses"]) {
			if nestedObjectString(address, "targetRef", "uid") != uid {
				return errRecoveryWaiting
			}
			count++
		}
		if len(mapSlice(subset["notReadyAddresses"])) > 0 {
			return errRecoveryWaiting
		}
	}
	if count != 1 {
		return errRecoveryWaiting
	}
	return nil
}

func (s *Service) probeRecoveryReplacement(ctx context.Context, c *kubeClient, app model.App, r *model.AppRecoveryRecord) error {
	if err := s.recoveryReplacementReady(ctx, c, app, *r); err != nil {
		return err
	}
	if r.ReplacementPodUID == "" {
		pods, err := c.listPodsBySelector(ctx, r.Namespace, runtime.FugueLabelAppWorkload+"="+r.ReplacementDeployment)
		if err != nil {
			return err
		}
		if len(pods) != 1 {
			return errRecoveryWaiting
		}
		r.ReplacementPodUID = pods[0].ObservedUID
	}
	replacement := app
	replacement.Spec = *r.ReplacementSpec
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	evaluator := releaseflow.ReleaseGateEvaluator{HTTPClient: client}
	index := len(r.ProbeResults)
	for i, result := range r.ProbeResults {
		if result.Status != model.AppReleaseGateStatusPass {
			index = i
			r.ProbeResults = r.ProbeResults[:i]
			break
		}
	}
	if index < len(r.Request.Probes) {
		result := evaluator.RunProbe(ctx, model.AppRelease{UpstreamURL: s.controllerServiceURLForApp(ctx, replacement)}, r.Request.Probes[index])
		if result.Status != model.AppReleaseGateStatusPass {
			result.Error = "business probe failed; inspect application request logs"
			r.Message = "business probe failed; original traffic remains selected"
		}
		r.ProbeResults = append(r.ProbeResults, result)
		if result.Status != model.AppReleaseGateStatusPass {
			return errRecoveryWaiting
		}
	}
	return s.recoveryReplacementReady(ctx, c, app, *r)
}

func (s *Service) publishRecovery(ctx context.Context, c *kubeClient, app model.App, records []model.AppRecoveryRecord, r *model.AppRecoveryRecord) error {
	if r.CutoverAt == nil || r.ReplacementSpec == nil || app.Spec.WorkloadGeneration != r.ReplacementSpec.WorkloadGeneration {
		return fmt.Errorf("cutover intent is inconsistent")
	}
	if err := s.recoveryReplacementReady(ctx, c, app, *r); err != nil {
		return err
	}
	scheduling, err := s.managedSchedulingConstraintsForApp(ctx, app)
	if err != nil {
		return err
	}
	stateObjects := runtime.BuildManagedAppStateObjects(s.Renderer.PrepareApp(app), scheduling)
	if err := c.applyObjects(ctx, stateObjects); err != nil {
		return err
	}
	if err := c.replaceObjectSpecsByKind(ctx, stateObjects, runtime.ManagedAppAPIVersion, runtime.ManagedAppKind); err != nil {
		return err
	}
	for _, old := range records {
		if old.SourceService == "" || old.CutoverAt == nil {
			continue
		}
		if err := redirectRecoveryService(ctx, c, r.Namespace, old.SourceService, old.SourceServiceUID, r.ReplacementDeployment, app.ID); err != nil {
			return err
		}
	}
	// Existing internal aliases are traffic entry points too. Change their
	// selectors only after the same business gate as the public route.
	placements, err := s.managedPostgresPlacements(ctx, app)
	if err != nil {
		return err
	}
	objects := s.Renderer.BuildManagedAppChildObjectsWithPlacements(s.Renderer.PrepareApp(app), scheduling, placements, nil)
	for _, object := range objects {
		if objectStringField(object, "kind") != "Service" {
			continue
		}
		raw, err := json.Marshal(object)
		if err != nil {
			return err
		}
		var normalized map[string]any
		if err := json.Unmarshal(raw, &normalized); err != nil {
			return err
		}
		if nestedObjectString(normalized, "metadata", "labels", runtime.FugueLabelBackingServiceType) != "" || nestedObjectString(normalized, "metadata", "labels", runtime.FugueLabelBackingServiceID) != "" {
			continue
		}
		name := nestedObjectString(normalized, "metadata", "name")
		if name == r.ReplacementService {
			continue
		}
		existing, found, err := c.getRawObject(ctx, recoveryObjectPath(r.Namespace, "services", name))
		if err != nil {
			return err
		}
		if !found || nestedObjectString(existing, "metadata", "labels", runtime.FugueLabelAppID) != app.ID {
			return fmt.Errorf("internal alias ownership unavailable")
		}
		if err := redirectRecoveryService(ctx, c, r.Namespace, name, nestedObjectString(existing, "metadata", "uid"), r.ReplacementDeployment, app.ID); err != nil {
			return err
		}
	}
	pods, err := c.listPodsBySelector(ctx, r.Namespace, runtime.FugueLabelAppWorkload+"="+r.ReplacementDeployment)
	if err != nil {
		return err
	}
	if len(pods) != 1 {
		return errRecoveryWaiting
	}
	if err := recoveryServiceTargetsPod(ctx, c, r.Namespace, r.SourceService, pods[0].ObservedUID); err != nil {
		return err
	}
	if app.Route != nil {
		policy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
		if err != nil {
			return err
		}
		stable, err := s.Store.GetAppRelease(app.TenantID, true, policy.StableReleaseID)
		if err != nil {
			return err
		}
		observation, err := s.edgeBundleObserverForSafeRollout().WaitForSafeRolloutEdgeRouteBundle(ctx, app, stable, 0, *r.CutoverAt)
		if err != nil {
			return err
		}
		if !observation.Ready {
			return errRecoveryWaiting
		}
	}
	now := time.Now().UTC()
	r.Phase = "retained"
	r.Message = "business probes passed; traffic publication confirmed; original containers retained for investigation"
	if *r.Request.RetentionSeconds > 0 {
		until := now.Add(time.Duration(*r.Request.RetentionSeconds) * time.Second)
		r.RetentionUntil = &until
	}
	return nil
}

func redirectRecoveryService(ctx context.Context, c *kubeClient, ns, name, uid, workload, appID string) error {
	path := recoveryObjectPath(ns, "services", name)
	raw, found, err := c.getRawObject(ctx, path)
	if err != nil {
		return err
	}
	if !found || nestedObjectString(raw, "metadata", "uid") != uid {
		return fmt.Errorf("compatibility Service identity changed")
	}
	selector := map[string]any{runtime.FugueLabelAppID: appID, runtime.FugueLabelAppWorkload: workload}
	// Keep existing tenant/managed-by restrictions while changing only the
	// selected workload identity. Do not broaden a compatibility entry point.
	for key, value := range objectMapField(objectMapField(raw, "spec"), "selector") {
		if key != runtime.FugueLabelAppWorkload {
			selector[key] = value
		}
	}
	if normalizedKubeValueEqual(nestedObjectValue(raw, "spec", "selector"), selector) {
		return nil
	}
	objectMapField(raw, "spec")["selector"] = selector
	_, err = c.doJSON(ctx, http.MethodPut, path, raw, nil)
	return err
}

func (s *Service) observeRecoveryEvidence(ctx context.Context, c *kubeClient, r model.AppRecoveryRecord) (string, error) {
	state := r.EvidenceStatus
	if state == "pending" || state == "" {
		state = "intact"
	}
	for _, identity := range r.SourcePods {
		p, found, err := c.getPod(ctx, r.Namespace, identity.Name)
		if err != nil {
			return state, err
		}
		if !found || p.ObservedUID != identity.UID || p.Metadata.DeletionTimestamp != "" {
			return "lost", nil
		}
		statuses := map[string]kubeContainerStatus{}
		for _, cs := range append(p.Status.InitContainerStatuses, p.Status.ContainerStatuses...) {
			statuses[cs.Name] = cs
		}
		for _, cs := range identity.Containers {
			observed, ok := statuses[cs.Name]
			if !ok || cs.ID != observed.ContainerID || cs.RestartCount != observed.RestartCount {
				state = "changed"
			}
		}
	}
	return state, nil
}

func deleteRecoveryObject(ctx context.Context, c *kubeClient, path, uid, propagation string) error {
	if uid == "" {
		return fmt.Errorf("deletion requires a recorded UID")
	}
	status, err := c.doJSON(ctx, http.MethodDelete, path, map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": map[string]any{"uid": uid}, "propagationPolicy": propagation}, nil)
	if status == http.StatusNotFound {
		return nil
	}
	return err
}

func (s *Service) releaseRecoveryRuntime(ctx context.Context, c *kubeClient, app model.App, r *model.AppRecoveryRecord) error {
	if r.CutoverAt == nil {
		if r.ReplacementDeployment != "" && r.ReplacementDeploymentUID == "" {
			raw, found, err := c.getRawObject(ctx, recoveryObjectPath(r.Namespace, "deployments", r.ReplacementDeployment))
			if err != nil {
				return err
			}
			if found {
				if nestedObjectString(raw, "metadata", "labels", recoveryLabel) != r.ID {
					return fmt.Errorf("replacement ownership changed")
				}
				r.ReplacementDeploymentUID = nestedObjectString(raw, "metadata", "uid")
			}
		}
		if r.ReplacementDeploymentUID != "" {
			if err := deleteRecoveryObject(ctx, c, recoveryObjectPath(r.Namespace, "deployments", r.ReplacementDeployment), r.ReplacementDeploymentUID, "Foreground"); err != nil {
				return err
			}
			if _, found, err := c.getRawObject(ctx, recoveryObjectPath(r.Namespace, "deployments", r.ReplacementDeployment)); err != nil {
				return err
			} else if found {
				return errRecoveryWaiting
			}
		}
		if r.SourceDeploymentUID != "" {
			path := recoveryObjectPath(r.Namespace, "deployments", r.SourceDeployment)
			raw, found, err := c.getRawObject(ctx, path)
			if err != nil {
				return err
			}
			if !found || nestedObjectString(raw, "metadata", "uid") != r.SourceDeploymentUID {
				return fmt.Errorf("source deployment identity changed")
			}
			if err := c.patchAppWorkloadObject(ctx, path, raw, map[string]any{"spec": map[string]any{"paused": false, "replicas": r.SourceSpec.Replicas}}); err != nil {
				return err
			}
		}
	} else {
		if err := s.recoveryReplacementReady(ctx, c, app, *r); err != nil {
			return err
		}
		for _, p := range r.SourcePods {
			if err := deleteRecoveryObject(ctx, c, recoveryObjectPath(r.Namespace, "pods", p.Name), p.UID, "Background"); err != nil {
				return err
			}
			if _, found, err := c.getRawObject(ctx, recoveryObjectPath(r.Namespace, "pods", p.Name)); err != nil {
				return err
			} else if found {
				return errRecoveryWaiting
			}
		}
		if err := deleteRecoveryObject(ctx, c, recoveryObjectPath(r.Namespace, "deployments", r.SourceDeployment), r.SourceDeploymentUID, "Foreground"); err != nil {
			return err
		}
	}
	r.Active = false
	r.Phase = "released"
	r.EvidenceStatus = "released"
	r.Message = "investigation hold released; persistent volumes retained for explicit storage administration"
	return nil
}

package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const runtimeStateMaxPods = 16
const runtimeStateMaxFiles = 32
const runtimeStateMaxRead = 1 << 20

func runtimeStateHash(value any) string {
	raw, _ := json.Marshal(value)
	return runtimeStateBytesHash(raw)
}
func runtimeStateBytesHash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func stateComparison(kind, key, pod, source string, desired, observed any) model.AppRuntimeCheck {
	a, b := runtimeStateHash(desired), runtimeStateHash(observed)
	state := "drifted"
	if a == b {
		state = "in_sync"
	}
	return model.AppRuntimeCheck{Kind: kind, Key: key, Pod: pod, Source: source, State: state, DesiredSHA256: a, ObservedSHA256: b}
}
func unknownRuntimeCheck(kind, key, pod, source, reason string) model.AppRuntimeCheck {
	return model.AppRuntimeCheck{Kind: kind, Key: key, Pod: pod, Source: source, State: "unknown", Reason: reason}
}

func (s *Server) handleGetAppRuntimeState(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}
	result := model.AppRuntimeState{SchemaVersion: 1, AppID: app.ID, Namespace: runtime.NamespaceForTenant(app.TenantID), DesiredSpecHash: model.AppSpecSHA256(app.Spec), DesiredSource: "committed_app_spec", ObservedAt: time.Now().UTC(), State: "inconclusive", ReadyPods: []string{}, ServingPods: []string{}, PodUIDs: map[string]string{}, Revisions: map[string]string{}, PendingOperations: []string{}, Checks: []model.AppRuntimeCheck{}, MissingEvidence: []string{}}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	operations, err := s.store.ListOperationsWithDesiredSourceByApp(app.TenantID, true, app.ID)
	if err != nil {
		result.MissingEvidence = append(result.MissingEvidence, "pending_operations")
	} else {
		for _, op := range operations {
			if op.Status == model.OperationStatusPending || op.Status == model.OperationStatusRunning || op.Status == model.OperationStatusWaitingAgent {
				result.PendingOperations = append(result.PendingOperations, op.ID)
			}
		}
	}
	runtimeObj, runtimeErr := s.store.GetRuntime(app.Spec.RuntimeID)
	if runtimeErr != nil || runtimeObj.Type == model.RuntimeTypeExternalOwned {
		result.MissingEvidence = append(result.MissingEvidence, "managed_runtime_state")
		httpx.WriteJSON(w, 200, result)
		return
	}
	logClient, err := s.newLogsClient(result.Namespace)
	if err != nil {
		result.MissingEvidence = append(result.MissingEvidence, "runtime_client")
		httpx.WriteJSON(w, 200, result)
		return
	}
	selector, container, err := runtimeLogTarget(app, "app")
	if err != nil {
		httpx.WriteError(w, 400, err.Error())
		return
	}
	pods, err := logClient.listPodsBySelector(ctx, result.Namespace, selector)
	if err != nil {
		result.MissingEvidence = append(result.MissingEvidence, "runtime_pods")
		httpx.WriteJSON(w, 200, result)
		return
	}
	releases, releaseErr := s.store.ListAppReleases(model.AppReleaseFilter{AppID: app.ID, TenantID: app.TenantID, PlatformAdmin: true})
	releaseByID := map[string]model.AppRelease{}
	if releaseErr != nil {
		result.MissingEvidence = append(result.MissingEvidence, "release_spec_snapshots")
	} else {
		for _, release := range releases {
			releaseByID[release.ID] = release
		}
	}
	ready := []kubePodInfo{}
	for _, pod := range pods {
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == container && status.Ready {
				ready = append(ready, pod)
				result.ReadyPods = append(result.ReadyPods, pod.Metadata.Name)
				result.PodUIDs[pod.Metadata.Name] = pod.Metadata.UID
				result.Revisions[pod.Metadata.Name] = pod.Metadata.Labels["pod-template-hash"]
				break
			}
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].Metadata.Name < ready[j].Metadata.Name })
	sort.Strings(result.ReadyPods)
	expectedByRelease := map[string]int{}
	if len(ready) == 0 {
		expectedByRelease[""] = app.Spec.Replicas
	}
	services := map[string]bool{}
	if model.AppHasClusterService(app.Spec) {
		services[runtime.RuntimeAppServiceName(app)] = true
	}
	if len(ready) > runtimeStateMaxPods {
		ready = ready[:runtimeStateMaxPods]
		result.MissingEvidence = append(result.MissingEvidence, "pod_inspection_limit")
	}
	for _, pod := range ready {
		spec := app.Spec
		releaseID := pod.Metadata.Labels[runtime.FugueLabelAppReleaseID]
		if releaseID != "" {
			release, found := releaseByID[releaseID]
			if !found || release.SpecSnapshot == nil {
				result.Checks = append(result.Checks, unknownRuntimeCheck("release_spec", "", pod.Metadata.Name, "release_snapshot", "release has no immutable spec snapshot"))
				continue
			}
			spec = *release.SpecSnapshot
			expectedByRelease[releaseID] = spec.Replicas

			if release.ServiceName != "" {
				services[release.ServiceName] = true
			}
		}
		if releaseID == "" {
			expectedByRelease[""] = app.Spec.Replicas
		}
		env := stripFugueInjectedAppEnvDetails(mergedAppEnvDetails(app, spec)).Env
		digest := s.runtimeStateExpectedDigest(app, spec)
		result.Checks = append(result.Checks, s.inspectRuntimePod(ctx, result.Namespace, container, pod, spec, env, digest)...)
	}
	expectedReplicas := 0
	for _, count := range expectedByRelease {
		expectedReplicas += count
	}
	result.Checks = append(result.Checks, stateComparison("replicas", "", "", "ready_pod_inventory", expectedReplicas, len(result.ReadyPods)))
	if len(services) > 0 {
		serving, err := runtimeStateServingPods(ctx, logClient, result.Namespace, services, result.PodUIDs)
		if err != nil {
			result.Checks = append(result.Checks, unknownRuntimeCheck("serving_endpoints", "", "", "kubernetes_endpoints", "endpoint identity evidence unavailable"))
		} else {
			result.ServingPods = serving
			state := "in_sync"
			if app.Spec.Replicas > 0 && len(serving) == 0 {
				state = "drifted"
			}
			result.Checks = append(result.Checks, model.AppRuntimeCheck{Kind: "serving_endpoints", State: state, Source: "kubernetes_endpoint_pod_uids"})
		}
	}
	current, err := s.store.GetApp(app.ID)
	if err != nil || model.AppSpecSHA256(current.Spec) != result.DesiredSpecHash {
		result.MissingEvidence = append(result.MissingEvidence, "stable_intent_during_observation")
	}
	result.State = summarizeRuntimeState(result)
	if result.State == "in_sync" && app.Spec.Replicas == 0 {
		result.State = "inactive"
	}
	s.appendAudit(principal, "app.runtime_state.read", "app", app.ID, app.TenantID, map[string]string{"state": result.State})
	httpx.WriteJSON(w, http.StatusOK, result)
}
func (s *Server) runtimeStateExpectedDigest(app model.App, spec model.AppSpec) string {
	if at := strings.LastIndex(spec.Image, "@sha256:"); at >= 0 {
		return store.CanonicalImageDigest(spec.Image[at+1:])
	}
	images, err := s.store.ListImages(model.ImageFilter{TenantID: app.TenantID, AppID: app.ID, ImageRef: spec.Image})
	if err != nil {
		return ""
	}
	digest := ""
	for _, item := range images {
		if item.CanonicalDigest == "" {
			continue
		}
		if digest != "" && digest != item.CanonicalDigest {
			return ""
		}
		digest = item.CanonicalDigest
	}
	return digest
}
func (s *Server) readRuntimeStateFile(ctx context.Context, namespace, pod, container, path string) ([]byte, error) {
	if s.filesystemExecRunner == nil {
		return nil, fmt.Errorf("runtime reader unavailable")
	}
	raw, err := s.filesystemExecRunner.Run(ctx, namespace, pod, container, nil, "head", "-c", "1048577", path)
	if err != nil {
		return nil, err
	}
	if len(raw) > runtimeStateMaxRead {
		return nil, fmt.Errorf("runtime evidence exceeds size limit")
	}
	return raw, nil
}
func (s *Server) inspectRuntimePod(ctx context.Context, namespace, container string, pod kubePodInfo, spec model.AppSpec, desiredEnv map[string]string, digest string) []model.AppRuntimeCheck {
	checks := []model.AppRuntimeCheck{}
	if desiredEnv == nil {
		desiredEnv = map[string]string{}
	}
	name := pod.Metadata.Name
	imageID := ""
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == container {
			imageID = status.ImageID
		}
	}
	if at := strings.LastIndex(imageID, "sha256:"); at >= 0 {
		imageID = imageID[at:]
	}
	if digest == "" || imageID == "" {
		checks = append(checks, unknownRuntimeCheck("image", "", name, "container_status", "immutable desired digest or running image ID unavailable"))
	} else {
		checks = append(checks, stateComparison("image", "", name, "container_status", digest, imageID))
	}
	envRaw, err := s.readRuntimeStateFile(ctx, namespace, name, container, "/proc/1/environ")
	if err != nil {
		checks = append(checks, unknownRuntimeCheck("env", "", name, "process_environ", "process environment unavailable"))
	} else {
		actual := map[string]string{}
		for _, entry := range strings.Split(string(envRaw), "\x00") {
			key, value, ok := strings.Cut(entry, "=")
			if ok {
				actual[key] = value
			}
		}
		observed := map[string]string{}
		missing := false
		for key := range desiredEnv {
			value, found := actual[key]
			if !found {
				missing = true
			} else {
				observed[key] = value
			}
		}
		check := stateComparison("env", "", name, "process_environ", desiredEnv, observed)
		if missing {
			check.State = "drifted"
		}
		checks = append(checks, check)
	}
	desiredCommand := append(append([]string{}, spec.Command...), spec.Args...)
	if len(desiredCommand) == 0 {
		checks = append(checks, model.AppRuntimeCheck{Kind: "command", Pod: name, State: "not_configured", Source: "image_entrypoint", Reason: "command comes from the immutable image"})
	} else {
		raw, err := s.readRuntimeStateFile(ctx, namespace, name, container, "/proc/1/cmdline")
		if err != nil {
			checks = append(checks, unknownRuntimeCheck("command", "", name, "process_cmdline", "process command unavailable"))
		} else {
			actual := strings.Split(strings.TrimRight(string(raw), "\x00"), "\x00")
			check := stateComparison("command", "", name, "process_cmdline", desiredCommand, actual)
			if check.State == "drifted" && (strings.HasSuffix(desiredCommand[0], "sh")) {
				check.State = "unknown"
				check.Reason = "entrypoint shell may transform process arguments"
			}
			checks = append(checks, check)
		}
	}
	paths := []string{}
	if spec.Workspace != nil && spec.Workspace.StoragePath != "" {
		paths = append(paths, spec.Workspace.StoragePath)
	}
	if spec.PersistentStorage != nil {
		for _, mount := range spec.PersistentStorage.Mounts {
			if mount.Path != "" {
				paths = append(paths, mount.Path)
			}
		}
	}
	if len(paths) == 0 {
		checks = append(checks, model.AppRuntimeCheck{Kind: "mounts", Pod: name, State: "not_configured", Source: "app_spec"})
	} else {
		raw, err := s.readRuntimeStateFile(ctx, namespace, name, container, "/proc/1/mountinfo")
		if err != nil {
			checks = append(checks, unknownRuntimeCheck("mounts", "", name, "process_mountinfo", "mount information unavailable"))
		} else {
			mounts := map[string]bool{}
			for _, line := range strings.Split(string(raw), "\n") {
				fields := strings.Fields(line)
				if len(fields) > 4 {
					mounts[strings.NewReplacer(`\040`, " ", `\011`, "\t", `\134`, `\`).Replace(fields[4])] = true
				}
			}
			for _, path := range paths {
				checks = append(checks, stateComparison("mount", path, name, "process_mountinfo", true, mounts[path]))
			}
		}
	}
	if len(spec.Files) == 0 {
		checks = append(checks, model.AppRuntimeCheck{Kind: "files", Pod: name, State: "not_configured", Source: "app_spec"})
	}
	for i, file := range spec.Files {
		if i >= runtimeStateMaxFiles {
			checks = append(checks, unknownRuntimeCheck("files", "", name, "runtime_file", "file inspection limit reached"))
			break
		}
		if !strings.HasPrefix(file.Path, "/") || strings.ContainsRune(file.Path, 0) {
			checks = append(checks, unknownRuntimeCheck("file", file.Path, name, "runtime_file", "invalid desired file path"))
			continue
		}
		raw, err := s.readRuntimeStateFile(ctx, namespace, name, container, file.Path)
		if err != nil {
			checks = append(checks, unknownRuntimeCheck("file", file.Path, name, "runtime_file", "file could not be read"))
		} else {
			desired, observed := runtimeStateBytesHash([]byte(file.Content)), runtimeStateBytesHash(raw)
			state := "in_sync"
			if desired != observed {
				state = "drifted"
			}
			checks = append(checks, model.AppRuntimeCheck{Kind: "file", Key: file.Path, Pod: name, State: state, Source: "runtime_file", DesiredSHA256: desired, ObservedSHA256: observed})
		}
	}
	return checks
}
func summarizeRuntimeState(state model.AppRuntimeState) string {
	if len(state.MissingEvidence) > 0 || len(state.PendingOperations) > 0 {
		return "inconclusive"
	}
	drift, unknown := false, false
	for _, check := range state.Checks {
		drift = drift || check.State == "drifted"
		unknown = unknown || check.State == "unknown"
	}
	if unknown {
		return "inconclusive"
	}
	if drift {
		return "drifted"
	}
	return "in_sync"
}
func runtimeStateServingPods(ctx context.Context, client appLogsClient, namespace string, services map[string]bool, podUIDs map[string]string) ([]string, error) {
	names := map[string]bool{}
	add := func(name, uid string) error {
		expected, found := podUIDs[name]
		if !found || uid == "" || expected == "" || uid != expected {
			return fmt.Errorf("endpoint identity does not match a ready pod")
		}
		names[name] = true
		return nil
	}
	for service := range services {
		found := false
		if slices, ok := client.(endpointSliceTimelineClient); ok {
			items, err := slices.listEndpointSlicesForService(ctx, namespace, service)
			if err != nil {
				return nil, err
			}
			found = len(items) > 0
			for _, slice := range items {
				for _, endpoint := range slice.Endpoints {
					if endpoint.Conditions.Ready == nil || !*endpoint.Conditions.Ready {
						continue
					}
					if endpoint.TargetRef == nil || endpoint.TargetRef.Kind != "Pod" {
						return nil, fmt.Errorf("endpoint has no pod identity")
					}
					if err := add(endpoint.TargetRef.Name, string(endpoint.TargetRef.UID)); err != nil {
						return nil, err
					}
				}
			}
		}
		if !found {
			legacy, ok := client.(endpointTimelineClient)
			if !ok {
				return nil, fmt.Errorf("endpoint observation is unavailable")
			}
			endpoints, exists, err := legacy.getEndpointsForService(ctx, namespace, service)
			if err != nil {
				return nil, err
			}
			if !exists {
				continue
			}
			for _, subset := range endpoints.Subsets {
				for _, address := range subset.Addresses {
					if address.TargetRef == nil || address.TargetRef.Kind != "Pod" {
						return nil, fmt.Errorf("endpoint has no pod identity")
					}
					if err := add(address.TargetRef.Name, string(address.TargetRef.UID)); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	result := []string{}
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result, nil
}

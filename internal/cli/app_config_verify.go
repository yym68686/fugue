package cli

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"
	fugueruntime "fugue/internal/runtime"

	"github.com/spf13/cobra"
)

const (
	appConfigVerifySchemaVersion    = "fugue.app-config-verify.v1"
	appConfigReconcileSchemaVersion = "fugue.app-config-reconcile.v1"
	defaultConfigVerifyMaxBytes     = 1024 * 1024
	defaultConfigSwitchWait         = 2 * time.Minute
)

var deployedRevisionPattern = regexp.MustCompile(`(?i)\brevision\s+(\d+)\b`)

type appConfigVerifyOptions struct {
	Source   string
	Target   string
	Pod      string
	Assert   bool
	MaxBytes int
}

type appConfigReconcileOptions struct {
	appConfigVerifyOptions
	Wait    bool
	Timeout time.Duration
}

type appConfigDesiredFile struct {
	Source          string `json:"source"`
	SpecSource      string `json:"spec_source,omitempty"`
	OperationID     string `json:"operation_id,omitempty"`
	OperationStatus string `json:"operation_status,omitempty"`
	Path            string `json:"path"`
	Revision        string `json:"revision,omitempty"`
	SHA256          string `json:"sha256,omitempty"`
	SizeBytes       int    `json:"size_bytes,omitempty"`
	Found           bool   `json:"found"`
}

type appConfigLiveFile struct {
	Source    string `json:"source"`
	Pod       string `json:"pod,omitempty"`
	Revision  string `json:"revision,omitempty"`
	Path      string `json:"path"`
	SHA256    string `json:"sha256,omitempty"`
	SizeBytes int    `json:"size_bytes,omitempty"`
	Match     bool   `json:"match,omitempty"`
	Error     string `json:"error,omitempty"`
}

type appConfigMount struct {
	Path   string `json:"path"`
	Source string `json:"source,omitempty"`
	Name   string `json:"name,omitempty"`
	Detail string `json:"detail,omitempty"`
}

type appConfigStateDiff struct {
	DesiredImage    string            `json:"desired_image,omitempty"`
	LiveImage       string            `json:"live_image,omitempty"`
	ImageInSync     bool              `json:"image_in_sync"`
	DesiredCommand  []string          `json:"desired_command,omitempty"`
	LiveCommand     []string          `json:"live_command,omitempty"`
	CommandInSync   bool              `json:"command_in_sync"`
	DesiredEnvSHA   string            `json:"desired_env_sha256,omitempty"`
	LiveEnvSHA      string            `json:"live_env_sha256,omitempty"`
	DesiredEnv      map[string]string `json:"desired_env,omitempty"`
	LiveEnv         map[string]string `json:"live_env,omitempty"`
	MissingEnvKeys  []string          `json:"missing_env_keys,omitempty"`
	ExtraEnvKeys    []string          `json:"extra_env_keys,omitempty"`
	ChangedEnvKeys  []string          `json:"changed_env_keys,omitempty"`
	EnvInSync       bool              `json:"env_in_sync"`
	DesiredMounts   []appConfigMount  `json:"desired_mounts,omitempty"`
	LiveMounts      []appConfigMount  `json:"live_mounts,omitempty"`
	MissingMounts   []string          `json:"missing_mounts,omitempty"`
	ExtraMounts     []string          `json:"extra_mounts,omitempty"`
	MountsInSync    bool              `json:"mounts_in_sync"`
	StateObserved   bool              `json:"state_observed"`
	StateInSync     bool              `json:"state_in_sync"`
	DesiredRevision string            `json:"desired_revision,omitempty"`
	LiveRevisions   []string          `json:"live_revisions,omitempty"`
}

type appConfigVerifyResult struct {
	SchemaVersion   string                      `json:"schema_version"`
	App             string                      `json:"app"`
	AppID           string                      `json:"app_id"`
	Path            string                      `json:"path"`
	Source          string                      `json:"source"`
	Target          string                      `json:"target"`
	Namespace       string                      `json:"namespace,omitempty"`
	Deployment      string                      `json:"deployment,omitempty"`
	Service         *model.ClusterServiceDetail `json:"service,omitempty"`
	RevisionChecks  map[string]string           `json:"revision_checksums,omitempty"`
	Desired         appConfigDesiredFile        `json:"desired"`
	Live            []appConfigLiveFile         `json:"live"`
	ServingPods     []string                    `json:"serving_pods,omitempty"`
	ReadyPods       []string                    `json:"ready_pods,omitempty"`
	ReplicaDrift    bool                        `json:"replica_drift"`
	FileInSync      bool                        `json:"file_in_sync"`
	State           appConfigStateDiff          `json:"state"`
	InSync          bool                        `json:"in_sync"`
	AssertionPassed bool                        `json:"assertion_passed"`
	ConclusionCode  string                      `json:"conclusion_code"`
	Conclusion      string                      `json:"conclusion"`
	NextActions     []string                    `json:"next_actions,omitempty"`
	Warnings        []string                    `json:"warnings,omitempty"`
}

type appConfigReconcileResult struct {
	SchemaVersion    string                `json:"schema_version"`
	App              string                `json:"app"`
	AppID            string                `json:"app_id"`
	Operation        *model.Operation      `json:"operation,omitempty"`
	RestartToken     string                `json:"restart_token,omitempty"`
	ExpectedRevision string                `json:"expected_revision,omitempty"`
	Verification     appConfigVerifyResult `json:"verification"`
}

type appConfigStateObservation struct {
	Image      string
	Command    []string
	Env        map[string]string
	Mounts     []appConfigMount
	Checksums  map[string]string
	Deployment string
}

type appConfigContainerTemplate struct {
	Image   string
	Command []string
	Env     map[string]string
	Mounts  []appConfigMount
}

type desiredSpecSelection struct {
	Spec            model.AppSpec
	Source          string
	OperationID     string
	OperationStatus string
}

func (c *CLI) newFilesVerifyCommand() *cobra.Command {
	opts := appConfigVerifyOptions{
		Source:   "auto",
		Target:   "serving",
		MaxBytes: defaultConfigVerifyMaxBytes,
	}
	cmd := &cobra.Command{
		Use:   "verify <app> <absolute-path>",
		Short: "Compare one declarative file with the live runtime and summarize config drift",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveWorkspaceApp(client, args[0])
			if err != nil {
				return err
			}
			result, runErr := c.runAppConfigVerify(client, app, args[1], opts)
			sanitized := sanitizeAppConfigVerifyResult(result, c.shouldRedact())
			if c.wantsJSON() {
				if err := writeJSON(c.stdout, sanitized); err != nil {
					return err
				}
			} else {
				if err := renderAppConfigVerifyResult(c.stdout, sanitized); err != nil {
					return err
				}
			}
			if runErr != nil {
				return runErr
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.Source, "source", opts.Source, "Filesystem source: auto, persistent, or live")
	cmd.Flags().StringVar(&opts.Target, "target", opts.Target, "Verification target: serving or ready")
	cmd.Flags().StringVar(&opts.Pod, "pod", "", "Specific pod name")
	cmd.Flags().BoolVar(&opts.Assert, "assert", false, "Exit non-zero when desired and live file checksums do not match")
	cmd.Flags().IntVar(&opts.MaxBytes, "max-bytes", opts.MaxBytes, "Maximum bytes to read before checksuming a live file")
	return cmd
}

func (c *CLI) newFilesReconcileCommand() *cobra.Command {
	opts := appConfigReconcileOptions{
		appConfigVerifyOptions: appConfigVerifyOptions{
			Source:   "auto",
			Target:   "ready",
			Assert:   true,
			MaxBytes: defaultConfigVerifyMaxBytes,
		},
		Wait:    true,
		Timeout: defaultConfigSwitchWait,
	}
	cmd := &cobra.Command{
		Use:   "reconcile <app> <absolute-path>",
		Short: "Force a redeploy of the current desired config, wait for the new revision, and verify live file checksums",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveWorkspaceApp(client, args[0])
			if err != nil {
				return err
			}
			result, runErr := c.runAppConfigReconcile(client, app, args[1], opts)
			sanitized := sanitizeAppConfigReconcileResult(result, c.shouldRedact())
			if c.wantsJSON() {
				if err := writeJSON(c.stdout, sanitized); err != nil {
					return err
				}
			} else {
				if err := renderAppConfigReconcileResult(c.stdout, sanitized); err != nil {
					return err
				}
			}
			if runErr != nil {
				return runErr
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.Source, "source", opts.Source, "Filesystem source: auto, persistent, or live")
	cmd.Flags().StringVar(&opts.Target, "target", opts.Target, "Validation target after reconcile: serving or ready")
	cmd.Flags().StringVar(&opts.Pod, "pod", "", "Specific pod name to validate")
	cmd.Flags().BoolVar(&opts.Wait, "wait", opts.Wait, "Wait until the new revision is ready and service endpoints have switched")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", opts.Timeout, "Maximum time to wait for service endpoints to switch to the new revision")
	cmd.Flags().IntVar(&opts.MaxBytes, "max-bytes", opts.MaxBytes, "Maximum bytes to read before checksuming a live file")
	return cmd
}

func (c *CLI) runAppConfigReconcile(client *Client, app model.App, rawPath string, opts appConfigReconcileOptions) (appConfigReconcileResult, error) {
	response, err := client.RestartApp(app.ID)
	if err != nil {
		return appConfigReconcileResult{}, err
	}

	expectedRevision := revisionFromOperationMessage(response.Operation.ResultMessage)
	finalOp := response.Operation
	if opts.Wait {
		finalOps, err := c.waitForOperations(client, []model.Operation{response.Operation})
		if err != nil {
			return appConfigReconcileResult{
				SchemaVersion: appConfigReconcileSchemaVersion,
				App:           strings.TrimSpace(app.Name),
				AppID:         strings.TrimSpace(app.ID),
				Operation:     &response.Operation,
				RestartToken:  strings.TrimSpace(response.RestartToken),
			}, err
		}
		if len(finalOps) > 0 {
			finalOp = finalOps[0]
		}
		if expectedRevision == "" {
			expectedRevision = revisionFromOperationMessage(finalOp.ResultMessage)
		}
		if waitErr := c.waitForAppConfigRevision(client, app, expectedRevision, opts.Timeout); waitErr != nil {
			result := appConfigReconcileResult{
				SchemaVersion:    appConfigReconcileSchemaVersion,
				App:              strings.TrimSpace(app.Name),
				AppID:            strings.TrimSpace(app.ID),
				Operation:        &finalOp,
				RestartToken:     strings.TrimSpace(response.RestartToken),
				ExpectedRevision: expectedRevision,
			}
			return result, withExitCode(waitErr, ExitCodeSystemFault)
		}
	}

	verifyResult, verifyErr := c.runAppConfigVerify(client, app, rawPath, opts.appConfigVerifyOptions)
	result := appConfigReconcileResult{
		SchemaVersion:    appConfigReconcileSchemaVersion,
		App:              strings.TrimSpace(app.Name),
		AppID:            strings.TrimSpace(app.ID),
		Operation:        &finalOp,
		RestartToken:     strings.TrimSpace(response.RestartToken),
		ExpectedRevision: expectedRevision,
		Verification:     verifyResult,
	}
	if verifyErr != nil {
		return result, verifyErr
	}
	if !verifyResult.AssertionPassed {
		return result, withExitCode(fmt.Errorf("reconcile validation failed: %s", strings.TrimSpace(verifyResult.Conclusion)), ExitCodeSystemFault)
	}
	return result, nil
}

func (c *CLI) waitForAppConfigRevision(client *Client, app model.App, expectedRevision string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultConfigSwitchWait
	}
	deadline := time.Now().Add(timeout)
	for {
		inventory, err := client.GetAppRuntimePods(app.ID, "app")
		if err != nil {
			return err
		}
		if expectedRevision == "" {
			expectedRevision = latestRuntimeRevision(inventory.Groups)
		}
		revisions := readyPodRevisions(inventory)
		if len(revisions) > 0 && expectedRevision != "" && allStringsEqual(revisions, expectedRevision) {
			namespace := strings.TrimSpace(inventory.Namespace)
			service, serviceErr := c.tryLoadClusterService(client, app, namespace)
			if serviceErr != nil {
				return nil
			}
			endpointRevisions := endpointPodRevisions(service, inventory)
			if len(endpointRevisions) > 0 && allStringsEqual(endpointRevisions, expectedRevision) {
				return nil
			}
		}
		if time.Now().After(deadline) {
			if expectedRevision == "" {
				return fmt.Errorf("timed out waiting for runtime pods to converge on one ready revision")
			}
			return fmt.Errorf("timed out waiting for service endpoints to switch to revision %s", expectedRevision)
		}
		time.Sleep(2 * time.Second)
	}
}

func (c *CLI) runAppConfigVerify(client *Client, app model.App, rawPath string, opts appConfigVerifyOptions) (appConfigVerifyResult, error) {
	target, err := normalizeAppConfigTarget(opts.Target, opts.Pod)
	if err != nil {
		return appConfigVerifyResult{}, withExitCode(err, ExitCodeUserInput)
	}
	requestPath, err := resolveFilesystemPathForCLI(app, rawPath, false, opts.Source)
	if err != nil {
		return appConfigVerifyResult{}, withExitCode(err, ExitCodeUserInput)
	}

	result := appConfigVerifyResult{
		SchemaVersion: appConfigVerifySchemaVersion,
		App:           strings.TrimSpace(app.Name),
		AppID:         strings.TrimSpace(app.ID),
		Path:          requestPath,
		Source:        strings.TrimSpace(opts.Source),
		Target:        target,
		Desired: appConfigDesiredFile{
			Source: "desired",
			Path:   requestPath,
		},
		Live:        []appConfigLiveFile{},
		ReadyPods:   []string{},
		ServingPods: []string{},
		State: appConfigStateDiff{
			DesiredCommand: []string{},
			LiveCommand:    []string{},
			DesiredEnv:     map[string]string{},
			LiveEnv:        map[string]string{},
			DesiredMounts:  []appConfigMount{},
			LiveMounts:     []appConfigMount{},
		},
		Warnings: []string{},
	}

	operations, err := client.ListOperationsWithDesiredState(app.ID)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("operations unavailable: %v", err))
	}
	selected := desiredSpecForVerification(app, operations)
	result.Desired.SpecSource = selected.Source
	result.Desired.OperationID = selected.OperationID
	result.Desired.OperationStatus = selected.OperationStatus

	desiredRevision := ""
	inventory, inventoryErr := client.GetAppRuntimePods(app.ID, "app")
	if inventoryErr != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("runtime pod inventory unavailable: %v", inventoryErr))
	} else {
		result.Namespace = strings.TrimSpace(inventory.Namespace)
		result.ReadyPods = readyPodNames(inventory)
		desiredRevision = latestRuntimeRevision(inventory.Groups)
		result.State.DesiredRevision = desiredRevision
		result.State.LiveRevisions = uniqueSortedStrings(readyPodRevisions(inventory))
		result.ServingPods, result.Service, err = c.loadServingPodsForVerification(client, app, inventory)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("service endpoints unavailable: %v", err))
		}
		result.Deployment = deploymentNameFromInventory(inventory)
	}
	result.Desired.Revision = desiredRevision

	desiredContent, desiredMode, found := desiredFileFromSpec(selected.Spec, requestPath)
	result.Desired.Found = found
	if found {
		desiredBytes := []byte(desiredContent)
		result.Desired.SHA256 = appConfigSHA256Hex(desiredBytes)
		result.Desired.SizeBytes = len(desiredBytes)
		_ = desiredMode
	}

	envResponse, envErr := client.GetAppEnv(app.ID)
	if envErr != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("effective env unavailable: %v", envErr))
	} else {
		result.State.DesiredEnv = cloneStringMap(envResponse.Env)
		result.State.DesiredEnvSHA = sha256StringMap(envResponse.Env)
	}
	result.State.DesiredImage = strings.TrimSpace(selected.Spec.Image)
	result.State.DesiredCommand = normalizeCommandSummary(selected.Spec.Command, selected.Spec.Args)
	result.State.DesiredMounts = desiredMountsFromSpec(selected.Spec)

	if inventoryErr == nil {
		observation, obsErr := c.observeLiveAppConfigState(client, app, inventory)
		if obsErr != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("runtime workload state unavailable: %v", obsErr))
		} else {
			result.Deployment = firstNonEmptyTrimmed(result.Deployment, observation.Deployment)
			result.RevisionChecks = cloneStringMap(observation.Checksums)
			result.State.LiveImage = observation.Image
			result.State.LiveCommand = append([]string(nil), observation.Command...)
			result.State.LiveEnv = cloneStringMap(observation.Env)
			result.State.LiveEnvSHA = sha256StringMap(observation.Env)
			result.State.LiveMounts = append([]appConfigMount(nil), observation.Mounts...)
			result.State.StateObserved = true
		}
	}

	targetPods := verifyTargetPods(target, strings.TrimSpace(opts.Pod), result.ServingPods, result.ReadyPods)
	if len(targetPods) == 0 && inventoryErr == nil {
		result.Warnings = append(result.Warnings, "no target pods matched the requested verification scope")
	}
	revisionByPod := podRevisionMap(inventory)
	for _, podName := range targetPods {
		snapshot := appConfigLiveFile{
			Source:   "live",
			Pod:      podName,
			Revision: revisionByPod[podName],
			Path:     requestPath,
		}
		response, readErr := client.GetAppFilesystemFile(app.ID, "app", requestPath, podName, opts.MaxBytes)
		if readErr != nil {
			snapshot.Error = readErr.Error()
			result.Live = append(result.Live, snapshot)
			continue
		}
		if response.Truncated {
			snapshot.Error = "live file was truncated; increase --max-bytes for checksum comparison"
			snapshot.SizeBytes = int(response.Size)
			result.Live = append(result.Live, snapshot)
			continue
		}
		liveBytes, decodeErr := decodeFilesystemContent(response.Content, response.Encoding)
		if decodeErr != nil {
			snapshot.Error = decodeErr.Error()
			result.Live = append(result.Live, snapshot)
			continue
		}
		snapshot.SizeBytes = len(liveBytes)
		snapshot.SHA256 = appConfigSHA256Hex(liveBytes)
		snapshot.Match = found && snapshot.SHA256 == result.Desired.SHA256
		result.Live = append(result.Live, snapshot)
	}

	result.ReplicaDrift = detectReplicaDrift(result.Live)
	result.FileInSync = result.Desired.Found && !result.ReplicaDrift && allLiveSnapshotsMatch(result.Live, result.Desired.SHA256)
	if result.State.StateObserved {
		result.State.ImageInSync = strings.TrimSpace(result.State.DesiredImage) == strings.TrimSpace(result.State.LiveImage)
		result.State.CommandInSync = normalizedStringSlicesEqual(result.State.DesiredCommand, result.State.LiveCommand)
		result.State.MissingEnvKeys, result.State.ExtraEnvKeys, result.State.ChangedEnvKeys = diffStringMaps(result.State.DesiredEnv, result.State.LiveEnv)
		result.State.EnvInSync = len(result.State.MissingEnvKeys) == 0 && len(result.State.ExtraEnvKeys) == 0 && len(result.State.ChangedEnvKeys) == 0
		result.State.MissingMounts, result.State.ExtraMounts = diffMountSets(result.State.DesiredMounts, result.State.LiveMounts)
		result.State.MountsInSync = len(result.State.MissingMounts) == 0 && len(result.State.ExtraMounts) == 0
		result.State.StateInSync = result.State.ImageInSync && result.State.CommandInSync && result.State.EnvInSync && result.State.MountsInSync
	}
	result.InSync = result.FileInSync && (!result.State.StateObserved || result.State.StateInSync)
	result.AssertionPassed = result.FileInSync
	finalizeAppConfigVerifyResult(&result)

	if result.ConclusionCode == "in_sync" {
		return result, nil
	}
	if result.ConclusionCode == "inconclusive" {
		return result, withExitCode(errors.New(result.Conclusion), ExitCodeIndeterminate)
	}
	return result, withExitCode(errors.New(result.Conclusion), ExitCodeSystemFault)
}

func (c *CLI) observeLiveAppConfigState(client *Client, app model.App, inventory model.AppRuntimePodInventory) (appConfigStateObservation, error) {
	namespace := strings.TrimSpace(inventory.Namespace)
	deploymentName := deploymentNameFromInventory(inventory)
	if deploymentName == "" {
		return appConfigStateObservation{}, fmt.Errorf("no deployment owner is visible in the runtime pod inventory")
	}
	workload, err := client.GetClusterWorkload(namespace, "deployment", deploymentName)
	if err != nil {
		return appConfigStateObservation{}, err
	}
	containerName := strings.TrimSpace(inventory.Container)
	template, err := workloadContainerTemplate(workload.Manifest, containerName)
	if err != nil {
		return appConfigStateObservation{}, err
	}
	return appConfigStateObservation{
		Image:      strings.TrimSpace(template.Image),
		Command:    append([]string(nil), template.Command...),
		Env:        cloneStringMap(template.Env),
		Mounts:     append([]appConfigMount(nil), template.Mounts...),
		Checksums:  extractRevisionChecksums(workload.Manifest),
		Deployment: deploymentName,
	}, nil
}

func (c *CLI) loadServingPodsForVerification(client *Client, app model.App, inventory model.AppRuntimePodInventory) ([]string, *model.ClusterServiceDetail, error) {
	namespace := strings.TrimSpace(inventory.Namespace)
	service, err := c.tryLoadClusterService(client, app, namespace)
	if err != nil {
		return nil, nil, err
	}
	serving := make([]string, 0, len(service.Endpoints))
	for _, endpoint := range service.Endpoints {
		if !endpoint.Ready {
			continue
		}
		if podName := strings.TrimSpace(endpoint.Pod); podName != "" {
			serving = append(serving, podName)
		}
	}
	return uniqueSortedStrings(serving), &service, nil
}

func (c *CLI) tryLoadClusterService(client *Client, app model.App, namespace string) (model.ClusterServiceDetail, error) {
	serviceName := strings.TrimSpace(serviceNameForApp(app))
	if serviceName == "" {
		return model.ClusterServiceDetail{}, fmt.Errorf("app does not expose a cluster service")
	}
	if namespace == "" {
		namespace = fugueruntime.NamespaceForTenant(app.TenantID)
	}
	return client.GetClusterService(namespace, serviceName)
}

func normalizeAppConfigTarget(raw, pod string) (string, error) {
	if strings.TrimSpace(pod) != "" {
		return "pod", nil
	}
	value := strings.TrimSpace(strings.ToLower(raw))
	if value == "" {
		value = "serving"
	}
	switch value {
	case "serving", "ready":
		return value, nil
	default:
		return "", fmt.Errorf("target must be serving or ready")
	}
}

func desiredSpecForVerification(app model.App, operations []model.Operation) desiredSpecSelection {
	selection := desiredSpecSelection{
		Spec:   cloneAppSpec(app.Spec),
		Source: "app_spec",
	}
	for _, op := range operations {
		if op.DesiredSpec == nil {
			continue
		}
		switch strings.TrimSpace(op.Status) {
		case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
			selection.Spec = cloneAppSpec(*op.DesiredSpec)
			selection.Source = "latest_operation"
			selection.OperationID = strings.TrimSpace(op.ID)
			selection.OperationStatus = strings.TrimSpace(op.Status)
			return selection
		}
	}
	return selection
}

func desiredFileFromSpec(spec model.AppSpec, requestPath string) (string, int32, bool) {
	requestPath = strings.TrimSpace(requestPath)
	for _, file := range spec.Files {
		if strings.EqualFold(strings.TrimSpace(file.Path), requestPath) {
			return file.Content, file.Mode, true
		}
	}
	if spec.PersistentStorage != nil {
		for _, mount := range spec.PersistentStorage.Mounts {
			if !strings.EqualFold(strings.TrimSpace(mount.Path), requestPath) {
				continue
			}
			return mount.SeedContent, mount.Mode, true
		}
	}
	return "", 0, false
}

func readyPodNames(inventory model.AppRuntimePodInventory) []string {
	names := make([]string, 0)
	for _, group := range inventory.Groups {
		for _, pod := range group.Pods {
			if pod.Ready {
				names = append(names, strings.TrimSpace(pod.Name))
			}
		}
	}
	return uniqueSortedStrings(names)
}

func readyPodRevisions(inventory model.AppRuntimePodInventory) []string {
	revisionByPod := podRevisionMap(inventory)
	revisions := make([]string, 0)
	for _, podName := range readyPodNames(inventory) {
		if revision := strings.TrimSpace(revisionByPod[podName]); revision != "" {
			revisions = append(revisions, revision)
		}
	}
	return uniqueSortedStrings(revisions)
}

func podRevisionMap(inventory model.AppRuntimePodInventory) map[string]string {
	out := map[string]string{}
	for _, group := range inventory.Groups {
		revision := strings.TrimSpace(group.Revision)
		for _, pod := range group.Pods {
			if name := strings.TrimSpace(pod.Name); name != "" {
				out[name] = revision
			}
		}
	}
	return out
}

func latestRuntimeRevision(groups []model.AppRuntimePodGroup) string {
	candidate := ""
	best := -1
	for _, group := range groups {
		revision := strings.TrimSpace(group.Revision)
		if revision == "" {
			continue
		}
		if number := parseRevisionNumber(revision); number > best {
			best = number
			candidate = revision
		}
	}
	return candidate
}

func deploymentNameFromInventory(inventory model.AppRuntimePodInventory) string {
	for _, group := range inventory.Groups {
		if group.Parent != nil && strings.EqualFold(strings.TrimSpace(group.Parent.Kind), "Deployment") {
			return strings.TrimSpace(group.Parent.Name)
		}
		if strings.EqualFold(strings.TrimSpace(group.OwnerKind), "Deployment") {
			return strings.TrimSpace(group.OwnerName)
		}
	}
	return ""
}

func verifyTargetPods(target, pod string, servingPods, readyPods []string) []string {
	if value := strings.TrimSpace(pod); value != "" {
		return []string{value}
	}
	switch strings.TrimSpace(target) {
	case "ready":
		return append([]string(nil), readyPods...)
	default:
		if len(servingPods) > 0 {
			return append([]string(nil), servingPods...)
		}
		return append([]string(nil), readyPods...)
	}
}

func serviceNameForApp(app model.App) string {
	if app.InternalService != nil && strings.TrimSpace(app.InternalService.Name) != "" {
		return strings.TrimSpace(app.InternalService.Name)
	}
	return fugueruntime.RuntimeAppResourceName(app)
}

func workloadContainerTemplate(manifest map[string]any, containerName string) (appConfigContainerTemplate, error) {
	container, volumes, err := lookupManifestContainer(manifest, containerName)
	if err != nil {
		return appConfigContainerTemplate{}, err
	}
	template := appConfigContainerTemplate{
		Image:   nestedString(container, "image"),
		Command: append(trimStringAnySlice(container["command"]), trimStringAnySlice(container["args"])...),
		Env:     map[string]string{},
		Mounts:  []appConfigMount{},
	}
	for _, entry := range nestedSlice(container, "env") {
		envEntry, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		key := nestedString(envEntry, "name")
		if key == "" {
			continue
		}
		if value, ok := envEntry["value"].(string); ok {
			template.Env[key] = value
		}
	}
	for _, entry := range nestedSlice(container, "volumeMounts") {
		mount, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		path := nestedString(mount, "mountPath")
		if path == "" {
			continue
		}
		name := nestedString(mount, "name")
		source := volumes[name]
		detail := source
		if subPath := nestedString(mount, "subPath"); subPath != "" {
			detail = firstNonEmptyTrimmed(detail, name)
			detail = detail + " subPath=" + subPath
		}
		template.Mounts = append(template.Mounts, appConfigMount{
			Path:   path,
			Name:   name,
			Source: source,
			Detail: detail,
		})
	}
	sort.Slice(template.Mounts, func(i, j int) bool {
		return template.Mounts[i].Path < template.Mounts[j].Path
	})
	return template, nil
}

func lookupManifestContainer(manifest map[string]any, containerName string) (map[string]any, map[string]string, error) {
	spec, ok := nestedMap(manifest, "spec")
	if !ok {
		return nil, nil, fmt.Errorf("workload manifest is missing spec")
	}
	template, ok := nestedMap(spec, "template")
	if !ok {
		return nil, nil, fmt.Errorf("workload manifest is missing spec.template")
	}
	podSpec, ok := nestedMap(template, "spec")
	if !ok {
		return nil, nil, fmt.Errorf("workload manifest is missing spec.template.spec")
	}
	volumes := map[string]string{}
	for _, entry := range nestedSlice(podSpec, "volumes") {
		volume, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		name := nestedString(volume, "name")
		if name == "" {
			continue
		}
		volumes[name] = describeManifestVolume(volume)
	}
	containers := nestedSlice(podSpec, "containers")
	if len(containers) == 0 {
		return nil, nil, fmt.Errorf("workload manifest does not contain containers")
	}
	for _, entry := range containers {
		container, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if value := nestedString(container, "name"); containerName != "" && strings.EqualFold(value, containerName) {
			return container, volumes, nil
		}
	}
	first, ok := containers[0].(map[string]any)
	if !ok {
		return nil, nil, fmt.Errorf("workload manifest first container is invalid")
	}
	return first, volumes, nil
}

func describeManifestVolume(volume map[string]any) string {
	for _, candidate := range []struct {
		Key    string
		Source string
		Name   string
	}{
		{Key: "configMap", Source: "configMap", Name: "name"},
		{Key: "secret", Source: "secret", Name: "secretName"},
		{Key: "persistentVolumeClaim", Source: "persistentVolumeClaim", Name: "claimName"},
		{Key: "emptyDir", Source: "emptyDir"},
		{Key: "projected", Source: "projected"},
		{Key: "downwardAPI", Source: "downwardAPI"},
	} {
		value, ok := volume[candidate.Key].(map[string]any)
		if !ok {
			continue
		}
		if candidate.Name != "" {
			if sourceName := nestedString(value, candidate.Name); sourceName != "" {
				return candidate.Source + ":" + sourceName
			}
		}
		return candidate.Source
	}
	return ""
}

func desiredMountsFromSpec(spec model.AppSpec) []appConfigMount {
	out := make([]appConfigMount, 0)
	if spec.PersistentStorage != nil {
		for _, mount := range spec.PersistentStorage.Mounts {
			if path := strings.TrimSpace(mount.Path); path != "" {
				out = append(out, appConfigMount{
					Path:   path,
					Source: "persistent_storage",
					Detail: strings.TrimSpace(mount.Kind),
				})
			}
		}
	}
	if len(out) == 0 && spec.Workspace != nil {
		mountPath := strings.TrimSpace(spec.Workspace.MountPath)
		if mountPath == "" {
			mountPath = model.DefaultAppWorkspaceMountPath
		}
		out = append(out, appConfigMount{
			Path:   mountPath,
			Source: "workspace",
			Detail: "workspace",
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Path < out[j].Path
	})
	return out
}

func extractRevisionChecksums(manifest map[string]any) map[string]string {
	template, ok := nestedMap(manifest, "spec", "template", "metadata", "annotations")
	if !ok {
		return nil
	}
	out := map[string]string{}
	for key, raw := range template {
		value, ok := raw.(string)
		if !ok {
			continue
		}
		normalized := strings.ToLower(strings.TrimSpace(key))
		if strings.Contains(normalized, "checksum") || strings.HasSuffix(normalized, "hash") || strings.Contains(normalized, "/hash") {
			out[key] = strings.TrimSpace(value)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func finalizeAppConfigVerifyResult(result *appConfigVerifyResult) {
	if result == nil {
		return
	}
	switch {
	case !result.Desired.Found:
		result.ConclusionCode = "desired_missing"
		result.Conclusion = "the control-plane desired spec does not contain the requested file path"
	case len(result.Live) == 0:
		result.ConclusionCode = "live_missing"
		result.Conclusion = "no live pods were available for checksum verification"
	case result.ReplicaDrift:
		result.ConclusionCode = "replica_drift"
		result.Conclusion = "ready replicas do not agree on the live file checksum"
	case !result.FileInSync:
		result.ConclusionCode = "runtime_drift"
		result.Conclusion = "the desired file checksum does not match the live runtime file"
	case result.State.StateObserved && !result.State.StateInSync:
		result.ConclusionCode = "state_drift"
		result.Conclusion = "the file checksum matches, but image, command, env, or mount state still differs from the desired spec"
	case !result.State.StateObserved:
		result.ConclusionCode = "inconclusive"
		result.Conclusion = "the file checksum matches, but the broader runtime state could not be observed"
	default:
		result.ConclusionCode = "in_sync"
		result.Conclusion = "desired and live state are in sync"
	}

	switch result.ConclusionCode {
	case "runtime_drift", "replica_drift", "state_drift":
		result.NextActions = append(result.NextActions,
			fmt.Sprintf("fugue app config reconcile %s %s --wait", result.App, result.Path),
			fmt.Sprintf("fugue app logs pods %s", result.App),
		)
	case "desired_missing":
		result.NextActions = append(result.NextActions,
			fmt.Sprintf("fugue app config get %s %s", result.App, result.Path),
			fmt.Sprintf("fugue app config put %s %s --from-file <local-path>", result.App, result.Path),
		)
	case "inconclusive":
		if strings.TrimSpace(result.Namespace) != "" && strings.TrimSpace(result.Deployment) != "" {
			result.NextActions = append(result.NextActions,
				fmt.Sprintf("fugue admin cluster workload show %s deployment %s", result.Namespace, result.Deployment),
			)
		}
	}
}

func renderAppConfigVerifyResult(w io.Writer, result appConfigVerifyResult) error {
	if err := writeKeyValues(w,
		kvPair{Key: "app", Value: formatDisplayName(result.App, result.AppID, false)},
		kvPair{Key: "path", Value: result.Path},
		kvPair{Key: "source", Value: result.Source},
		kvPair{Key: "target", Value: result.Target},
		kvPair{Key: "namespace", Value: result.Namespace},
		kvPair{Key: "deployment", Value: result.Deployment},
		kvPair{Key: "desired_sha256", Value: result.Desired.SHA256},
		kvPair{Key: "desired_revision", Value: result.Desired.Revision},
		kvPair{Key: "file_in_sync", Value: fmt.Sprintf("%t", result.FileInSync)},
		kvPair{Key: "replica_drift", Value: fmt.Sprintf("%t", result.ReplicaDrift)},
		kvPair{Key: "state_observed", Value: fmt.Sprintf("%t", result.State.StateObserved)},
		kvPair{Key: "state_in_sync", Value: fmt.Sprintf("%t", result.State.StateInSync)},
		kvPair{Key: "conclusion_code", Value: result.ConclusionCode},
		kvPair{Key: "conclusion", Value: result.Conclusion},
	); err != nil {
		return err
	}
	if len(result.ReadyPods) > 0 {
		if _, err := fmt.Fprintf(w, "ready_pods=%s\n", strings.Join(result.ReadyPods, ",")); err != nil {
			return err
		}
	}
	if len(result.ServingPods) > 0 {
		if _, err := fmt.Fprintf(w, "serving_pods=%s\n", strings.Join(result.ServingPods, ",")); err != nil {
			return err
		}
	}
	if len(result.Live) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeAppConfigLiveTable(w, result.Live); err != nil {
			return err
		}
	}
	if result.State.StateObserved {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if err := writeKeyValues(w,
			kvPair{Key: "desired_image", Value: result.State.DesiredImage},
			kvPair{Key: "live_image", Value: result.State.LiveImage},
			kvPair{Key: "image_in_sync", Value: fmt.Sprintf("%t", result.State.ImageInSync)},
			kvPair{Key: "command_in_sync", Value: fmt.Sprintf("%t", result.State.CommandInSync)},
			kvPair{Key: "env_in_sync", Value: fmt.Sprintf("%t", result.State.EnvInSync)},
			kvPair{Key: "mounts_in_sync", Value: fmt.Sprintf("%t", result.State.MountsInSync)},
		); err != nil {
			return err
		}
	}
	for _, warning := range result.Warnings {
		if _, err := fmt.Fprintf(w, "warning=%s\n", warning); err != nil {
			return err
		}
	}
	for _, action := range result.NextActions {
		if _, err := fmt.Fprintf(w, "next_action=%s\n", action); err != nil {
			return err
		}
	}
	return nil
}

func renderAppConfigReconcileResult(w io.Writer, result appConfigReconcileResult) error {
	if err := writeKeyValues(w,
		kvPair{Key: "app", Value: formatDisplayName(result.App, result.AppID, false)},
		kvPair{Key: "operation_id", Value: valueOrEmpty(result.Operation)},
		kvPair{Key: "restart_token", Value: result.RestartToken},
		kvPair{Key: "expected_revision", Value: result.ExpectedRevision},
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	return renderAppConfigVerifyResult(w, result.Verification)
}

func writeAppConfigLiveTable(w io.Writer, live []appConfigLiveFile) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "POD\tREVISION\tSHA256\tSIZE\tMATCH\tERROR"); err != nil {
		return err
	}
	for _, entry := range live {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%d\t%t\t%s\n",
			entry.Pod,
			entry.Revision,
			entry.SHA256,
			entry.SizeBytes,
			entry.Match,
			entry.Error,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func sanitizeAppConfigVerifyResult(result appConfigVerifyResult, redact bool) appConfigVerifyResult {
	out := result
	if !redact {
		return out
	}
	out.State.DesiredEnv = cloneStringMapForOutput(out.State.DesiredEnv, true)
	out.State.LiveEnv = cloneStringMapForOutput(out.State.LiveEnv, true)
	out.Warnings = redactDiagnosticStringSlice(out.Warnings)
	out.NextActions = redactDiagnosticStringSlice(out.NextActions)
	out.Conclusion = redactDiagnosticString(out.Conclusion)
	for index := range out.Live {
		out.Live[index].Error = redactDiagnosticString(out.Live[index].Error)
	}
	return out
}

func sanitizeAppConfigReconcileResult(result appConfigReconcileResult, redact bool) appConfigReconcileResult {
	out := result
	if out.Operation != nil {
		copy := redactOperationForOutput(*out.Operation)
		out.Operation = &copy
	}
	out.Verification = sanitizeAppConfigVerifyResult(out.Verification, redact)
	return out
}

func decodeFilesystemContent(content, encoding string) ([]byte, error) {
	switch strings.TrimSpace(strings.ToLower(encoding)) {
	case "", "utf-8", "utf8", "text":
		return []byte(content), nil
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(content))
		if err != nil {
			return nil, fmt.Errorf("decode live file body: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported live file encoding %q", encoding)
	}
}

func appConfigSHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func sha256StringMap(values map[string]string) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		lines = append(lines, key+"="+values[key])
	}
	return appConfigSHA256Hex([]byte(strings.Join(lines, "\n")))
}

func diffStringMaps(desired, live map[string]string) ([]string, []string, []string) {
	missing := make([]string, 0)
	extra := make([]string, 0)
	changed := make([]string, 0)
	for key, value := range desired {
		liveValue, ok := live[key]
		if !ok {
			missing = append(missing, key)
			continue
		}
		if liveValue != value {
			changed = append(changed, key)
		}
	}
	for key := range live {
		if _, ok := desired[key]; !ok {
			extra = append(extra, key)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	sort.Strings(changed)
	return missing, extra, changed
}

func diffMountSets(desired, live []appConfigMount) ([]string, []string) {
	desiredSet := map[string]string{}
	liveSet := map[string]string{}
	for _, mount := range desired {
		desiredSet[mount.Path] = mount.Detail
	}
	for _, mount := range live {
		liveSet[mount.Path] = mount.Detail
	}
	missing := make([]string, 0)
	extra := make([]string, 0)
	for path, detail := range desiredSet {
		if liveDetail, ok := liveSet[path]; !ok || liveDetail != detail {
			missing = append(missing, path)
		}
	}
	for path, detail := range liveSet {
		if desiredDetail, ok := desiredSet[path]; !ok || desiredDetail != detail {
			extra = append(extra, path)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return missing, extra
}

func normalizeCommandSummary(command, args []string) []string {
	combined := make([]string, 0, len(command)+len(args))
	for _, value := range command {
		value = strings.TrimSpace(value)
		if value != "" {
			combined = append(combined, value)
		}
	}
	for _, value := range args {
		value = strings.TrimSpace(value)
		if value != "" {
			combined = append(combined, value)
		}
	}
	return combined
}

func normalizedStringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if strings.TrimSpace(left[index]) != strings.TrimSpace(right[index]) {
			return false
		}
	}
	return true
}

func detectReplicaDrift(live []appConfigLiveFile) bool {
	values := map[string]struct{}{}
	for _, entry := range live {
		if strings.TrimSpace(entry.Error) != "" || strings.TrimSpace(entry.SHA256) == "" {
			continue
		}
		values[entry.SHA256] = struct{}{}
	}
	return len(values) > 1
}

func allLiveSnapshotsMatch(live []appConfigLiveFile, desiredSHA string) bool {
	if strings.TrimSpace(desiredSHA) == "" || len(live) == 0 {
		return false
	}
	for _, entry := range live {
		if strings.TrimSpace(entry.Error) != "" || strings.TrimSpace(entry.SHA256) == "" {
			return false
		}
		if entry.SHA256 != desiredSHA {
			return false
		}
	}
	return true
}

func endpointPodRevisions(service model.ClusterServiceDetail, inventory model.AppRuntimePodInventory) []string {
	revisionByPod := podRevisionMap(inventory)
	out := make([]string, 0, len(service.Endpoints))
	for _, endpoint := range service.Endpoints {
		if !endpoint.Ready {
			continue
		}
		if podName := strings.TrimSpace(endpoint.Pod); podName != "" {
			if revision := strings.TrimSpace(revisionByPod[podName]); revision != "" {
				out = append(out, revision)
			}
		}
	}
	return uniqueSortedStrings(out)
}

func revisionFromOperationMessage(message string) string {
	matches := deployedRevisionPattern.FindStringSubmatch(strings.TrimSpace(message))
	if len(matches) != 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func parseRevisionNumber(raw string) int {
	value := 0
	for _, ch := range strings.TrimSpace(raw) {
		if ch < '0' || ch > '9' {
			return -1
		}
		value = value*10 + int(ch-'0')
	}
	return value
}

func uniqueSortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func allStringsEqual(values []string, want string) bool {
	if len(values) == 0 || strings.TrimSpace(want) == "" {
		return false
	}
	for _, value := range values {
		if strings.TrimSpace(value) != strings.TrimSpace(want) {
			return false
		}
	}
	return true
}

func nestedMap(root map[string]any, path ...string) (map[string]any, bool) {
	current := root
	for index, key := range path {
		value, ok := current[key]
		if !ok {
			return nil, false
		}
		if index == len(path)-1 {
			next, ok := value.(map[string]any)
			return next, ok
		}
		next, ok := value.(map[string]any)
		if !ok {
			return nil, false
		}
		current = next
	}
	return nil, false
}

func nestedSlice(root map[string]any, key string) []any {
	values, _ := root[key].([]any)
	return values
}

func nestedString(root map[string]any, key string) string {
	value, _ := root[key].(string)
	return strings.TrimSpace(value)
}

func trimStringAnySlice(values any) []string {
	raw, _ := values.([]any)
	out := make([]string, 0, len(raw))
	for _, entry := range raw {
		value, _ := entry.(string)
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func valueOrEmpty(op *model.Operation) string {
	if op == nil {
		return ""
	}
	return strings.TrimSpace(op.ID)
}

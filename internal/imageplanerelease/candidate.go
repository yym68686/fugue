// Package imageplanerelease defines the immutable, non-executable candidate
// boundary for one image-plane shadow cell. A candidate proves that source,
// artifact, chart, release-control observation, and rendered workload agree;
// it deliberately has no Kubernetes client or production adapter capability.
package imageplanerelease

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"fugue/internal/componentmanifest"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	CandidateRequestAPIVersion      = "image-plane-release.fugue.dev/v1"
	CandidateRequestKind            = "ImagePlaneShadowCandidateRequest"
	CandidateAPIVersion             = "image-plane-release.fugue.dev/v1"
	CandidateKind                   = "ImagePlaneShadowCandidate"
	CandidatePolicy                 = "cell-bound-observation-only-v1"
	CandidateReleaseChannel         = "shadow"
	CandidateRollbackMode           = "helm-revision-ondelete-lkg-preserve-v1"
	ComponentPlanStatusAPIVersionV1 = "release-control.fugue.dev/v1"
	ComponentPlanStatusKindV1       = "ComponentPlanStatus"
	ComponentPlanStatusPolicyV1     = "artifact-ledger-shadow-v1"
)

var (
	ErrCandidate              = errors.New("invalid image-plane shadow release candidate")
	exactCommitPattern        = regexp.MustCompile(`^[0-9a-f]{40}$`)
	exactDigestPattern        = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	imageRepositoryPattern    = regexp.MustCompile(`^[a-z0-9._-]+(:[0-9]+)?(/[a-z0-9._-]+)+$`)
	componentPlanGenerationV1 = regexp.MustCompile(`^git-[0-9a-f]{40}$`)
	componentPlanScopeV1      = regexp.MustCompile(`^component-release-plan:[0-9a-f]{40}\.\.[0-9a-f]{40}$`)
)

// ComponentPlanStatusV1 is the versioned wire/file contract consumed from
// release-control. Keeping this shape local prevents the image-plane planner
// from importing release-control's concrete HTTP adapter implementation.
type ComponentPlanStatusV1 struct {
	APIVersion                string `json:"apiVersion"`
	Kind                      string `json:"kind"`
	Policy                    string `json:"policy"`
	State                     string `json:"state"`
	ArtifactID                string `json:"artifactId"`
	ContentHash               string `json:"contentHash"`
	ScopeKey                  string `json:"scopeKey"`
	Generation                string `json:"generation"`
	PlanDigest                string `json:"planDigest"`
	CoordinationDigest        string `json:"coordinationDigest"`
	ReleaseID                 string `json:"releaseId"`
	LaneKey                   string `json:"laneKey"`
	FencingToken              int64  `json:"fencingToken"`
	LaneVersion               int64  `json:"laneVersion"`
	IdempotencyKey            string `json:"idempotencyKey"`
	ObservationOnly           bool   `json:"observationOnly"`
	ProductionMutationAllowed bool   `json:"productionMutationAllowed"`
	Digest                    string `json:"digest"`
}

// CandidateRequest is trusted, immutable input assembled before any cluster
// discovery. The embedded release-control status remains observation-only and
// therefore cannot be confused with a deploy authorization.
type CandidateRequest struct {
	APIVersion            string                                   `json:"apiVersion"`
	Kind                  string                                   `json:"kind"`
	SourceCommit          string                                   `json:"sourceCommit"`
	CellID                string                                   `json:"cellId"`
	ReleaseName           string                                   `json:"releaseName"`
	ReleaseNamespace      string                                   `json:"releaseNamespace"`
	ImageRepository       string                                   `json:"imageRepository"`
	ImageDigest           string                                   `json:"imageDigest"`
	APIBaseURL            string                                   `json:"apiBaseUrl"`
	RuntimePort           int32                                    `json:"runtimePort"`
	ChartDigest           string                                   `json:"chartDigest"`
	ComponentPlanEnvelope componentmanifest.ShadowArtifactEnvelope `json:"componentPlanEnvelope"`
	ComponentPlanStatus   ComponentPlanStatusV1                    `json:"componentPlanStatus"`
}

// Candidate is a digest-bound handoff to a later, separately reviewed live
// preflight. ExecutionAllowed and ProductionMutationAllowed are permanently
// false in v1; no consumer may reinterpret this file as a cluster write grant.
type Candidate struct {
	APIVersion                string   `json:"apiVersion"`
	Kind                      string   `json:"kind"`
	Policy                    string   `json:"policy"`
	SourceCommit              string   `json:"sourceCommit"`
	CellID                    string   `json:"cellId"`
	ReleaseName               string   `json:"releaseName"`
	ReleaseNamespace          string   `json:"releaseNamespace"`
	ImageRepository           string   `json:"imageRepository"`
	ImageDigest               string   `json:"imageDigest"`
	APIBaseURL                string   `json:"apiBaseUrl"`
	RuntimePort               int32    `json:"runtimePort"`
	ChartDigest               string   `json:"chartDigest"`
	ManifestDigest            string   `json:"manifestDigest"`
	WorkloadName              string   `json:"workloadName"`
	ReleaseChannel            string   `json:"releaseChannel"`
	CellLockKey               string   `json:"cellLockKey"`
	IdempotencyKey            string   `json:"idempotencyKey"`
	ComponentPlanArtifactID   string   `json:"componentPlanArtifactId"`
	ComponentPlanScopeKey     string   `json:"componentPlanScopeKey"`
	ComponentPlanGeneration   string   `json:"componentPlanGeneration"`
	ComponentPlanPlanDigest   string   `json:"componentPlanPlanDigest"`
	ComponentPlanCoordination string   `json:"componentPlanCoordinationDigest"`
	ComponentPlanStatusDigest string   `json:"componentPlanStatusDigest"`
	ComponentPlanLaneKey      string   `json:"componentPlanLaneKey"`
	ComponentPlanFence        int64    `json:"componentPlanFence"`
	ComponentPlanLaneVersion  int64    `json:"componentPlanLaneVersion"`
	ObservationOnly           bool     `json:"observationOnly"`
	ExecutionAllowed          bool     `json:"executionAllowed"`
	ProductionMutationAllowed bool     `json:"productionMutationAllowed"`
	RollbackRequired          bool     `json:"rollbackRequired"`
	RollbackMode              string   `json:"rollbackMode"`
	Blockers                  []string `json:"blockers"`
	Digest                    string   `json:"digest"`
}

// BuildCandidate verifies every duplicated binding and exactly one isolated
// rendered DaemonSet. It performs no file, network, process, or cluster I/O.
func BuildCandidate(request CandidateRequest, renderedManifest []byte) (Candidate, error) {
	if err := ValidateCandidateRequest(request); err != nil {
		return Candidate{}, err
	}
	workloadName, manifestDigest, err := validateRenderedManifest(request, renderedManifest)
	if err != nil {
		return Candidate{}, err
	}
	blockers := append([]string(nil), request.ComponentPlanEnvelope.CoordinationPlan.Blockers...)
	blockers = append(blockers,
		"candidate is observation-only and cannot authorize a cluster mutation",
		"production release freeze must be cleared by the unique coordinator",
	)
	blockers = uniqueSorted(blockers)
	candidate := Candidate{
		APIVersion:                CandidateAPIVersion,
		Kind:                      CandidateKind,
		Policy:                    CandidatePolicy,
		SourceCommit:              request.SourceCommit,
		CellID:                    request.CellID,
		ReleaseName:               request.ReleaseName,
		ReleaseNamespace:          request.ReleaseNamespace,
		ImageRepository:           request.ImageRepository,
		ImageDigest:               request.ImageDigest,
		APIBaseURL:                request.APIBaseURL,
		RuntimePort:               request.RuntimePort,
		ChartDigest:               request.ChartDigest,
		ManifestDigest:            manifestDigest,
		WorkloadName:              workloadName,
		ReleaseChannel:            CandidateReleaseChannel,
		CellLockKey:               "lane/image-plane/cell/" + request.CellID,
		IdempotencyKey:            "image-plane-shadow/" + request.CellID + "/" + strings.TrimPrefix(manifestDigest, "sha256:"),
		ComponentPlanArtifactID:   request.ComponentPlanStatus.ArtifactID,
		ComponentPlanScopeKey:     request.ComponentPlanStatus.ScopeKey,
		ComponentPlanGeneration:   request.ComponentPlanStatus.Generation,
		ComponentPlanPlanDigest:   request.ComponentPlanStatus.PlanDigest,
		ComponentPlanCoordination: request.ComponentPlanStatus.CoordinationDigest,
		ComponentPlanStatusDigest: request.ComponentPlanStatus.Digest,
		ComponentPlanLaneKey:      request.ComponentPlanStatus.LaneKey,
		ComponentPlanFence:        request.ComponentPlanStatus.FencingToken,
		ComponentPlanLaneVersion:  request.ComponentPlanStatus.LaneVersion,
		ObservationOnly:           true,
		ExecutionAllowed:          false,
		ProductionMutationAllowed: false,
		RollbackRequired:          true,
		RollbackMode:              CandidateRollbackMode,
		Blockers:                  blockers,
	}
	candidate.Digest = DigestCandidate(candidate)
	if err := VerifyCandidate(candidate); err != nil {
		return Candidate{}, err
	}
	return candidate, nil
}

// ValidateCandidateRequest proves the component-plan status and envelope are
// the same exact observed image-plane-only change before inspecting a render.
func ValidateCandidateRequest(request CandidateRequest) error {
	if request.APIVersion != CandidateRequestAPIVersion || request.Kind != CandidateRequestKind {
		return fmt.Errorf("%w: apiVersion and kind must be %q and %q", ErrCandidate, CandidateRequestAPIVersion, CandidateRequestKind)
	}
	if !exactCommitPattern.MatchString(request.SourceCommit) {
		return fmt.Errorf("%w: sourceCommit must be exact lowercase 40-hex", ErrCandidate)
	}
	if errs := validation.IsDNS1123Label(request.CellID); len(errs) != 0 || strings.TrimSpace(request.CellID) != request.CellID {
		return fmt.Errorf("%w: cellId must be an exact lowercase DNS label", ErrCandidate)
	}
	if errs := validation.IsDNS1123Label(request.ReleaseName); len(errs) != 0 || strings.TrimSpace(request.ReleaseName) != request.ReleaseName {
		return fmt.Errorf("%w: releaseName must be an exact lowercase DNS label", ErrCandidate)
	}
	if request.ReleaseName != "image-plane-"+request.CellID {
		return fmt.Errorf("%w: releaseName must be exactly image-plane-<cellId>", ErrCandidate)
	}
	if errs := validation.IsDNS1123Label(request.ReleaseNamespace); len(errs) != 0 || strings.TrimSpace(request.ReleaseNamespace) != request.ReleaseNamespace {
		return fmt.Errorf("%w: releaseNamespace must be an exact lowercase DNS label", ErrCandidate)
	}
	if !imageRepositoryPattern.MatchString(request.ImageRepository) ||
		!strings.HasSuffix(request.ImageRepository, "/fugue-image-plane-agent") {
		return fmt.Errorf("%w: imageRepository must be a fully qualified dedicated fugue-image-plane-agent repository", ErrCandidate)
	}
	if !exactDigestPattern.MatchString(request.ImageDigest) || !exactDigestPattern.MatchString(request.ChartDigest) {
		return fmt.Errorf("%w: imageDigest and chartDigest must be exact lowercase sha256 digests", ErrCandidate)
	}
	if err := validateAPIBaseURL(request.APIBaseURL); err != nil {
		return fmt.Errorf("%w: %v", ErrCandidate, err)
	}
	if request.RuntimePort < 1024 || request.RuntimePort > 65535 {
		return fmt.Errorf("%w: runtimePort must be between 1024 and 65535", ErrCandidate)
	}
	if err := request.ComponentPlanEnvelope.Validate(); err != nil {
		return fmt.Errorf("%w: component plan envelope: %v", ErrCandidate, err)
	}
	if err := VerifyComponentPlanStatusV1(request.ComponentPlanStatus); err != nil {
		return fmt.Errorf("%w: component plan status is invalid", ErrCandidate)
	}
	identity, err := request.ComponentPlanEnvelope.ArtifactIdentity()
	if err != nil {
		return fmt.Errorf("%w: component plan identity: %v", ErrCandidate, err)
	}
	contentHash, err := componentPlanContentHash(request.ComponentPlanEnvelope)
	if err != nil {
		return fmt.Errorf("%w: component plan content: %v", ErrCandidate, err)
	}
	status := request.ComponentPlanStatus
	plan := request.ComponentPlanEnvelope.ChangePlan
	coordination := request.ComponentPlanEnvelope.CoordinationPlan
	if request.SourceCommit != request.ComponentPlanEnvelope.TargetCommit ||
		status.ScopeKey != identity.ScopeKey || status.Generation != identity.Generation ||
		status.ContentHash != contentHash || status.PlanDigest != plan.PlanDigest ||
		status.CoordinationDigest != coordination.CoordinationDigest ||
		status.IdempotencyKey != coordination.IdempotencyKey {
		return fmt.Errorf("%w: component plan envelope, status, and source commit do not match", ErrCandidate)
	}
	if len(plan.ImpactedComponents) != 1 ||
		plan.ImpactedComponents[0].ID != "image-plane" ||
		plan.ImpactedComponents[0].ReleaseLane != "image-plane" ||
		plan.DispatchMode != componentmanifest.DispatchModeShadow || !plan.RequiresLegacyRelease ||
		!coordination.ObservationOnly || coordination.ProductionMutationAllowed ||
		!containsCoordinationScope(coordination.Scopes, "lane/image-plane", "lane", "image-plane", "exclusive") {
		return fmt.Errorf("%w: component plan is not the exact observation-only image-plane lane", ErrCandidate)
	}
	return nil
}

// VerifyComponentPlanStatusV1 independently verifies the release-control v1
// contract, including its canonical digest and server-derived shadow lane.
func VerifyComponentPlanStatusV1(status ComponentPlanStatusV1) error {
	if status.APIVersion != ComponentPlanStatusAPIVersionV1 || status.Kind != ComponentPlanStatusKindV1 ||
		status.Policy != ComponentPlanStatusPolicyV1 || status.State != "observed" ||
		!status.ObservationOnly || status.ProductionMutationAllowed ||
		strings.TrimSpace(status.ArtifactID) == "" || strings.TrimSpace(status.ReleaseID) == "" ||
		!exactDigestPattern.MatchString(status.ContentHash) || !exactDigestPattern.MatchString(status.PlanDigest) ||
		!exactDigestPattern.MatchString(status.CoordinationDigest) || !componentPlanScopeV1.MatchString(status.ScopeKey) ||
		!componentPlanGenerationV1.MatchString(status.Generation) || status.FencingToken <= 0 || status.LaneVersion <= 0 ||
		status.IdempotencyKey != "component-shadow/"+strings.TrimPrefix(status.PlanDigest, "sha256:") {
		return ErrCandidate
	}
	expectedLane := strings.Join([]string{"component_release_plan", status.ScopeKey, "shadow"}, "|")
	if status.LaneKey != expectedLane || status.Digest != DigestComponentPlanStatusV1(status) {
		return ErrCandidate
	}
	return nil
}

// DigestComponentPlanStatusV1 mirrors the documented release-control v1
// canonical JSON digest while keeping the implementation dependency one-way.
func DigestComponentPlanStatusV1(status ComponentPlanStatusV1) string {
	status.Digest = ""
	encoded, err := json.Marshal(status)
	if err != nil {
		panic(fmt.Sprintf("marshal component plan status v1: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// VerifyCandidate validates the persisted shape without trusting its digest.
func VerifyCandidate(candidate Candidate) error {
	if candidate.APIVersion != CandidateAPIVersion || candidate.Kind != CandidateKind || candidate.Policy != CandidatePolicy ||
		!exactCommitPattern.MatchString(candidate.SourceCommit) ||
		!exactDigestPattern.MatchString(candidate.ImageDigest) || !exactDigestPattern.MatchString(candidate.ChartDigest) ||
		!exactDigestPattern.MatchString(candidate.ManifestDigest) || !exactDigestPattern.MatchString(candidate.ComponentPlanPlanDigest) ||
		!exactDigestPattern.MatchString(candidate.ComponentPlanCoordination) || !exactDigestPattern.MatchString(candidate.ComponentPlanStatusDigest) ||
		candidate.ReleaseChannel != CandidateReleaseChannel || !candidate.ObservationOnly || candidate.ExecutionAllowed ||
		candidate.ProductionMutationAllowed || !candidate.RollbackRequired || candidate.RollbackMode != CandidateRollbackMode ||
		candidate.ComponentPlanFence <= 0 || candidate.ComponentPlanLaneVersion <= 0 ||
		candidate.RuntimePort < 1024 || candidate.RuntimePort > 65535 ||
		strings.TrimSpace(candidate.ComponentPlanArtifactID) == "" || strings.TrimSpace(candidate.ComponentPlanGeneration) == "" ||
		strings.TrimSpace(candidate.ComponentPlanLaneKey) == "" || strings.TrimSpace(candidate.WorkloadName) == "" {
		return ErrCandidate
	}
	if !imageRepositoryPattern.MatchString(candidate.ImageRepository) ||
		!strings.HasSuffix(candidate.ImageRepository, "/fugue-image-plane-agent") || validateAPIBaseURL(candidate.APIBaseURL) != nil ||
		!componentPlanScopeV1.MatchString(candidate.ComponentPlanScopeKey) ||
		candidate.ComponentPlanGeneration != "git-"+candidate.SourceCommit ||
		!strings.HasSuffix(candidate.ComponentPlanScopeKey, ".."+candidate.SourceCommit) ||
		candidate.ComponentPlanLaneKey != strings.Join([]string{"component_release_plan", candidate.ComponentPlanScopeKey, "shadow"}, "|") {
		return ErrCandidate
	}
	if errs := validation.IsDNS1123Label(candidate.CellID); len(errs) != 0 {
		return ErrCandidate
	}
	if errs := validation.IsDNS1123Label(candidate.ReleaseName); len(errs) != 0 {
		return ErrCandidate
	}
	if errs := validation.IsDNS1123Label(candidate.ReleaseNamespace); len(errs) != 0 {
		return ErrCandidate
	}
	if candidate.ReleaseName != "image-plane-"+candidate.CellID || candidate.WorkloadName != expectedWorkloadName(candidate.ReleaseName) ||
		candidate.CellLockKey != "lane/image-plane/cell/"+candidate.CellID ||
		candidate.IdempotencyKey != "image-plane-shadow/"+candidate.CellID+"/"+strings.TrimPrefix(candidate.ManifestDigest, "sha256:") ||
		len(candidate.Blockers) == 0 || !sort.StringsAreSorted(candidate.Blockers) || hasDuplicate(candidate.Blockers) ||
		!containsString(candidate.Blockers, "candidate is observation-only and cannot authorize a cluster mutation") ||
		!containsString(candidate.Blockers, "production release freeze must be cleared by the unique coordinator") ||
		candidate.Digest != DigestCandidate(candidate) {
		return ErrCandidate
	}
	return nil
}

func expectedWorkloadName(releaseName string) string {
	name := releaseName + "-fugue-image-plane"
	if len(name) > 63 {
		name = strings.TrimSuffix(name[:63], "-")
	}
	return name
}

// DigestCandidate returns the canonical digest with the self-reference empty.
func DigestCandidate(candidate Candidate) string {
	candidate.Digest = ""
	encoded, err := json.Marshal(candidate)
	if err != nil {
		panic(fmt.Sprintf("marshal image-plane candidate: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func validateRenderedManifest(request CandidateRequest, manifest []byte) (string, string, error) {
	if len(bytes.TrimSpace(manifest)) == 0 {
		return "", "", fmt.Errorf("%w: rendered manifest is empty", ErrCandidate)
	}
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(manifest), 4096)
	objects := make([]*unstructured.Unstructured, 0, 1)
	for {
		object := &unstructured.Unstructured{}
		err := decoder.Decode(object)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", "", fmt.Errorf("%w: decode rendered manifest: %v", ErrCandidate, err)
		}
		if len(object.Object) != 0 {
			objects = append(objects, object)
		}
	}
	if len(objects) != 1 || objects[0].GetAPIVersion() != "apps/v1" || objects[0].GetKind() != "DaemonSet" {
		return "", "", fmt.Errorf("%w: render must contain exactly one apps/v1 DaemonSet", ErrCandidate)
	}
	object := objects[0]
	encoded, err := json.Marshal(object.Object)
	if err != nil {
		return "", "", fmt.Errorf("%w: encode rendered DaemonSet: %v", ErrCandidate, err)
	}
	var daemonSet appsv1.DaemonSet
	if err := json.Unmarshal(encoded, &daemonSet); err != nil {
		return "", "", fmt.Errorf("%w: parse rendered DaemonSet: %v", ErrCandidate, err)
	}
	if daemonSet.Name == "" || daemonSet.GenerateName != "" || daemonSet.Namespace != request.ReleaseNamespace ||
		len(daemonSet.OwnerReferences) != 0 || len(daemonSet.Finalizers) != 0 || len(daemonSet.Status.Conditions) != 0 {
		return "", "", fmt.Errorf("%w: rendered DaemonSet identity is unsafe or unbound", ErrCandidate)
	}
	wantSelector := map[string]string{
		"app.kubernetes.io/name":      "fugue-image-plane",
		"app.kubernetes.io/instance":  request.ReleaseName,
		"app.kubernetes.io/component": "image-plane-shadow",
		"fugue.io/cell-id":            request.CellID,
	}
	for key, value := range map[string]string{
		"app.kubernetes.io/name":       "fugue-image-plane",
		"app.kubernetes.io/instance":   request.ReleaseName,
		"app.kubernetes.io/component":  "image-plane-shadow",
		"fugue.io/cell-id":             request.CellID,
		"fugue.io/release-lane":        "image-plane",
		"fugue.io/ownership-mode":      "shadow",
		"fugue.io/production-mutation": "forbidden",
	} {
		if daemonSet.Labels[key] != value || daemonSet.Spec.Template.Labels[key] != value {
			return "", "", fmt.Errorf("%w: rendered DaemonSet label %s is not bound", ErrCandidate, key)
		}
	}
	if daemonSet.Spec.Selector == nil || len(daemonSet.Spec.Selector.MatchExpressions) != 0 ||
		!equalStringMap(daemonSet.Spec.Selector.MatchLabels, wantSelector) ||
		daemonSet.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType ||
		daemonSet.Spec.RevisionHistoryLimit == nil || *daemonSet.Spec.RevisionHistoryLimit != 2 {
		return "", "", fmt.Errorf("%w: rendered DaemonSet selector or OnDelete boundary drifted", ErrCandidate)
	}
	pod := daemonSet.Spec.Template.Spec
	if !equalStringMap(pod.NodeSelector, map[string]string{
		"fugue.io/image-plane-shadow": "true",
		"fugue.io/image-plane-cell":   request.CellID,
	}) || pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken ||
		pod.ServiceAccountName != "" || pod.HostNetwork || len(pod.InitContainers) != 0 || len(pod.EphemeralContainers) != 0 ||
		len(pod.Containers) != 1 || len(pod.Volumes) != 2 {
		return "", "", fmt.Errorf("%w: rendered Pod cell, identity, or workload boundary drifted", ErrCandidate)
	}
	container := pod.Containers[0]
	if container.Name != "image-plane-shadow" || container.Image != request.ImageRepository+"@"+request.ImageDigest ||
		container.ImagePullPolicy != corev1.PullIfNotPresent || len(container.Ports) != 0 || len(container.Command) != 0 || len(container.Args) != 0 ||
		!hasExactEnvironment(container.Env, "FUGUE_API_BASE", request.APIBaseURL) ||
		!hasExactEnvironment(container.Env, "FUGUE_IMAGE_CACHE_LISTEN_ADDR", fmt.Sprintf("127.0.0.1:%d", request.RuntimePort)) ||
		!validateVolumeBoundary(pod.Volumes, container.VolumeMounts) || !validateSecurityBoundary(pod, container) ||
		!validateExecProbe(container.StartupProbe, request.RuntimePort, "/healthz") ||
		!validateExecProbe(container.LivenessProbe, request.RuntimePort, "/healthz") ||
		!validateExecProbe(container.ReadinessProbe, request.RuntimePort, "/fugue/cache/v1/platform-plan/readyz") {
		return "", "", fmt.Errorf("%w: rendered agent artifact or API boundary drifted", ErrCandidate)
	}
	for _, variable := range container.Env {
		if variable.Name == "FUGUE_API_KEY" || variable.Name == "FUGUE_NODE_UPDATER_TOKEN" ||
			variable.Name == "FUGUE_IMAGE_CACHE_MANAGEMENT_TOKEN" || variable.Name == "FUGUE_IMAGE_CACHE_PLATFORM_PLAN_ALLOW_INSECURE_HTTP" {
			return "", "", fmt.Errorf("%w: rendered agent contains forbidden environment %s", ErrCandidate, variable.Name)
		}
	}
	manifestDigest := sha256.Sum256(encoded)
	return daemonSet.Name, "sha256:" + hex.EncodeToString(manifestDigest[:]), nil
}

func validateAPIBaseURL(raw string) error {
	if strings.TrimSpace(raw) != raw || raw == "" || strings.ContainsAny(raw, "\r\n\t ") {
		return errors.New("apiBaseUrl must be an exact HTTPS URL")
	}
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return errors.New("apiBaseUrl must be an absolute HTTPS URL without credentials, query, or fragment")
	}
	return nil
}

func componentPlanContentHash(envelope componentmanifest.ShadowArtifactEnvelope) (string, error) {
	content, err := envelope.Content()
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(content)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func containsCoordinationScope(scopes []componentmanifest.CoordinationScope, key, scopeType, owner, conflictMode string) bool {
	for _, scope := range scopes {
		if scope.Key == key && scope.ScopeType == scopeType && scope.Owner == owner && scope.ConflictMode == conflictMode {
			return true
		}
	}
	return false
}

func hasExactEnvironment(environment []corev1.EnvVar, name, value string) bool {
	found := false
	for _, variable := range environment {
		if variable.Name != name {
			continue
		}
		if found || variable.Value != value || variable.ValueFrom != nil {
			return false
		}
		found = true
	}
	return found
}

func validateVolumeBoundary(volumes []corev1.Volume, mounts []corev1.VolumeMount) bool {
	if len(volumes) != 2 || len(mounts) != 2 {
		return false
	}
	wantVolumes := map[string]string{
		"shadow-state":       "/var/lib/fugue/image-plane-shadow",
		"component-identity": "/run/fugue/image-cache",
	}
	for _, volume := range volumes {
		wantPath, exists := wantVolumes[volume.Name]
		if !exists || volume.HostPath == nil || volume.HostPath.Path != wantPath || volume.HostPath.Type == nil ||
			*volume.HostPath.Type != corev1.HostPathDirectory {
			return false
		}
		delete(wantVolumes, volume.Name)
	}
	if len(wantVolumes) != 0 {
		return false
	}
	wantMounts := map[string]struct {
		path     string
		readOnly bool
	}{
		"shadow-state":       {path: "/var/lib/fugue/image-cache", readOnly: false},
		"component-identity": {path: "/run/fugue/image-cache", readOnly: true},
	}
	for _, mount := range mounts {
		want, exists := wantMounts[mount.Name]
		if !exists || mount.MountPath != want.path || mount.ReadOnly != want.readOnly || mount.SubPath != "" || mount.SubPathExpr != "" {
			return false
		}
		delete(wantMounts, mount.Name)
	}
	return len(wantMounts) == 0
}

func validateSecurityBoundary(pod corev1.PodSpec, container corev1.Container) bool {
	podSecurity := pod.SecurityContext
	containerSecurity := container.SecurityContext
	return podSecurity != nil && podSecurity.RunAsNonRoot != nil && *podSecurity.RunAsNonRoot &&
		podSecurity.RunAsUser != nil && *podSecurity.RunAsUser == 65532 &&
		podSecurity.RunAsGroup != nil && *podSecurity.RunAsGroup == 65532 &&
		podSecurity.FSGroup != nil && *podSecurity.FSGroup == 65532 &&
		podSecurity.SeccompProfile != nil && podSecurity.SeccompProfile.Type == corev1.SeccompProfileTypeRuntimeDefault &&
		containerSecurity != nil && containerSecurity.AllowPrivilegeEscalation != nil && !*containerSecurity.AllowPrivilegeEscalation &&
		containerSecurity.Privileged != nil && !*containerSecurity.Privileged &&
		containerSecurity.ReadOnlyRootFilesystem != nil && *containerSecurity.ReadOnlyRootFilesystem &&
		containerSecurity.RunAsNonRoot != nil && *containerSecurity.RunAsNonRoot &&
		containerSecurity.RunAsUser != nil && *containerSecurity.RunAsUser == 65532 &&
		containerSecurity.RunAsGroup != nil && *containerSecurity.RunAsGroup == 65532 &&
		containerSecurity.Capabilities != nil && len(containerSecurity.Capabilities.Add) == 0 &&
		len(containerSecurity.Capabilities.Drop) == 1 && containerSecurity.Capabilities.Drop[0] == corev1.Capability("ALL")
}

func validateExecProbe(probe *corev1.Probe, port int32, path string) bool {
	want := []string{
		"/usr/bin/wget", "-q", "-T", "2", "-Y", "off", "--spider",
		fmt.Sprintf("http://127.0.0.1:%d%s", port, path),
	}
	if probe == nil || probe.Exec == nil || probe.HTTPGet != nil || probe.TCPSocket != nil || len(probe.Exec.Command) != len(want) {
		return false
	}
	for index := range want {
		if probe.Exec.Command[index] != want[index] {
			return false
		}
	}
	return true
}

func equalStringMap(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func uniqueSorted(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			set[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func hasDuplicate(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index] == values[index-1] {
			return true
		}
	}
	return false
}

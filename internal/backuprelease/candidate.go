// Package backuprelease defines the immutable, non-executable candidate
// boundary for one backup observer cell. A candidate proves that source,
// artifact, chart, release-control observation, cell identity, and rendered
// workload agree; it has no Kubernetes client or production adapter.
package backuprelease

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/componentmanifest"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

const (
	CandidateRequestAPIVersion      = "backup-release.fugue.dev/v1"
	CandidateRequestKind            = "BackupObserverShadowCandidateRequest"
	CandidateAPIVersion             = "backup-release.fugue.dev/v1"
	CandidateKind                   = "BackupObserverShadowCandidate"
	CandidatePolicy                 = "backup-cell-observation-only-v1"
	CandidateReleaseChannel         = "shadow"
	CandidateRollbackMode           = "helm-revision-recreate-lkg-preserve-v1"
	CandidateRecoveryLane           = "backup"
	BackupSpecContractV1            = "backup-control.fugue.dev/v1/BackupRunSpec"
	BackupStatusContractV1          = "backup-control.fugue.dev/v1/BackupRunStatus"
	BackupObserverStatusContractV2  = "backup-observer.fugue.dev/v2/BackupObserverStatus"
	ComponentPlanStatusAPIVersionV1 = "release-control.fugue.dev/v1"
	ComponentPlanStatusKindV1       = "ComponentPlanStatus"
	ComponentPlanStatusPolicyV1     = "artifact-ledger-shadow-v1"
)

var (
	ErrCandidate              = errors.New("invalid backup observer shadow release candidate")
	exactCommitPattern        = regexp.MustCompile(`^[0-9a-f]{40}$`)
	exactDigestPattern        = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	imageRepositoryPattern    = regexp.MustCompile(`^[a-z0-9._-]+(:[0-9]+)?(/[a-z0-9._-]+)+$`)
	cellKeyPattern            = regexp.MustCompile(`^backup/(control-plane-db|app-database|persistent-storage|data-workspace|registry|platform-component)/[0-9a-f]{16}$`)
	objectKeyPattern          = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	wholeSecondPattern        = regexp.MustCompile(`^[1-9][0-9]{0,3}s$`)
	componentPlanGenerationV1 = regexp.MustCompile(`^git-[0-9a-f]{40}$`)
	componentPlanScopeV1      = regexp.MustCompile(`^component-release-plan:[0-9a-f]{40}\.\.[0-9a-f]{40}$`)
)

// ComponentPlanStatusV1 is the versioned wire/file contract consumed from
// release-control. The backup lane independently validates this shape instead
// of importing release-control's concrete adapter implementation.
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

// CandidateRequest contains only immutable release inputs and externally
// owned object references. It never contains a bearer token or backup object
// location, and the embedded plan status cannot authorize execution.
type CandidateRequest struct {
	APIVersion            string                                   `json:"apiVersion"`
	Kind                  string                                   `json:"kind"`
	SourceCommit          string                                   `json:"sourceCommit"`
	CellKey               string                                   `json:"cellKey"`
	ReleaseName           string                                   `json:"releaseName"`
	ReleaseNamespace      string                                   `json:"releaseNamespace"`
	ImageRepository       string                                   `json:"imageRepository"`
	ImageDigest           string                                   `json:"imageDigest"`
	ChartDigest           string                                   `json:"chartDigest"`
	APIBaseURL            string                                   `json:"apiBaseUrl"`
	SpecConfigMapName     string                                   `json:"specConfigMapName"`
	SpecConfigMapKey      string                                   `json:"specConfigMapKey"`
	TokenSecretName       string                                   `json:"tokenSecretName"`
	TokenSecretKey        string                                   `json:"tokenSecretKey"`
	ReconcileInterval     string                                   `json:"reconcileInterval"`
	AttemptTimeout        string                                   `json:"attemptTimeout"`
	RequestTimeout        string                                   `json:"requestTimeout"`
	ShutdownTimeout       string                                   `json:"shutdownTimeout"`
	MaxResponseBytes      int64                                    `json:"maxResponseBytes"`
	ComponentPlanEnvelope componentmanifest.ShadowArtifactEnvelope `json:"componentPlanEnvelope"`
	ComponentPlanStatus   ComponentPlanStatusV1                    `json:"componentPlanStatus"`
}

// Candidate is a digest-bound handoff for a later separately reviewed live
// preflight. Its execution and production mutation flags are permanently false
// in v1, even when every artifact binding is valid.
type Candidate struct {
	APIVersion                string                `json:"apiVersion"`
	Kind                      string                `json:"kind"`
	Policy                    string                `json:"policy"`
	SourceCommit              string                `json:"sourceCommit"`
	CellKey                   string                `json:"cellKey"`
	CellID                    string                `json:"cellId"`
	ReleaseName               string                `json:"releaseName"`
	ReleaseNamespace          string                `json:"releaseNamespace"`
	ImageRepository           string                `json:"imageRepository"`
	ImageDigest               string                `json:"imageDigest"`
	ChartDigest               string                `json:"chartDigest"`
	ManifestDigest            string                `json:"manifestDigest"`
	WorkloadName              string                `json:"workloadName"`
	APIBaseURL                string                `json:"apiBaseUrl"`
	SpecConfigMapName         string                `json:"specConfigMapName"`
	SpecConfigMapKey          string                `json:"specConfigMapKey"`
	TokenSecretName           string                `json:"tokenSecretName"`
	TokenSecretKey            string                `json:"tokenSecretKey"`
	ReconcileInterval         string                `json:"reconcileInterval"`
	AttemptTimeout            string                `json:"attemptTimeout"`
	RequestTimeout            string                `json:"requestTimeout"`
	ShutdownTimeout           string                `json:"shutdownTimeout"`
	MaxResponseBytes          int64                 `json:"maxResponseBytes"`
	BackupSpecContract        string                `json:"backupSpecContract"`
	BackupStatusContract      string                `json:"backupStatusContract"`
	ObserverStatusContract    string                `json:"observerStatusContract"`
	ReleaseChannel            string                `json:"releaseChannel"`
	RecoveryLane              string                `json:"recoveryLane"`
	FailureBoundary           string                `json:"failureBoundary"`
	PlanLaneLockKey           string                `json:"planLaneLockKey"`
	CellLockKey               string                `json:"cellLockKey"`
	SharedResourceLockKeys    []string              `json:"sharedResourceLockKeys"`
	IdempotencyKey            string                `json:"idempotencyKey"`
	ComponentPlanArtifactID   string                `json:"componentPlanArtifactId"`
	ComponentPlanScopeKey     string                `json:"componentPlanScopeKey"`
	ComponentPlanGeneration   string                `json:"componentPlanGeneration"`
	ComponentPlanPlanDigest   string                `json:"componentPlanPlanDigest"`
	ComponentPlanCoordination string                `json:"componentPlanCoordinationDigest"`
	ComponentPlanStatusDigest string                `json:"componentPlanStatusDigest"`
	ComponentPlanLaneKey      string                `json:"componentPlanLaneKey"`
	ComponentPlanFence        int64                 `json:"componentPlanFence"`
	ComponentPlanLaneVersion  int64                 `json:"componentPlanLaneVersion"`
	ComponentPlanStatus       ComponentPlanStatusV1 `json:"componentPlanStatus"`
	ObservationOnly           bool                  `json:"observationOnly"`
	ExecutionAllowed          bool                  `json:"executionAllowed"`
	ProductionMutationAllowed bool                  `json:"productionMutationAllowed"`
	RollbackRequired          bool                  `json:"rollbackRequired"`
	RollbackMode              string                `json:"rollbackMode"`
	LastKnownGoodRequired     bool                  `json:"lastKnownGoodRequired"`
	Blockers                  []string              `json:"blockers"`
	Digest                    string                `json:"digest"`
}

// BuildCandidate verifies all duplicated bindings and exactly one isolated
// rendered Deployment. It performs no file, network, process, or cluster I/O.
func BuildCandidate(request CandidateRequest, renderedManifest []byte) (Candidate, error) {
	if err := ValidateCandidateRequest(request); err != nil {
		return Candidate{}, err
	}
	workloadName, manifestDigest, err := validateRenderedManifest(request, renderedManifest)
	if err != nil {
		return Candidate{}, err
	}
	cellID := cellIDFromKey(request.CellKey)
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
		CellKey:                   request.CellKey,
		CellID:                    cellID,
		ReleaseName:               request.ReleaseName,
		ReleaseNamespace:          request.ReleaseNamespace,
		ImageRepository:           request.ImageRepository,
		ImageDigest:               request.ImageDigest,
		ChartDigest:               request.ChartDigest,
		ManifestDigest:            manifestDigest,
		WorkloadName:              workloadName,
		APIBaseURL:                request.APIBaseURL,
		SpecConfigMapName:         request.SpecConfigMapName,
		SpecConfigMapKey:          request.SpecConfigMapKey,
		TokenSecretName:           request.TokenSecretName,
		TokenSecretKey:            request.TokenSecretKey,
		ReconcileInterval:         request.ReconcileInterval,
		AttemptTimeout:            request.AttemptTimeout,
		RequestTimeout:            request.RequestTimeout,
		ShutdownTimeout:           request.ShutdownTimeout,
		MaxResponseBytes:          request.MaxResponseBytes,
		BackupSpecContract:        BackupSpecContractV1,
		BackupStatusContract:      BackupStatusContractV1,
		ObserverStatusContract:    BackupObserverStatusContractV2,
		ReleaseChannel:            CandidateReleaseChannel,
		RecoveryLane:              CandidateRecoveryLane,
		FailureBoundary:           request.CellKey,
		PlanLaneLockKey:           "lane/backup",
		CellLockKey:               "lane/backup/cell/" + cellID,
		SharedResourceLockKeys:    expectedSharedResourceLockKeys(),
		IdempotencyKey:            "backup-observer-shadow/" + cellID + "/" + strings.TrimPrefix(manifestDigest, "sha256:"),
		ComponentPlanArtifactID:   request.ComponentPlanStatus.ArtifactID,
		ComponentPlanScopeKey:     request.ComponentPlanStatus.ScopeKey,
		ComponentPlanGeneration:   request.ComponentPlanStatus.Generation,
		ComponentPlanPlanDigest:   request.ComponentPlanStatus.PlanDigest,
		ComponentPlanCoordination: request.ComponentPlanStatus.CoordinationDigest,
		ComponentPlanStatusDigest: request.ComponentPlanStatus.Digest,
		ComponentPlanLaneKey:      request.ComponentPlanStatus.LaneKey,
		ComponentPlanFence:        request.ComponentPlanStatus.FencingToken,
		ComponentPlanLaneVersion:  request.ComponentPlanStatus.LaneVersion,
		ComponentPlanStatus:       request.ComponentPlanStatus,
		ObservationOnly:           true,
		ExecutionAllowed:          false,
		ProductionMutationAllowed: false,
		RollbackRequired:          true,
		RollbackMode:              CandidateRollbackMode,
		LastKnownGoodRequired:     true,
		Blockers:                  blockers,
	}
	candidate.Digest = DigestCandidate(candidate)
	if err := VerifyCandidate(candidate); err != nil {
		return Candidate{}, err
	}
	return candidate, nil
}

// ValidateCandidateRequest proves the release-control observation is the same
// exact backup-only source plan before the rendered workload is inspected.
func ValidateCandidateRequest(request CandidateRequest) error {
	if request.APIVersion != CandidateRequestAPIVersion || request.Kind != CandidateRequestKind {
		return fmt.Errorf("%w: apiVersion and kind must be %q and %q", ErrCandidate, CandidateRequestAPIVersion, CandidateRequestKind)
	}
	if !exactCommitPattern.MatchString(request.SourceCommit) || !cellKeyPattern.MatchString(request.CellKey) {
		return fmt.Errorf("%w: sourceCommit or cellKey is not canonical", ErrCandidate)
	}
	cellID := cellIDFromKey(request.CellKey)
	if errs := validation.IsDNS1123Label(request.ReleaseName); len(errs) != 0 || request.ReleaseName != "backup-"+cellID {
		return fmt.Errorf("%w: releaseName must be exactly backup-<cellId>", ErrCandidate)
	}
	if request.ReleaseNamespace != "fugue-system" {
		return fmt.Errorf("%w: releaseNamespace must be exactly fugue-system during shadow migration", ErrCandidate)
	}
	if err := validateImageRepository(request.ImageRepository); err != nil {
		return fmt.Errorf("%w: %v", ErrCandidate, err)
	}
	if !exactDigestPattern.MatchString(request.ImageDigest) || !exactDigestPattern.MatchString(request.ChartDigest) {
		return fmt.Errorf("%w: imageDigest and chartDigest must be exact lowercase sha256 digests", ErrCandidate)
	}
	if err := validateAPIBaseURL(request.APIBaseURL); err != nil {
		return fmt.Errorf("%w: %v", ErrCandidate, err)
	}
	if err := validateObjectName(request.SpecConfigMapName, "specConfigMapName"); err != nil {
		return fmt.Errorf("%w: %v", ErrCandidate, err)
	}
	if err := validateObjectName(request.TokenSecretName, "tokenSecretName"); err != nil {
		return fmt.Errorf("%w: %v", ErrCandidate, err)
	}
	if err := validateObjectKey(request.SpecConfigMapKey, "specConfigMapKey"); err != nil {
		return fmt.Errorf("%w: %v", ErrCandidate, err)
	}
	if err := validateObjectKey(request.TokenSecretKey, "tokenSecretKey"); err != nil {
		return fmt.Errorf("%w: %v", ErrCandidate, err)
	}
	reconcileSeconds, err := validateDuration(request.ReconcileInterval, 1, 600)
	if err != nil {
		return fmt.Errorf("%w: reconcileInterval: %v", ErrCandidate, err)
	}
	attemptSeconds, err := validateDuration(request.AttemptTimeout, 1, 60)
	if err != nil {
		return fmt.Errorf("%w: attemptTimeout: %v", ErrCandidate, err)
	}
	requestSeconds, err := validateDuration(request.RequestTimeout, 1, 30)
	if err != nil || requestSeconds >= attemptSeconds {
		return fmt.Errorf("%w: requestTimeout must be canonical and less than attemptTimeout", ErrCandidate)
	}
	if _, err := validateDuration(request.ShutdownTimeout, 1, 60); err != nil {
		return fmt.Errorf("%w: shutdownTimeout: %v", ErrCandidate, err)
	}
	if reconcileSeconds <= 0 || request.MaxResponseBytes <= 0 || request.MaxResponseBytes > 1<<20 {
		return fmt.Errorf("%w: runtime bounds are invalid", ErrCandidate)
	}
	if err := validateComponentPlan(request); err != nil {
		return err
	}
	return nil
}

func validateComponentPlan(request CandidateRequest) error {
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
	if len(plan.ImpactedComponents) != 1 || plan.ImpactedComponents[0].ID != "backup-storage" ||
		plan.ImpactedComponents[0].ReleaseLane != CandidateRecoveryLane || plan.DispatchMode != componentmanifest.DispatchModeShadow ||
		!plan.RequiresLegacyRelease || !coordination.ObservationOnly || coordination.ProductionMutationAllowed ||
		!equalCoordinationScopes(coordination.Scopes, expectedCoordinationScopes()) {
		return fmt.Errorf("%w: component plan is not the exact observation-only backup lane", ErrCandidate)
	}
	return nil
}

// VerifyComponentPlanStatusV1 validates release-control's canonical observed
// status independently, including its digest and server-derived shadow lane.
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

// DigestComponentPlanStatusV1 mirrors release-control v1 canonical JSON while
// keeping the consumer implementation and release adapter dependency one-way.
func DigestComponentPlanStatusV1(status ComponentPlanStatusV1) string {
	status.Digest = ""
	encoded, err := json.Marshal(status)
	if err != nil {
		panic(fmt.Sprintf("marshal component plan status v1: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// VerifyCandidate validates a persisted candidate without trusting its digest.
func VerifyCandidate(candidate Candidate) error {
	if candidate.APIVersion != CandidateAPIVersion || candidate.Kind != CandidateKind || candidate.Policy != CandidatePolicy ||
		!exactCommitPattern.MatchString(candidate.SourceCommit) || !cellKeyPattern.MatchString(candidate.CellKey) ||
		!exactDigestPattern.MatchString(candidate.ImageDigest) || !exactDigestPattern.MatchString(candidate.ChartDigest) ||
		!exactDigestPattern.MatchString(candidate.ManifestDigest) || !exactDigestPattern.MatchString(candidate.ComponentPlanPlanDigest) ||
		!exactDigestPattern.MatchString(candidate.ComponentPlanCoordination) || !exactDigestPattern.MatchString(candidate.ComponentPlanStatusDigest) ||
		candidate.ReleaseChannel != CandidateReleaseChannel || candidate.RecoveryLane != CandidateRecoveryLane ||
		!candidate.ObservationOnly || candidate.ExecutionAllowed || candidate.ProductionMutationAllowed ||
		!candidate.RollbackRequired || candidate.RollbackMode != CandidateRollbackMode || !candidate.LastKnownGoodRequired ||
		candidate.BackupSpecContract != BackupSpecContractV1 || candidate.BackupStatusContract != BackupStatusContractV1 ||
		candidate.ObserverStatusContract != BackupObserverStatusContractV2 || candidate.ComponentPlanFence <= 0 ||
		candidate.ComponentPlanLaneVersion <= 0 || strings.TrimSpace(candidate.ComponentPlanArtifactID) == "" {
		return ErrCandidate
	}
	cellID := cellIDFromKey(candidate.CellKey)
	if candidate.CellID != cellID || candidate.ReleaseName != "backup-"+cellID || candidate.ReleaseNamespace != "fugue-system" ||
		candidate.WorkloadName != "fugue-backup-observer-"+cellID || candidate.FailureBoundary != candidate.CellKey ||
		candidate.PlanLaneLockKey != "lane/backup" ||
		candidate.CellLockKey != "lane/backup/cell/"+cellID ||
		candidate.IdempotencyKey != "backup-observer-shadow/"+cellID+"/"+strings.TrimPrefix(candidate.ManifestDigest, "sha256:") ||
		!equalStringSlices(candidate.SharedResourceLockKeys, expectedSharedResourceLockKeys()) {
		return ErrCandidate
	}
	if validateImageRepository(candidate.ImageRepository) != nil || validateAPIBaseURL(candidate.APIBaseURL) != nil ||
		validateObjectName(candidate.SpecConfigMapName, "spec") != nil || validateObjectName(candidate.TokenSecretName, "token") != nil ||
		validateObjectKey(candidate.SpecConfigMapKey, "spec") != nil || validateObjectKey(candidate.TokenSecretKey, "token") != nil {
		return ErrCandidate
	}
	requestSeconds, requestErr := validateDuration(candidate.RequestTimeout, 1, 30)
	attemptSeconds, attemptErr := validateDuration(candidate.AttemptTimeout, 1, 60)
	if _, err := validateDuration(candidate.ReconcileInterval, 1, 600); err != nil || requestErr != nil || attemptErr != nil || requestSeconds >= attemptSeconds {
		return ErrCandidate
	}
	if _, err := validateDuration(candidate.ShutdownTimeout, 1, 60); err != nil || candidate.MaxResponseBytes <= 0 || candidate.MaxResponseBytes > 1<<20 {
		return ErrCandidate
	}
	if !componentPlanScopeV1.MatchString(candidate.ComponentPlanScopeKey) ||
		candidate.ComponentPlanGeneration != "git-"+candidate.SourceCommit ||
		!strings.HasSuffix(candidate.ComponentPlanScopeKey, ".."+candidate.SourceCommit) ||
		candidate.ComponentPlanLaneKey != strings.Join([]string{"component_release_plan", candidate.ComponentPlanScopeKey, "shadow"}, "|") {
		return ErrCandidate
	}
	status := candidate.ComponentPlanStatus
	if VerifyComponentPlanStatusV1(status) != nil || candidate.ComponentPlanArtifactID != status.ArtifactID ||
		candidate.ComponentPlanScopeKey != status.ScopeKey || candidate.ComponentPlanGeneration != status.Generation ||
		candidate.ComponentPlanPlanDigest != status.PlanDigest || candidate.ComponentPlanCoordination != status.CoordinationDigest ||
		candidate.ComponentPlanStatusDigest != status.Digest || candidate.ComponentPlanLaneKey != status.LaneKey ||
		candidate.ComponentPlanFence != status.FencingToken || candidate.ComponentPlanLaneVersion != status.LaneVersion {
		return ErrCandidate
	}
	if len(candidate.Blockers) == 0 || !sort.StringsAreSorted(candidate.Blockers) || hasDuplicate(candidate.Blockers) ||
		!containsString(candidate.Blockers, "candidate is observation-only and cannot authorize a cluster mutation") ||
		!containsString(candidate.Blockers, "production release freeze must be cleared by the unique coordinator") ||
		candidate.Digest != DigestCandidate(candidate) {
		return ErrCandidate
	}
	return nil
}

// DigestCandidate returns the canonical digest with the self-reference empty.
func DigestCandidate(candidate Candidate) string {
	candidate.Digest = ""
	encoded, err := json.Marshal(candidate)
	if err != nil {
		panic(fmt.Sprintf("marshal backup candidate: %v", err))
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
	if len(objects) != 1 || objects[0].GetAPIVersion() != "apps/v1" || objects[0].GetKind() != "Deployment" {
		return "", "", fmt.Errorf("%w: render must contain exactly one apps/v1 Deployment", ErrCandidate)
	}
	object := objects[0]
	encoded, err := json.Marshal(object.Object)
	if err != nil {
		return "", "", fmt.Errorf("%w: encode rendered Deployment: %v", ErrCandidate, err)
	}
	var deployment appsv1.Deployment
	if err := json.Unmarshal(encoded, &deployment); err != nil {
		return "", "", fmt.Errorf("%w: parse rendered Deployment: %v", ErrCandidate, err)
	}
	cellID := cellIDFromKey(request.CellKey)
	wantName := "fugue-backup-observer-" + cellID
	if deployment.Name != wantName || deployment.GenerateName != "" || deployment.Namespace != request.ReleaseNamespace ||
		len(deployment.OwnerReferences) != 0 || len(deployment.Finalizers) != 0 ||
		!reflect.DeepEqual(deployment.Status, appsv1.DeploymentStatus{}) ||
		deployment.Annotations["fugue.io/backup-cell-key"] != request.CellKey ||
		deployment.Annotations["fugue.io/production-mutation"] != "forbidden" {
		return "", "", fmt.Errorf("%w: rendered Deployment identity is unsafe or unbound", ErrCandidate)
	}
	wantSelector := map[string]string{
		"app.kubernetes.io/name":      "fugue-backup-observer",
		"app.kubernetes.io/instance":  request.ReleaseName,
		"app.kubernetes.io/component": "backup-observer",
		"fugue.io/backup-cell-id":     cellID,
	}
	for key, value := range map[string]string{
		"app.kubernetes.io/name":       "fugue-backup-observer",
		"app.kubernetes.io/instance":   request.ReleaseName,
		"app.kubernetes.io/component":  "backup-observer",
		"fugue.io/backup-cell-id":      cellID,
		"fugue.io/release-lane":        CandidateRecoveryLane,
		"fugue.io/ownership-mode":      "shadow",
		"fugue.io/production-mutation": "forbidden",
	} {
		if deployment.Labels[key] != value || deployment.Spec.Template.Labels[key] != value {
			return "", "", fmt.Errorf("%w: rendered Deployment label %s is not bound", ErrCandidate, key)
		}
	}
	if deployment.Spec.Selector == nil || len(deployment.Spec.Selector.MatchExpressions) != 0 ||
		!equalStringMap(deployment.Spec.Selector.MatchLabels, wantSelector) ||
		deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 1 ||
		deployment.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType || deployment.Spec.Strategy.RollingUpdate != nil ||
		deployment.Spec.RevisionHistoryLimit == nil || *deployment.Spec.RevisionHistoryLimit != 2 || deployment.Spec.Paused ||
		deployment.Spec.MinReadySeconds != 0 || deployment.Spec.ProgressDeadlineSeconds != nil {
		return "", "", fmt.Errorf("%w: rendered Deployment singleton or selector boundary drifted", ErrCandidate)
	}
	pod := deployment.Spec.Template.Spec
	shutdownSeconds, _ := validateDuration(request.ShutdownTimeout, 1, 60)
	if deployment.Spec.Template.Annotations["fugue.io/backup-cell-key"] != request.CellKey ||
		deployment.Spec.Template.Annotations["fugue.io/production-mutation"] != "forbidden" ||
		pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken || pod.ServiceAccountName != "" ||
		pod.EnableServiceLinks == nil || *pod.EnableServiceLinks || pod.HostNetwork || pod.HostPID || pod.HostIPC ||
		pod.DNSPolicy != corev1.DNSClusterFirst || pod.RestartPolicy != corev1.RestartPolicyAlways ||
		pod.TerminationGracePeriodSeconds == nil || *pod.TerminationGracePeriodSeconds != int64(shutdownSeconds+5) ||
		len(pod.InitContainers) != 0 || len(pod.EphemeralContainers) != 0 || len(pod.Containers) != 1 || len(pod.Volumes) != 2 ||
		len(pod.ImagePullSecrets) != 0 || len(pod.NodeSelector) != 0 || len(pod.Tolerations) != 0 || pod.Affinity != nil ||
		len(pod.TopologySpreadConstraints) != 0 || pod.PriorityClassName != "" {
		return "", "", fmt.Errorf("%w: rendered Pod identity, network, or scheduling boundary drifted", ErrCandidate)
	}
	container := pod.Containers[0]
	if container.Name != "backup-observer" || container.Image != request.ImageRepository+"@"+request.ImageDigest ||
		container.ImagePullPolicy != corev1.PullIfNotPresent || len(container.Ports) != 0 || len(container.Command) != 0 ||
		len(container.Args) != 0 || len(container.EnvFrom) != 0 || len(container.VolumeDevices) != 0 ||
		!validateEnvironment(request, container.Env) || !validateVolumeBoundary(request, pod.Volumes, container.VolumeMounts) ||
		!validateSecurityBoundary(pod, container) || !validateResourceBoundary(container.Resources) ||
		!validateExecProbe(container.StartupProbe, "health", 2, 2, 30) ||
		!validateExecProbe(container.LivenessProbe, "health", 10, 2, 3) ||
		!validateExecProbe(container.ReadinessProbe, "ready", 5, 2, 2) {
		return "", "", fmt.Errorf("%w: rendered observer artifact or isolation boundary drifted", ErrCandidate)
	}
	manifestDigest := sha256.Sum256(encoded)
	return deployment.Name, "sha256:" + hex.EncodeToString(manifestDigest[:]), nil
}

func validateEnvironment(request CandidateRequest, environment []corev1.EnvVar) bool {
	want := map[string]string{
		"FUGUE_BACKUP_OBSERVER_ENABLED":            "true",
		"FUGUE_BACKUP_OBSERVER_BIND_ADDR":          "127.0.0.1:8092",
		"FUGUE_BACKUP_OBSERVER_CELL_KEY":           request.CellKey,
		"FUGUE_BACKUP_OBSERVER_SPEC_FILE":          "/run/fugue/backup-observer/spec/spec.json",
		"FUGUE_BACKUP_OBSERVER_TOKEN_FILE":         "/run/fugue/backup-observer/token/token",
		"FUGUE_BACKUP_OBSERVER_API_BASE_URL":       request.APIBaseURL,
		"FUGUE_BACKUP_OBSERVER_RECONCILE_INTERVAL": request.ReconcileInterval,
		"FUGUE_BACKUP_OBSERVER_ATTEMPT_TIMEOUT":    request.AttemptTimeout,
		"FUGUE_BACKUP_OBSERVER_REQUEST_TIMEOUT":    request.RequestTimeout,
		"FUGUE_BACKUP_OBSERVER_SHUTDOWN_TIMEOUT":   request.ShutdownTimeout,
		"FUGUE_BACKUP_OBSERVER_MAX_RESPONSE_BYTES": strconv.FormatInt(request.MaxResponseBytes, 10),
	}
	if len(environment) != len(want) {
		return false
	}
	for _, variable := range environment {
		value, exists := want[variable.Name]
		if !exists || variable.ValueFrom != nil || variable.Value != value {
			return false
		}
		delete(want, variable.Name)
	}
	return len(want) == 0
}

func validateVolumeBoundary(request CandidateRequest, volumes []corev1.Volume, mounts []corev1.VolumeMount) bool {
	if len(volumes) != 2 || len(mounts) != 2 {
		return false
	}
	seenSpec := false
	seenToken := false
	for _, volume := range volumes {
		switch volume.Name {
		case "desired-spec":
			defaultMode := int32(0o444)
			optional := false
			want := corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: request.SpecConfigMapName},
				Items:                []corev1.KeyToPath{{Key: request.SpecConfigMapKey, Path: "spec.json"}},
				DefaultMode:          &defaultMode,
				Optional:             &optional,
			}}
			if seenSpec || !reflect.DeepEqual(volume.VolumeSource, want) {
				return false
			}
			seenSpec = true
		case "observer-token":
			defaultMode := int32(0o440)
			optional := false
			want := corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  request.TokenSecretName,
				Items:       []corev1.KeyToPath{{Key: request.TokenSecretKey, Path: "token"}},
				DefaultMode: &defaultMode,
				Optional:    &optional,
			}}
			if seenToken || !reflect.DeepEqual(volume.VolumeSource, want) {
				return false
			}
			seenToken = true
		default:
			return false
		}
	}
	wantMounts := map[string]string{
		"desired-spec":   "/run/fugue/backup-observer/spec",
		"observer-token": "/run/fugue/backup-observer/token",
	}
	for _, mount := range mounts {
		wantPath, exists := wantMounts[mount.Name]
		if !exists || !reflect.DeepEqual(mount, corev1.VolumeMount{Name: mount.Name, MountPath: wantPath, ReadOnly: true}) {
			return false
		}
		delete(wantMounts, mount.Name)
	}
	return seenSpec && seenToken && len(wantMounts) == 0
}

func validateSecurityBoundary(pod corev1.PodSpec, container corev1.Container) bool {
	trueValue := true
	falseValue := false
	runAs := int64(65532)
	fsGroupPolicy := corev1.FSGroupChangeOnRootMismatch
	wantPod := &corev1.PodSecurityContext{
		RunAsUser:           &runAs,
		RunAsGroup:          &runAs,
		RunAsNonRoot:        &trueValue,
		FSGroup:             &runAs,
		FSGroupChangePolicy: &fsGroupPolicy,
		SeccompProfile:      &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	wantContainer := &corev1.SecurityContext{
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		Privileged:               &falseValue,
		RunAsUser:                &runAs,
		RunAsGroup:               &runAs,
		RunAsNonRoot:             &trueValue,
		ReadOnlyRootFilesystem:   &trueValue,
		AllowPrivilegeEscalation: &falseValue,
	}
	return reflect.DeepEqual(pod.SecurityContext, wantPod) && reflect.DeepEqual(container.SecurityContext, wantContainer)
}

func validateResourceBoundary(resources corev1.ResourceRequirements) bool {
	if len(resources.Requests) != 3 || len(resources.Limits) != 3 {
		return false
	}
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
		request, requestExists := resources.Requests[name]
		limit, limitExists := resources.Limits[name]
		if !requestExists || !limitExists || request.IsZero() || limit.IsZero() || limit.Cmp(request) < 0 {
			return false
		}
	}
	return true
}

func validateExecProbe(probe *corev1.Probe, target string, period, timeout, failures int32) bool {
	want := []string{"/usr/local/bin/fugue-backup-observer", "probe", target}
	if probe == nil || probe.Exec == nil || probe.HTTPGet != nil || probe.TCPSocket != nil || probe.GRPC != nil ||
		probe.PeriodSeconds != period || probe.TimeoutSeconds != timeout || probe.FailureThreshold != failures ||
		probe.InitialDelaySeconds != 0 || probe.SuccessThreshold != 0 || probe.TerminationGracePeriodSeconds != nil ||
		len(probe.Exec.Command) != len(want) {
		return false
	}
	for index := range want {
		if probe.Exec.Command[index] != want[index] {
			return false
		}
	}
	return true
}

func validateImageRepository(repository string) error {
	if strings.TrimSpace(repository) != repository || !imageRepositoryPattern.MatchString(repository) ||
		!strings.HasSuffix(repository, "/fugue-backup-observer") {
		return errors.New("imageRepository must be a fully qualified dedicated fugue-backup-observer repository")
	}
	authority := strings.SplitN(repository, "/", 2)[0]
	if index := strings.LastIndex(authority, ":"); index >= 0 {
		portNumber, err := strconv.Atoi(authority[index+1:])
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return errors.New("imageRepository registry port must be between 1 and 65535")
		}
	}
	return nil
}

func validateAPIBaseURL(raw string) error {
	if strings.TrimSpace(raw) != raw || raw == "" || strings.ContainsAny(raw, "\r\n\t ") || strings.HasSuffix(raw, "/") {
		return errors.New("apiBaseUrl must be an exact canonical HTTPS URL")
	}
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Hostname() == "" ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" || parsed.RawPath != "" ||
		(parsed.Path != "" && path.Clean(parsed.Path) != parsed.Path) {
		return errors.New("apiBaseUrl must be absolute HTTPS without credentials, query, fragment, or non-canonical path")
	}
	if portValue := parsed.Port(); portValue != "" {
		portNumber, portErr := strconv.Atoi(portValue)
		if portErr != nil || portNumber < 1 || portNumber > 65535 {
			return errors.New("apiBaseUrl port must be between 1 and 65535")
		}
	}
	return nil
}

func validateObjectName(value, label string) error {
	if strings.TrimSpace(value) != value || len(validation.IsDNS1123Label(value)) != 0 {
		return fmt.Errorf("%s must be an exact lowercase DNS label", label)
	}
	return nil
}

func validateObjectKey(value, label string) error {
	if strings.TrimSpace(value) != value || value == "" || len(value) > 253 || !objectKeyPattern.MatchString(value) {
		return fmt.Errorf("%s must be a canonical ConfigMap/Secret key", label)
	}
	return nil
}

func validateDuration(value string, minimum, maximum int) (int, error) {
	if !wholeSecondPattern.MatchString(value) {
		return 0, errors.New("must be a positive whole-second duration")
	}
	parsed, err := time.ParseDuration(value)
	seconds := int(parsed / time.Second)
	if err != nil || seconds < minimum || seconds > maximum || parsed != time.Duration(seconds)*time.Second {
		return 0, fmt.Errorf("must be between %ds and %ds", minimum, maximum)
	}
	return seconds, nil
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

func cellIDFromKey(cellKey string) string {
	return strings.ReplaceAll(strings.TrimPrefix(cellKey, "backup/"), "/", "-")
}

func expectedCoordinationScopes() []componentmanifest.CoordinationScope {
	return []componentmanifest.CoordinationScope{
		{Key: "lane/backup", ScopeType: "lane", Owner: "backup-storage", ConflictMode: "exclusive"},
		{Key: "legacy-release/fugue", ScopeType: "legacy-release", Owner: "release-control", ConflictMode: "exclusive"},
		{Key: "resource/control-plane-postgres", ScopeType: "resource", Owner: "control-plane", ConflictMode: "exclusive"},
		{Key: "resource/legacy-fugue-helm-release", ScopeType: "resource", Owner: "release-control", ConflictMode: "exclusive"},
	}
}

func expectedSharedResourceLockKeys() []string {
	return []string{
		"legacy-release/fugue",
		"resource/control-plane-postgres",
		"resource/legacy-fugue-helm-release",
	}
}

func equalCoordinationScopes(left, right []componentmanifest.CoordinationScope) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
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

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
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

package releasedomain

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	ControlPlaneHotfixAdoptionAPIVersion    = "release-domain.fugue.dev/v1"
	ControlPlaneHotfixAdoptionPlanKind      = "ControlPlaneHotfixBaselineAdoptionPlan"
	ControlPlaneHotfixAdoptionWALKind       = "ControlPlaneHotfixBaselineAdoptionWAL"
	ControlPlaneHotfixAdoptionPolicy        = "control-plane-hotfix-baseline-adoption-v1"
	ControlPlaneAPIHotfixRolloutPolicyV2    = "control-plane-api-hotfix-rollout-v2"
	ControlPlaneControllerM16PolicyV3       = "control-plane-controller-m16-rollout-v1"
	controlPlaneHotfixManifestObjects       = 79
	controlPlaneHotfixBaseRevision          = 806
	controlPlaneAPIHotfixBaseRevisionV2     = 819
	controlPlaneAPIHotfixTargetSourceV2     = "57dc767999741cea25fe4820a6c9603984dfa0b9"
	controlPlaneAPIHotfixHybridSourceV2     = "a0f5bc0ac36b4e29c4c7928dda1923c2c4727759"
	controlPlaneAPIHotfixHybridImageV2      = "ghcr.io/yym68686/fugue-api@sha256:7eb7e7682d44c3f283cd347e032de6fac2f6304221fbf72dfa788845950ccfd9"
	controlPlaneControllerM16BaseRevisionV3 = 820
	controlPlaneControllerM16TargetSourceV3 = "58fc2e560064214e3f329765c9ec7839ee513c27"
	controlPlaneControllerM16HybridSourceV3 = "d1e7ed9cdedbaa09db9bd78b4e433b94c7357510"
	controlPlaneControllerM16TargetImageV3  = "ghcr.io/yym68686/fugue-controller@sha256:444bca23386cc0f19012fcbaba20d71db1b9863ee80d50d1bde6d87376e190df"
	controlPlaneControllerM16HybridImageV3  = "ghcr.io/yym68686/fugue-controller@sha256:e636b35fe8718e1f20895c0a290924a0d48a6cb7d1072d741612df18483fa13d"
)

var (
	hotfixSHA256 = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	hotfixSHA    = regexp.MustCompile(`^[0-9a-f]{40}$`)
	hotfixName   = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)
)

type ControlPlaneHotfixProvenance struct {
	BuildRunID             string `json:"buildRunId"`
	BuildRunAttempt        int    `json:"buildRunAttempt"`
	ArtifactName           string `json:"artifactName"`
	ArtifactDigest         string `json:"artifactDigest"`
	Repository             string `json:"repository"`
	IndexDigest            string `json:"indexDigest"`
	PlatformManifestDigest string `json:"platformManifestDigest"`
	ConfigDigest           string `json:"configDigest"`
	OCIRevision            string `json:"ociRevision"`
	Verified               bool   `json:"verified"`
}

type ControlPlaneHotfixKubernetesEvidence struct {
	APIName                      string `json:"apiName"`
	APIUID                       string `json:"apiUid"`
	APIResourceVersion           string `json:"apiResourceVersion"`
	APIGeneration                int64  `json:"apiGeneration"`
	APIObservedGeneration        int64  `json:"apiObservedGeneration"`
	APITemplateDigest            string `json:"apiTemplateDigest"`
	APIImageRef                  string `json:"apiImageRef"`
	APIImageID                   string `json:"apiImageId"`
	APIHealthDigest              string `json:"apiHealthDigest"`
	APIReplicas                  int64  `json:"apiReplicas"`
	APIReady                     int64  `json:"apiReady"`
	APIUpdated                   int64  `json:"apiUpdated"`
	APIAvailable                 int64  `json:"apiAvailable"`
	APIUnavailable               int64  `json:"apiUnavailable"`
	ServiceName                  string `json:"serviceName"`
	ServiceUID                   string `json:"serviceUid"`
	ServiceResourceVersion       string `json:"serviceResourceVersion"`
	ServiceSelectorDigest        string `json:"serviceSelectorDigest"`
	EndpointSliceName            string `json:"endpointSliceName"`
	EndpointSliceUID             string `json:"endpointSliceUid"`
	EndpointSliceResourceVersion string `json:"endpointSliceResourceVersion"`
	EndpointServiceName          string `json:"endpointServiceName"`
	EndpointBindingDigest        string `json:"endpointBindingDigest"`
	ReadyServingEndpoints        int64  `json:"readyServingEndpoints"`
	FrozenNonAPIWorkloadDigest   string `json:"frozenNonApiWorkloadDigest,omitempty"`
	ControllerName               string `json:"controllerName,omitempty"`
	ControllerUID                string `json:"controllerUid,omitempty"`
	ControllerResourceVersion    string `json:"controllerResourceVersion,omitempty"`
	ControllerGeneration         int64  `json:"controllerGeneration,omitempty"`
	ControllerObservedGeneration int64  `json:"controllerObservedGeneration,omitempty"`
	ControllerTemplateDigest     string `json:"controllerTemplateDigest,omitempty"`
	ControllerImageRef           string `json:"controllerImageRef,omitempty"`
	ControllerImageID            string `json:"controllerImageId,omitempty"`
	ControllerReplicas           int64  `json:"controllerReplicas,omitempty"`
	ControllerReady              int64  `json:"controllerReady,omitempty"`
	ControllerUpdated            int64  `json:"controllerUpdated,omitempty"`
	ControllerAvailable          int64  `json:"controllerAvailable,omitempty"`
	ControllerUnavailable        int64  `json:"controllerUnavailable,omitempty"`
	ControllerLeaderLeaseName    string `json:"controllerLeaderLeaseName,omitempty"`
	ControllerLeaderLeaseUID     string `json:"controllerLeaderLeaseUid,omitempty"`
	ControllerLeaderLeaseVersion string `json:"controllerLeaderLeaseResourceVersion,omitempty"`
	ControllerLeaderHolder       string `json:"controllerLeaderHolder,omitempty"`
	ControllerMetricsDigest      string `json:"controllerMetricsDigest,omitempty"`
	ControllerLKGDigest          string `json:"controllerLkgDigest,omitempty"`
	FrozenNonControllerDigest    string `json:"frozenNonControllerWorkloadDigest,omitempty"`
}

type ControlPlaneHotfixLeaseEvidence struct {
	Namespace        string `json:"namespace"`
	Name             string `json:"name"`
	UID              string `json:"uid"`
	ResourceVersion  string `json:"resourceVersion"`
	HolderIdentity   string `json:"holderIdentity"`
	RecoveryRequired bool   `json:"recoveryRequired"`
}

type ControlPlaneHotfixAdoptionInput struct {
	PlanVersion                  int                                  `json:"planVersion,omitempty"`
	ExpectedSHA                  string                               `json:"expectedSha"`
	RunID                        string                               `json:"runId"`
	RunAttempt                   int                                  `json:"runAttempt"`
	Namespace                    string                               `json:"namespace"`
	ReleaseName                  string                               `json:"releaseName"`
	ReleaseFullname              string                               `json:"releaseFullname"`
	HelmRevision                 int64                                `json:"helmRevision"`
	HelmStatus                   string                               `json:"helmStatus"`
	HelmRecordDigest             string                               `json:"helmRecordDigest"`
	BaseValuesDigest             string                               `json:"baseValuesDigest"`
	TargetValuesDigest           string                               `json:"targetValuesDigest"`
	HybridValuesDigest           string                               `json:"hybridValuesDigest,omitempty"`
	RawTargetManifestDigest      string                               `json:"rawTargetManifestDigest,omitempty"`
	RawHybridManifestDigest      string                               `json:"rawHybridManifestDigest,omitempty"`
	TargetPostRenderDigest       string                               `json:"targetPostRenderDigest,omitempty"`
	HybridPostRenderDigest       string                               `json:"hybridPostRenderDigest,omitempty"`
	NonAPIEdgeRestorePlanDigest  string                               `json:"nonApiEdgeRestorePlanDigest,omitempty"`
	ChartTreeDigest              string                               `json:"chartTreeDigest"`
	CurrentSource                string                               `json:"currentSource"`
	AdoptedSource                string                               `json:"adoptedSource"`
	LiveImageRef                 string                               `json:"liveImageRef"`
	TargetAPIImageRef            string                               `json:"targetApiImageRef,omitempty"`
	LiveHybridAPIImageRef        string                               `json:"liveHybridApiImageRef,omitempty"`
	TargetAPIImageID             string                               `json:"targetApiImageId,omitempty"`
	TargetControllerImageRef     string                               `json:"targetControllerImageRef,omitempty"`
	LiveHybridControllerImageRef string                               `json:"liveHybridControllerImageRef,omitempty"`
	TargetControllerImageID      string                               `json:"targetControllerImageId,omitempty"`
	Fence                        string                               `json:"fence"`
	Nonce                        string                               `json:"nonce"`
	Confirm                      string                               `json:"confirm"`
	Provenance                   ControlPlaneHotfixProvenance         `json:"provenance"`
	Kubernetes                   ControlPlaneHotfixKubernetesEvidence `json:"kubernetes"`
	Lease                        ControlPlaneHotfixLeaseEvidence      `json:"lease"`
	BaseManifest                 []byte                               `json:"-"`
	TargetManifest               []byte                               `json:"-"`
	RepeatedTarget               []byte                               `json:"-"`
	HybridManifest               []byte                               `json:"-"`
}

type ControlPlaneHotfixAdoptionPlan struct {
	PlanVersion                    int                                  `json:"planVersion,omitempty"`
	APIVersion                     string                               `json:"apiVersion"`
	Kind                           string                               `json:"kind"`
	Policy                         string                               `json:"policy"`
	ExpectedSHA                    string                               `json:"expectedSha"`
	RunID                          string                               `json:"runId"`
	RunAttempt                     int                                  `json:"runAttempt"`
	Namespace                      string                               `json:"namespace"`
	ReleaseName                    string                               `json:"releaseName"`
	ReleaseFullname                string                               `json:"releaseFullname"`
	BaseRevision                   int64                                `json:"baseRevision"`
	TargetRevision                 int64                                `json:"targetRevision"`
	BaseStatus                     string                               `json:"baseStatus"`
	HelmRecordDigest               string                               `json:"helmRecordDigest"`
	BaseValuesDigest               string                               `json:"baseValuesDigest"`
	TargetValuesDigest             string                               `json:"targetValuesDigest"`
	HybridValuesDigest             string                               `json:"hybridValuesDigest,omitempty"`
	RawTargetManifestDigest        string                               `json:"rawTargetManifestDigest,omitempty"`
	RawHybridManifestDigest        string                               `json:"rawHybridManifestDigest,omitempty"`
	TargetPostRenderDigest         string                               `json:"targetPostRenderDigest,omitempty"`
	HybridPostRenderDigest         string                               `json:"hybridPostRenderDigest,omitempty"`
	NonAPIEdgeRestorePlanDigest    string                               `json:"nonApiEdgeRestorePlanDigest,omitempty"`
	ChartTreeDigest                string                               `json:"chartTreeDigest"`
	BaseManifestDigest             string                               `json:"baseManifestDigest"`
	TargetManifestDigest           string                               `json:"targetManifestDigest"`
	HybridManifestDigest           string                               `json:"hybridManifestDigest"`
	TargetAPITemplateDigest        string                               `json:"targetApiTemplateDigest"`
	HybridAPITemplateDigest        string                               `json:"hybridApiTemplateDigest"`
	TargetControllerTemplateDigest string                               `json:"targetControllerTemplateDigest,omitempty"`
	HybridControllerTemplateDigest string                               `json:"hybridControllerTemplateDigest,omitempty"`
	CurrentSource                  string                               `json:"currentSource"`
	AdoptedSource                  string                               `json:"adoptedSource"`
	LiveImageRef                   string                               `json:"liveImageRef"`
	TargetAPIImageRef              string                               `json:"targetApiImageRef,omitempty"`
	LiveHybridAPIImageRef          string                               `json:"liveHybridApiImageRef,omitempty"`
	TargetAPIImageID               string                               `json:"targetApiImageId,omitempty"`
	TargetControllerImageRef       string                               `json:"targetControllerImageRef,omitempty"`
	LiveHybridControllerImageRef   string                               `json:"liveHybridControllerImageRef,omitempty"`
	TargetControllerImageID        string                               `json:"targetControllerImageId,omitempty"`
	Fence                          string                               `json:"fence"`
	Nonce                          string                               `json:"nonce"`
	Provenance                     ControlPlaneHotfixProvenance         `json:"provenance"`
	Kubernetes                     ControlPlaneHotfixKubernetesEvidence `json:"kubernetes"`
	Lease                          ControlPlaneHotfixLeaseEvidence      `json:"lease"`
	Digest                         string                               `json:"digest"`
}

type ControlPlaneHotfixAdoptionWAL struct {
	APIVersion           string `json:"apiVersion"`
	Kind                 string `json:"kind"`
	Policy               string `json:"policy"`
	PlanDigest           string `json:"planDigest"`
	Nonce                string `json:"nonce"`
	Fence                string `json:"fence"`
	Phase                string `json:"phase"`
	Sequence             int    `json:"sequence"`
	ForwardAttempts      int    `json:"forwardAttempts"`
	CompensationAttempts int    `json:"compensationAttempts"`
	RecoveryRequired     bool   `json:"recoveryRequired"`
	Digest               string `json:"digest"`
}

func BuildControlPlaneHotfixAdoptionPlan(input ControlPlaneHotfixAdoptionInput) (ControlPlaneHotfixAdoptionPlan, error) {
	if err := validateControlPlaneHotfixInput(input); err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, err
	}
	base, err := canonicalHotfixManifest(input.BaseManifest)
	if err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("base manifest: %w", err)
	}
	target, err := canonicalHotfixManifest(input.TargetManifest)
	if err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("target manifest: %w", err)
	}
	repeated, err := canonicalHotfixManifest(input.RepeatedTarget)
	if err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("repeated target manifest: %w", err)
	}
	hybrid, err := canonicalHotfixManifest(input.HybridManifest)
	if err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("hybrid manifest: %w", err)
	}
	if !bytes.Equal(target, repeated) {
		return ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("target render is not deterministic")
	}
	if err := verifyHotfixTransition(base, target, hybrid, input); err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, err
	}
	workloadName := input.Kubernetes.APIName
	observedTemplateEvidence := input.Kubernetes.APITemplateDigest
	if input.PlanVersion == 3 {
		workloadName = input.Kubernetes.ControllerName
		observedTemplateEvidence = input.Kubernetes.ControllerTemplateDigest
	}
	baseTemplateDigest, err := hotfixManifestTemplateDigest(base, input.Namespace, workloadName)
	if err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, err
	}
	targetTemplateDigest, err := hotfixManifestTemplateDigest(target, input.Namespace, workloadName)
	if err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, err
	}
	hybridTemplateDigest, err := hotfixManifestTemplateDigest(hybrid, input.Namespace, workloadName)
	if err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, err
	}
	observedTemplateDigest := baseTemplateDigest
	if input.PlanVersion == 2 || input.PlanVersion == 3 {
		observedTemplateDigest = hybridTemplateDigest
	}
	if observedTemplateDigest != observedTemplateEvidence {
		return ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("hotfix live workload template evidence drifted")
	}
	policy := ControlPlaneHotfixAdoptionPolicy
	if input.PlanVersion == 2 {
		policy = ControlPlaneAPIHotfixRolloutPolicyV2
	} else if input.PlanVersion == 3 {
		policy = ControlPlaneControllerM16PolicyV3
	}
	plan := ControlPlaneHotfixAdoptionPlan{
		PlanVersion: input.PlanVersion,
		APIVersion:  ControlPlaneHotfixAdoptionAPIVersion, Kind: ControlPlaneHotfixAdoptionPlanKind,
		Policy: policy, ExpectedSHA: input.ExpectedSHA, RunID: input.RunID,
		RunAttempt: input.RunAttempt, Namespace: input.Namespace, ReleaseName: input.ReleaseName,
		ReleaseFullname: input.ReleaseFullname, BaseRevision: input.HelmRevision,
		TargetRevision: input.HelmRevision + 1, BaseStatus: input.HelmStatus,
		HelmRecordDigest: input.HelmRecordDigest, BaseValuesDigest: input.BaseValuesDigest,
		TargetValuesDigest: input.TargetValuesDigest, HybridValuesDigest: input.HybridValuesDigest,
		RawTargetManifestDigest:     input.RawTargetManifestDigest,
		RawHybridManifestDigest:     input.RawHybridManifestDigest,
		TargetPostRenderDigest:      input.TargetPostRenderDigest,
		HybridPostRenderDigest:      input.HybridPostRenderDigest,
		NonAPIEdgeRestorePlanDigest: input.NonAPIEdgeRestorePlanDigest,
		ChartTreeDigest:             input.ChartTreeDigest, BaseManifestDigest: hotfixDigest(base),
		TargetManifestDigest: hotfixDigest(target), HybridManifestDigest: hotfixDigest(hybrid),
		TargetAPITemplateDigest: targetTemplateDigest, HybridAPITemplateDigest: hybridTemplateDigest,
		CurrentSource: input.CurrentSource, AdoptedSource: input.AdoptedSource,
		LiveImageRef: input.LiveImageRef, TargetAPIImageRef: input.TargetAPIImageRef,
		LiveHybridAPIImageRef: input.LiveHybridAPIImageRef, TargetAPIImageID: input.TargetAPIImageID,
		TargetControllerImageRef:     input.TargetControllerImageRef,
		LiveHybridControllerImageRef: input.LiveHybridControllerImageRef,
		TargetControllerImageID:      input.TargetControllerImageID,
		Fence:                        input.Fence, Nonce: input.Nonce,
		Provenance: input.Provenance, Kubernetes: input.Kubernetes, Lease: input.Lease,
	}
	if input.PlanVersion == 3 {
		plan.TargetAPITemplateDigest = ""
		plan.HybridAPITemplateDigest = ""
		plan.TargetControllerTemplateDigest = targetTemplateDigest
		plan.HybridControllerTemplateDigest = hybridTemplateDigest
	}
	plan.Digest = controlPlaneHotfixPlanDigest(plan)
	return plan, nil
}

// BuildControlPlaneHotfixAdoptionPlanFromRenderSet strips rendered Secret
// objects inside the caller's private evidence directory before applying the
// same exact two-pointer plan construction contract.
func BuildControlPlaneHotfixAdoptionPlanFromRenderSet(input ControlPlaneHotfixAdoptionInput) (ControlPlaneHotfixAdoptionPlan, error) {
	var err error
	input.BaseManifest, err = canonicalSecretFreeHotfixManifest(input.BaseManifest)
	if err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("base manifest: %w", err)
	}
	input.TargetManifest, err = canonicalSecretFreeHotfixManifest(input.TargetManifest)
	if err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("target manifest: %w", err)
	}
	input.RepeatedTarget, err = canonicalSecretFreeHotfixManifest(input.RepeatedTarget)
	if err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("repeated target manifest: %w", err)
	}
	input.HybridManifest, err = canonicalSecretFreeHotfixManifest(input.HybridManifest)
	if err != nil {
		return ControlPlaneHotfixAdoptionPlan{}, fmt.Errorf("hybrid manifest: %w", err)
	}
	return BuildControlPlaneHotfixAdoptionPlan(input)
}

func VerifyControlPlaneHotfixAdoptionPlan(plan ControlPlaneHotfixAdoptionPlan) error {
	if plan.APIVersion != ControlPlaneHotfixAdoptionAPIVersion || plan.Kind != ControlPlaneHotfixAdoptionPlanKind {
		return fmt.Errorf("hotfix plan identity is invalid")
	}
	if plan.TargetRevision != plan.BaseRevision+1 || plan.BaseStatus != "deployed" {
		return fmt.Errorf("hotfix revision pair is invalid")
	}
	if plan.PlanVersion == 0 {
		if plan.Policy != ControlPlaneHotfixAdoptionPolicy || plan.BaseRevision != controlPlaneHotfixBaseRevision || plan.TargetAPIImageRef != "" || plan.LiveHybridAPIImageRef != "" || plan.TargetAPIImageID != "" || plan.HybridValuesDigest != "" || plan.RawTargetManifestDigest != "" || plan.RawHybridManifestDigest != "" || plan.TargetPostRenderDigest != "" || plan.HybridPostRenderDigest != "" || plan.NonAPIEdgeRestorePlanDigest != "" {
			return fmt.Errorf("hotfix v1 identity is invalid")
		}
	} else if plan.PlanVersion == 2 {
		if plan.Policy != ControlPlaneAPIHotfixRolloutPolicyV2 || plan.BaseRevision != controlPlaneAPIHotfixBaseRevisionV2 || plan.CurrentSource != controlPlaneAPIHotfixHybridSourceV2 || plan.AdoptedSource != controlPlaneAPIHotfixTargetSourceV2 || plan.LiveHybridAPIImageRef != controlPlaneAPIHotfixHybridImageV2 || plan.TargetAPIImageRef == plan.LiveHybridAPIImageRef || !hotfixSHA256.MatchString(plan.HybridValuesDigest) || !hotfixSHA256.MatchString(plan.RawTargetManifestDigest) || !hotfixSHA256.MatchString(plan.RawHybridManifestDigest) || !hotfixSHA256.MatchString(plan.TargetPostRenderDigest) || !hotfixSHA256.MatchString(plan.HybridPostRenderDigest) || !hotfixSHA256.MatchString(plan.NonAPIEdgeRestorePlanDigest) {
			return fmt.Errorf("hotfix v2 identity is invalid")
		}
	} else if plan.PlanVersion == 3 {
		if plan.Policy != ControlPlaneControllerM16PolicyV3 || plan.BaseRevision != controlPlaneControllerM16BaseRevisionV3 || plan.CurrentSource != controlPlaneControllerM16HybridSourceV3 || plan.AdoptedSource != controlPlaneControllerM16TargetSourceV3 || plan.TargetControllerImageRef != controlPlaneControllerM16TargetImageV3 || plan.LiveHybridControllerImageRef != controlPlaneControllerM16HybridImageV3 || plan.TargetControllerImageRef == plan.LiveHybridControllerImageRef || plan.TargetAPIImageRef != "" || plan.LiveHybridAPIImageRef != "" || plan.TargetAPIImageID != "" || !hotfixSHA256.MatchString(plan.HybridValuesDigest) || !hotfixSHA256.MatchString(plan.RawTargetManifestDigest) || !hotfixSHA256.MatchString(plan.RawHybridManifestDigest) || !hotfixSHA256.MatchString(plan.TargetPostRenderDigest) || !hotfixSHA256.MatchString(plan.HybridPostRenderDigest) || !hotfixSHA256.MatchString(plan.NonAPIEdgeRestorePlanDigest) {
			return fmt.Errorf("hotfix v3 Controller identity is invalid")
		}
	} else {
		return fmt.Errorf("hotfix plan version is invalid")
	}
	if !hotfixSHA.MatchString(plan.ExpectedSHA) || !hotfixSHA.MatchString(plan.CurrentSource) || !hotfixSHA.MatchString(plan.AdoptedSource) {
		return fmt.Errorf("hotfix source identity is invalid")
	}
	if !hotfixSHA256.MatchString(plan.HelmRecordDigest) || !hotfixSHA256.MatchString(plan.BaseValuesDigest) || !hotfixSHA256.MatchString(plan.TargetValuesDigest) || !hotfixSHA256.MatchString(plan.ChartTreeDigest) || !hotfixSHA256.MatchString(plan.BaseManifestDigest) || !hotfixSHA256.MatchString(plan.TargetManifestDigest) || !hotfixSHA256.MatchString(plan.HybridManifestDigest) {
		return fmt.Errorf("hotfix plan digest material is invalid")
	}
	if (plan.PlanVersion != 3 && (!hotfixSHA256.MatchString(plan.TargetAPITemplateDigest) || !hotfixSHA256.MatchString(plan.HybridAPITemplateDigest))) ||
		(plan.PlanVersion == 3 && (!hotfixSHA256.MatchString(plan.TargetControllerTemplateDigest) || !hotfixSHA256.MatchString(plan.HybridControllerTemplateDigest))) {
		return fmt.Errorf("hotfix workload template digest material is invalid")
	}
	if err := validateHotfixProvenance(plan.Provenance, plan.AdoptedSource, controlPlaneHotfixTargetImage(plan)); err != nil {
		return err
	}
	if plan.PlanVersion == 3 {
		if err := validateHotfixControllerKubernetes(plan.Kubernetes, plan.ReleaseFullname); err != nil {
			return err
		}
	} else if err := validateHotfixKubernetes(plan.Kubernetes, plan.ReleaseFullname); err != nil {
		return err
	}
	if (plan.PlanVersion == 0 && plan.Kubernetes.FrozenNonAPIWorkloadDigest != "") ||
		(plan.PlanVersion == 2 && !hotfixSHA256.MatchString(plan.Kubernetes.FrozenNonAPIWorkloadDigest)) ||
		(plan.PlanVersion == 3 && !hotfixSHA256.MatchString(plan.Kubernetes.FrozenNonControllerDigest)) {
		return fmt.Errorf("hotfix non-API workload evidence is invalid")
	}
	liveImageRef := plan.Kubernetes.APIImageRef
	if plan.PlanVersion == 3 {
		liveImageRef = plan.Kubernetes.ControllerImageRef
	}
	if liveImageRef != controlPlaneHotfixHybridImage(plan) {
		return fmt.Errorf("hotfix Kubernetes evidence does not bind the live image")
	}
	if plan.PlanVersion == 2 && !hotfixImageIDMatchesProvenance(plan.TargetAPIImageID, plan.Provenance) {
		return fmt.Errorf("hotfix target image ID does not bind provenance")
	}
	if plan.PlanVersion == 3 && !hotfixImageIDMatchesProvenance(plan.TargetControllerImageID, plan.Provenance) {
		return fmt.Errorf("hotfix Controller target image ID does not bind provenance")
	}
	if err := validateHotfixLease(plan.Lease, plan.Namespace, plan.ReleaseFullname); err != nil {
		return err
	}
	if !validHotfixToken(plan.Fence) || !validHotfixToken(plan.Nonce) {
		return fmt.Errorf("hotfix fence or nonce is invalid")
	}
	if plan.Digest != controlPlaneHotfixPlanDigest(plan) {
		return fmt.Errorf("hotfix plan digest mismatch")
	}
	return nil
}

// VerifyControlPlaneHotfixRenderSet replays the plan's exact manifest proof
// without owning any production mutation. The production shell transaction
// uses it before acquiring the write boundary and again for fresh prewrite
// evidence while it owns the shared Lease.
func VerifyControlPlaneHotfixRenderSet(plan ControlPlaneHotfixAdoptionPlan, base, target, repeatedTarget, hybrid []byte) error {
	if err := VerifyControlPlaneHotfixAdoptionPlan(plan); err != nil {
		return err
	}
	base, err := canonicalSecretFreeHotfixManifest(base)
	if err != nil {
		return fmt.Errorf("base manifest: %w", err)
	}
	target, err = canonicalSecretFreeHotfixManifest(target)
	if err != nil {
		return fmt.Errorf("target manifest: %w", err)
	}
	repeatedTarget, err = canonicalSecretFreeHotfixManifest(repeatedTarget)
	if err != nil {
		return fmt.Errorf("repeated target manifest: %w", err)
	}
	hybrid, err = canonicalSecretFreeHotfixManifest(hybrid)
	if err != nil {
		return fmt.Errorf("hybrid manifest: %w", err)
	}
	if !bytes.Equal(target, repeatedTarget) {
		return fmt.Errorf("target render is not deterministic")
	}
	input := ControlPlaneHotfixAdoptionInput{
		PlanVersion:                  plan.PlanVersion,
		Namespace:                    plan.Namespace,
		CurrentSource:                plan.CurrentSource,
		AdoptedSource:                plan.AdoptedSource,
		LiveImageRef:                 plan.LiveImageRef,
		TargetAPIImageRef:            plan.TargetAPIImageRef,
		LiveHybridAPIImageRef:        plan.LiveHybridAPIImageRef,
		TargetAPIImageID:             plan.TargetAPIImageID,
		TargetControllerImageRef:     plan.TargetControllerImageRef,
		LiveHybridControllerImageRef: plan.LiveHybridControllerImageRef,
		TargetControllerImageID:      plan.TargetControllerImageID,
		Kubernetes:                   plan.Kubernetes,
		BaseManifest:                 base,
		TargetManifest:               target,
		RepeatedTarget:               repeatedTarget,
		HybridManifest:               hybrid,
	}
	if err := verifyHotfixTransition(base, target, hybrid, input); err != nil {
		return err
	}
	if hotfixDigest(base) != plan.BaseManifestDigest ||
		hotfixDigest(target) != plan.TargetManifestDigest ||
		hotfixDigest(hybrid) != plan.HybridManifestDigest {
		return fmt.Errorf("hotfix render digest drifted")
	}
	return nil
}

func NewControlPlaneHotfixAdoptionWAL(plan ControlPlaneHotfixAdoptionPlan) (ControlPlaneHotfixAdoptionWAL, error) {
	if err := VerifyControlPlaneHotfixAdoptionPlan(plan); err != nil {
		return ControlPlaneHotfixAdoptionWAL{}, err
	}
	wal := ControlPlaneHotfixAdoptionWAL{APIVersion: ControlPlaneHotfixAdoptionAPIVersion, Kind: ControlPlaneHotfixAdoptionWALKind, Policy: plan.Policy, PlanDigest: plan.Digest, Nonce: plan.Nonce, Fence: plan.Fence, Phase: "prepared", Sequence: 1}
	wal.Digest = controlPlaneHotfixWALDigest(wal)
	return wal, nil
}

func AdvanceControlPlaneHotfixAdoptionWAL(wal ControlPlaneHotfixAdoptionWAL, phase string) (ControlPlaneHotfixAdoptionWAL, error) {
	if err := VerifyControlPlaneHotfixAdoptionWAL(wal); err != nil {
		return ControlPlaneHotfixAdoptionWAL{}, err
	}
	allowed := map[string]string{
		"prepared": "prewrite-verified", "prewrite-verified": "forward-started", "forward-started": "forward-committed",
		"forward-committed": "verified", "forward-started:compensate": "compensation-started",
		"forward-committed:compensate": "compensation-started", "compensation-started": "compensated",
	}
	key := wal.Phase
	if phase == "compensation-started" {
		key += ":compensate"
	}
	if allowed[key] != phase {
		return ControlPlaneHotfixAdoptionWAL{}, fmt.Errorf("hotfix WAL transition %s -> %s is invalid", wal.Phase, phase)
	}
	wal.Phase, wal.Sequence = phase, wal.Sequence+1
	if phase == "forward-started" {
		if wal.ForwardAttempts != 0 {
			return ControlPlaneHotfixAdoptionWAL{}, fmt.Errorf("forward action was already attempted")
		}
		wal.ForwardAttempts = 1
		wal.RecoveryRequired = true
	}
	if phase == "compensation-started" {
		if wal.CompensationAttempts != 0 {
			return ControlPlaneHotfixAdoptionWAL{}, fmt.Errorf("compensation was already attempted")
		}
		wal.CompensationAttempts = 1
		wal.RecoveryRequired = true
	}
	if phase == "verified" || phase == "compensated" {
		wal.RecoveryRequired = false
	}
	wal.Digest = controlPlaneHotfixWALDigest(wal)
	return wal, nil
}

func VerifyControlPlaneHotfixAdoptionWAL(wal ControlPlaneHotfixAdoptionWAL) error {
	if wal.APIVersion != ControlPlaneHotfixAdoptionAPIVersion || wal.Kind != ControlPlaneHotfixAdoptionWALKind || (wal.Policy != ControlPlaneHotfixAdoptionPolicy && wal.Policy != ControlPlaneAPIHotfixRolloutPolicyV2 && wal.Policy != ControlPlaneControllerM16PolicyV3) || !hotfixSHA256.MatchString(wal.PlanDigest) || !validHotfixToken(wal.Nonce) || !validHotfixToken(wal.Fence) || wal.Sequence < 1 || wal.ForwardAttempts < 0 || wal.ForwardAttempts > 1 || wal.CompensationAttempts < 0 || wal.CompensationAttempts > 1 {
		return fmt.Errorf("hotfix WAL is invalid")
	}
	if wal.Digest != controlPlaneHotfixWALDigest(wal) {
		return fmt.Errorf("hotfix WAL digest mismatch")
	}
	return nil
}

type ControlPlaneHotfixCommitResult string

const (
	ControlPlaneHotfixCommitAcknowledged ControlPlaneHotfixCommitResult = "acknowledged"
	ControlPlaneHotfixCommitUnknown      ControlPlaneHotfixCommitResult = "unknown"
	ControlPlaneHotfixCommitRejected     ControlPlaneHotfixCommitResult = "rejected"
)

type ControlPlaneHotfixObservation struct {
	HelmRevision     int64                                `json:"helmRevision"`
	HelmStatus       string                               `json:"helmStatus"`
	HelmRecordDigest string                               `json:"helmRecordDigest"`
	ManifestDigest   string                               `json:"manifestDigest"`
	ValuesDigest     string                               `json:"valuesDigest"`
	ChartTreeDigest  string                               `json:"chartTreeDigest"`
	Source           string                               `json:"source"`
	LiveImageRef     string                               `json:"liveImageRef"`
	APIImageID       string                               `json:"apiImageId"`
	Kubernetes       ControlPlaneHotfixKubernetesEvidence `json:"kubernetes"`
	APIHealthStatus  int                                  `json:"apiHealthStatus"`
	APIHealthDigest  string                               `json:"apiHealthDigest"`
}

type ControlPlaneHotfixExecutionOptions struct {
	DryRun bool
}

type ControlPlaneHotfixExecutionResult struct {
	Status string                        `json:"status"`
	WAL    ControlPlaneHotfixAdoptionWAL `json:"wal"`
}

type ControlPlaneHotfixRuntime interface {
	Observe(context.Context) (ControlPlaneHotfixObservation, error)
	AcquireLease(context.Context, ControlPlaneHotfixAdoptionPlan) error
	VerifyLease(context.Context, ControlPlaneHotfixAdoptionPlan) error
	PersistWAL(context.Context, ControlPlaneHotfixAdoptionWAL) error
	Forward(context.Context, ControlPlaneHotfixAdoptionPlan) (ControlPlaneHotfixCommitResult, error)
	Compensate(context.Context, ControlPlaneHotfixAdoptionPlan) (ControlPlaneHotfixCommitResult, error)
	ReleaseLease(context.Context, ControlPlaneHotfixAdoptionPlan) error
}

func ExecuteControlPlaneHotfixAdoption(
	ctx context.Context,
	plan ControlPlaneHotfixAdoptionPlan,
	runtime ControlPlaneHotfixRuntime,
	options ControlPlaneHotfixExecutionOptions,
) (ControlPlaneHotfixExecutionResult, error) {
	if err := VerifyControlPlaneHotfixAdoptionPlan(plan); err != nil {
		return ControlPlaneHotfixExecutionResult{}, err
	}
	if plan.PlanVersion == 3 {
		return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("Controller M16 transaction uses the sealed production shell runtime")
	}
	if runtime == nil {
		return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("hotfix runtime is nil")
	}
	first, err := runtime.Observe(ctx)
	if err != nil {
		return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("first prewrite sample: %w", err)
	}
	if err := verifyControlPlaneHotfixObservation(plan, first, "base"); err != nil {
		return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("first prewrite sample: %w", err)
	}
	if options.DryRun {
		second, err := runtime.Observe(ctx)
		if err != nil {
			return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("second dry-run sample: %w", err)
		}
		if err := verifyControlPlaneHotfixObservation(plan, second, "base"); err != nil {
			return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("second dry-run sample: %w", err)
		}
		if !reflectHotfixEqual(first, second) {
			return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("hotfix dry-run samples drifted")
		}
		return ControlPlaneHotfixExecutionResult{Status: "dry-run-verified"}, nil
	}
	if err := runtime.AcquireLease(ctx, plan); err != nil {
		return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("acquire hotfix Lease: %w", err)
	}
	if err := runtime.VerifyLease(ctx, plan); err != nil {
		return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("verify acquired hotfix Lease: %w", err)
	}
	second, err := runtime.Observe(ctx)
	if err != nil {
		return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("second prewrite sample: %w", err)
	}
	if err := verifyControlPlaneHotfixObservation(plan, second, "base"); err != nil {
		return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("second prewrite sample: %w", err)
	}
	if !reflectHotfixEqual(first, second) {
		return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("hotfix prewrite samples drifted")
	}

	wal, err := NewControlPlaneHotfixAdoptionWAL(plan)
	if err != nil {
		return ControlPlaneHotfixExecutionResult{}, err
	}
	if err := runtime.PersistWAL(ctx, wal); err != nil {
		return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("persist prepared hotfix WAL: %w", err)
	}
	wal, err = AdvanceControlPlaneHotfixAdoptionWAL(wal, "prewrite-verified")
	if err != nil {
		return ControlPlaneHotfixExecutionResult{}, err
	}
	if err := runtime.PersistWAL(ctx, wal); err != nil {
		return ControlPlaneHotfixExecutionResult{}, fmt.Errorf("persist verified hotfix WAL: %w", err)
	}
	if err := runtime.VerifyLease(ctx, plan); err != nil {
		return ControlPlaneHotfixExecutionResult{WAL: wal}, fmt.Errorf("hotfix Lease lost before forward transaction: %w", err)
	}
	wal, err = AdvanceControlPlaneHotfixAdoptionWAL(wal, "forward-started")
	if err != nil {
		return ControlPlaneHotfixExecutionResult{}, err
	}
	if err := runtime.PersistWAL(ctx, wal); err != nil {
		return ControlPlaneHotfixExecutionResult{WAL: wal}, fmt.Errorf("arm hotfix recovery fence: %w", err)
	}
	forwardResult, forwardErr := runtime.Forward(ctx, plan)
	if !validControlPlaneHotfixCommitResult(forwardResult) {
		forwardResult = ControlPlaneHotfixCommitUnknown
	}
	afterForward, observeForwardErr := runtime.Observe(ctx)
	if observeForwardErr == nil && verifyControlPlaneHotfixObservation(plan, afterForward, "target") == nil {
		wal, err = AdvanceControlPlaneHotfixAdoptionWAL(wal, "forward-committed")
		if err != nil {
			return ControlPlaneHotfixExecutionResult{WAL: wal}, err
		}
		if err := runtime.PersistWAL(ctx, wal); err != nil {
			return ControlPlaneHotfixExecutionResult{WAL: wal}, fmt.Errorf("persist committed hotfix WAL: %w", err)
		}
		wal, err = AdvanceControlPlaneHotfixAdoptionWAL(wal, "verified")
		if err != nil {
			return ControlPlaneHotfixExecutionResult{WAL: wal}, err
		}
		if err := runtime.PersistWAL(ctx, wal); err != nil {
			return ControlPlaneHotfixExecutionResult{WAL: wal}, fmt.Errorf("persist verified hotfix WAL: %w", err)
		}
		if err := runtime.VerifyLease(ctx, plan); err != nil {
			return ControlPlaneHotfixExecutionResult{WAL: wal}, fmt.Errorf("hotfix committed but Lease ownership is unknown: %w", err)
		}
		if err := runtime.ReleaseLease(ctx, plan); err != nil {
			return ControlPlaneHotfixExecutionResult{WAL: wal}, fmt.Errorf("release verified hotfix Lease: %w", err)
		}
		return ControlPlaneHotfixExecutionResult{Status: "verified", WAL: wal}, nil
	}

	if err := runtime.VerifyLease(ctx, plan); err != nil {
		return ControlPlaneHotfixExecutionResult{WAL: wal}, fmt.Errorf("forward transaction is unverified and recovery fence must remain: %w", err)
	}
	wal, err = AdvanceControlPlaneHotfixAdoptionWAL(wal, "compensation-started")
	if err != nil {
		return ControlPlaneHotfixExecutionResult{WAL: wal}, err
	}
	if err := runtime.PersistWAL(ctx, wal); err != nil {
		return ControlPlaneHotfixExecutionResult{WAL: wal}, fmt.Errorf("persist compensation hotfix WAL: %w", err)
	}
	compensationResult, compensationErr := runtime.Compensate(ctx, plan)
	if !validControlPlaneHotfixCommitResult(compensationResult) {
		compensationResult = ControlPlaneHotfixCommitUnknown
	}
	afterCompensation, observeCompensationErr := runtime.Observe(ctx)
	if observeCompensationErr == nil && verifyControlPlaneHotfixObservation(plan, afterCompensation, "hybrid") == nil {
		wal, err = AdvanceControlPlaneHotfixAdoptionWAL(wal, "compensated")
		if err != nil {
			return ControlPlaneHotfixExecutionResult{WAL: wal}, err
		}
		if err := runtime.PersistWAL(ctx, wal); err != nil {
			return ControlPlaneHotfixExecutionResult{WAL: wal}, fmt.Errorf("persist compensated hotfix WAL: %w", err)
		}
		if err := runtime.VerifyLease(ctx, plan); err != nil {
			return ControlPlaneHotfixExecutionResult{WAL: wal}, fmt.Errorf("hotfix compensation committed but Lease ownership is unknown: %w", err)
		}
		if err := runtime.ReleaseLease(ctx, plan); err != nil {
			return ControlPlaneHotfixExecutionResult{WAL: wal}, fmt.Errorf("release compensated hotfix Lease: %w", err)
		}
		return ControlPlaneHotfixExecutionResult{Status: "compensated", WAL: wal}, fmt.Errorf("hotfix forward transaction failed and was exactly compensated")
	}
	return ControlPlaneHotfixExecutionResult{Status: "recovery-required", WAL: wal}, fmt.Errorf(
		"hotfix forward transaction (%s, %v) and compensation (%s, %v) are unverified; recovery fence must remain (forward readback: %v; compensation readback: %v)",
		forwardResult, forwardErr, compensationResult, compensationErr, observeForwardErr, observeCompensationErr,
	)
}

func validControlPlaneHotfixCommitResult(result ControlPlaneHotfixCommitResult) bool {
	switch result {
	case ControlPlaneHotfixCommitAcknowledged, ControlPlaneHotfixCommitUnknown, ControlPlaneHotfixCommitRejected:
		return true
	default:
		return false
	}
}

func verifyControlPlaneHotfixObservation(plan ControlPlaneHotfixAdoptionPlan, observation ControlPlaneHotfixObservation, phase string) error {
	wantRevision := plan.BaseRevision
	wantManifest := plan.BaseManifestDigest
	wantValues := plan.BaseValuesDigest
	wantSource := plan.CurrentSource
	wantTemplate := plan.Kubernetes.APITemplateDigest
	wantGeneration := plan.Kubernetes.APIGeneration
	wantImage := controlPlaneHotfixHybridImage(plan)
	wantImageID := plan.Kubernetes.APIImageID
	switch phase {
	case "base":
	case "target":
		wantRevision = plan.TargetRevision
		wantManifest = plan.TargetManifestDigest
		wantValues = plan.TargetValuesDigest
		wantSource = plan.AdoptedSource
		wantImage = controlPlaneHotfixTargetImage(plan)
		if plan.PlanVersion == 2 {
			wantImageID = plan.TargetAPIImageID
		}
		wantTemplate = plan.TargetAPITemplateDigest
		wantGeneration++
	case "hybrid":
		wantRevision = plan.TargetRevision + 1
		wantManifest = plan.HybridManifestDigest
		if plan.PlanVersion == 2 {
			wantValues = plan.HybridValuesDigest
		}
		wantTemplate = plan.HybridAPITemplateDigest
		wantGeneration += 2
	case "":
		return fmt.Errorf("hotfix observation phase is empty")
	default:
		return fmt.Errorf("hotfix observation phase is invalid")
	}
	if observation.HelmRevision != wantRevision || observation.HelmStatus != "deployed" || !hotfixSHA256.MatchString(observation.HelmRecordDigest) ||
		observation.ManifestDigest != wantManifest || observation.ValuesDigest != wantValues ||
		observation.ChartTreeDigest != plan.ChartTreeDigest || observation.Source != wantSource ||
		observation.LiveImageRef != wantImage || observation.APIImageID != wantImageID ||
		observation.APIHealthStatus != 200 || observation.APIHealthDigest != plan.Kubernetes.APIHealthDigest {
		return fmt.Errorf("hotfix %s observation does not match the exact Helm, image, or health binding", phase)
	}
	if phase == "base" {
		if !reflectHotfixEqual(observation.Kubernetes, plan.Kubernetes) {
			return fmt.Errorf("hotfix base Kubernetes evidence drifted")
		}
		return nil
	}
	value := observation.Kubernetes
	base := plan.Kubernetes
	if value.APIName != base.APIName || value.APIUID != base.APIUID || value.APIGeneration != wantGeneration || value.APIObservedGeneration != value.APIGeneration ||
		value.APIImageRef != wantImage || value.APIImageID != wantImageID || value.APITemplateDigest != wantTemplate || value.APIResourceVersion == base.APIResourceVersion ||
		value.APIReplicas != 2 || value.APIReady != 2 || value.APIUpdated != 2 || value.APIAvailable != 2 || value.APIUnavailable != 0 ||
		value.ServiceName != base.ServiceName || value.ServiceUID != base.ServiceUID ||
		value.EndpointSliceName != base.EndpointSliceName || value.EndpointSliceUID != base.EndpointSliceUID ||
		value.ReadyServingEndpoints != 2 {
		return fmt.Errorf("hotfix %s Kubernetes cohort is not exactly healthy and bound", phase)
	}
	return nil
}

// RenderControlPlaneHotfixTransaction applies the only two authorized forward
// pointers, or the one image pointer needed to restore the pre-adoption hybrid.
func RenderControlPlaneHotfixTransaction(rendered []byte, plan ControlPlaneHotfixAdoptionPlan, mode string) ([]byte, error) {
	if err := VerifyControlPlaneHotfixAdoptionPlan(plan); err != nil {
		return nil, err
	}
	canonical, err := canonicalHotfixManifest(rendered)
	if err != nil {
		return nil, err
	}
	if mode == "forward" {
		if hotfixDigest(canonical) != plan.TargetManifestDigest {
			return nil, fmt.Errorf("forward post-render input is not the exact authorized target")
		}
		return canonical, nil
	}
	if mode != "compensate" {
		return nil, fmt.Errorf("post-render mode is invalid")
	}
	if hotfixDigest(canonical) != plan.BaseManifestDigest {
		return nil, fmt.Errorf("compensation post-render input is not the exact transaction base")
	}
	objects, err := decodeHotfixObjects(canonical)
	if err != nil {
		return nil, err
	}
	workloadName := plan.Kubernetes.APIName
	if plan.PlanVersion == 3 {
		workloadName = plan.Kubernetes.ControllerName
	}
	deployment, err := exactHotfixDeployment(objects, plan.Namespace, workloadName)
	if err != nil {
		return nil, err
	}
	setImage := setHotfixImage
	if plan.PlanVersion == 3 {
		setImage = setHotfixControllerImage
	}
	if err := setImage(deployment, controlPlaneHotfixHybridImage(plan)); err != nil {
		return nil, err
	}
	if err := setHotfixSource(deployment, plan.CurrentSource); err != nil {
		return nil, err
	}
	output, err := encodeHotfixObjects(objects)
	if err != nil {
		return nil, err
	}
	if hotfixDigest(output) != plan.HybridManifestDigest {
		return nil, fmt.Errorf("post-render output is outside the exact transaction target")
	}
	return output, nil
}

// VerifyControlPlaneAPIHotfixPostRender binds the exact server-render bytes,
// the fixed Helm819 non-API restore plan, and the effective manifest bytes.
// The effective manifest is independently verified by the plan as an exact
// two-pointer API transition.
func VerifyControlPlaneAPIHotfixPostRender(plan ControlPlaneHotfixAdoptionPlan, mode string, rawInput, restorePlan, effectiveOutput []byte) error {
	if err := VerifyControlPlaneHotfixAdoptionPlan(plan); err != nil {
		return err
	}
	if plan.PlanVersion != 2 {
		return fmt.Errorf("API hotfix post-render requires v2")
	}
	wantRaw, wantOutput := plan.RawTargetManifestDigest, plan.TargetPostRenderDigest
	wantManifest := plan.TargetManifestDigest
	if mode == "compensate" {
		wantRaw, wantOutput, wantManifest = plan.RawHybridManifestDigest, plan.HybridPostRenderDigest, plan.HybridManifestDigest
	} else if mode != "forward" {
		return fmt.Errorf("API hotfix post-render mode is invalid")
	}
	if hotfixDigest(rawInput) != wantRaw || hotfixDigest(restorePlan) != plan.NonAPIEdgeRestorePlanDigest || hotfixDigest(effectiveOutput) != wantOutput {
		return fmt.Errorf("API hotfix post-render byte binding drifted")
	}
	canonical, err := canonicalSecretFreeHotfixManifest(effectiveOutput)
	if err != nil {
		return err
	}
	if hotfixDigest(canonical) != wantManifest {
		return fmt.Errorf("API hotfix post-render output is outside the exact two-pointer manifest")
	}
	return nil
}

func validateControlPlaneHotfixInput(input ControlPlaneHotfixAdoptionInput) error {
	if (input.PlanVersion == 0 && input.Confirm != "CONFIRM_CONTROL_PLANE_HOTFIX_BASELINE_ADOPTION") ||
		(input.PlanVersion == 2 && input.Confirm != "CONFIRM_CONTROL_PLANE_API_HOTFIX_ROLLOUT_V2") ||
		(input.PlanVersion == 3 && input.Confirm != "CONFIRM_CONTROL_PLANE_CONTROLLER_M16_ROLLOUT_V1") {
		return fmt.Errorf("hotfix confirmation literal is invalid")
	}
	if !hotfixSHA.MatchString(input.ExpectedSHA) || !hotfixSHA.MatchString(input.CurrentSource) || !hotfixSHA.MatchString(input.AdoptedSource) || input.CurrentSource == input.AdoptedSource {
		return fmt.Errorf("hotfix source identity is invalid")
	}
	if input.RunAttempt != 1 || !positiveDecimal(input.RunID) || input.HelmRevision < 1 || input.HelmStatus != "deployed" {
		return fmt.Errorf("hotfix run or Helm identity is invalid")
	}
	if input.PlanVersion == 0 {
		if input.HelmRevision != controlPlaneHotfixBaseRevision || input.TargetAPIImageRef != "" || input.LiveHybridAPIImageRef != "" || input.TargetAPIImageID != "" || input.HybridValuesDigest != "" || input.RawTargetManifestDigest != "" || input.RawHybridManifestDigest != "" || input.TargetPostRenderDigest != "" || input.HybridPostRenderDigest != "" || input.NonAPIEdgeRestorePlanDigest != "" {
			return fmt.Errorf("hotfix v1 input is invalid")
		}
	} else if input.PlanVersion == 2 {
		if input.HelmRevision != controlPlaneAPIHotfixBaseRevisionV2 || input.CurrentSource != controlPlaneAPIHotfixHybridSourceV2 || input.AdoptedSource != controlPlaneAPIHotfixTargetSourceV2 || input.LiveHybridAPIImageRef != controlPlaneAPIHotfixHybridImageV2 || input.TargetAPIImageRef == input.LiveHybridAPIImageRef || input.LiveImageRef != "" || !hotfixSHA256.MatchString(input.HybridValuesDigest) || !hotfixSHA256.MatchString(input.RawTargetManifestDigest) || !hotfixSHA256.MatchString(input.RawHybridManifestDigest) || !hotfixSHA256.MatchString(input.TargetPostRenderDigest) || !hotfixSHA256.MatchString(input.HybridPostRenderDigest) || !hotfixSHA256.MatchString(input.NonAPIEdgeRestorePlanDigest) {
			return fmt.Errorf("hotfix v2 input is invalid")
		}
	} else if input.PlanVersion == 3 {
		if input.HelmRevision != controlPlaneControllerM16BaseRevisionV3 || input.CurrentSource != controlPlaneControllerM16HybridSourceV3 || input.AdoptedSource != controlPlaneControllerM16TargetSourceV3 || input.TargetControllerImageRef != controlPlaneControllerM16TargetImageV3 || input.LiveHybridControllerImageRef != controlPlaneControllerM16HybridImageV3 || input.TargetControllerImageRef == input.LiveHybridControllerImageRef || input.LiveImageRef != "" || input.TargetAPIImageRef != "" || input.LiveHybridAPIImageRef != "" || input.TargetAPIImageID != "" || !hotfixSHA256.MatchString(input.HybridValuesDigest) || !hotfixSHA256.MatchString(input.RawTargetManifestDigest) || !hotfixSHA256.MatchString(input.RawHybridManifestDigest) || !hotfixSHA256.MatchString(input.TargetPostRenderDigest) || !hotfixSHA256.MatchString(input.HybridPostRenderDigest) || !hotfixSHA256.MatchString(input.NonAPIEdgeRestorePlanDigest) {
			return fmt.Errorf("hotfix v3 Controller input is invalid")
		}
	} else {
		return fmt.Errorf("hotfix plan version is invalid")
	}
	if !hotfixName.MatchString(input.Namespace) || !hotfixName.MatchString(input.ReleaseName) || !hotfixName.MatchString(input.ReleaseFullname) {
		return fmt.Errorf("hotfix release identity is invalid")
	}
	if !hotfixSHA256.MatchString(input.HelmRecordDigest) || !hotfixSHA256.MatchString(input.BaseValuesDigest) || !hotfixSHA256.MatchString(input.TargetValuesDigest) || !hotfixSHA256.MatchString(input.ChartTreeDigest) || !validHotfixToken(input.Fence) || !validHotfixToken(input.Nonce) {
		return fmt.Errorf("hotfix binding is invalid")
	}
	if err := validateHotfixProvenance(input.Provenance, input.AdoptedSource, controlPlaneHotfixInputTargetImage(input)); err != nil {
		return err
	}
	if input.PlanVersion == 3 {
		if err := validateHotfixControllerKubernetes(input.Kubernetes, input.ReleaseFullname); err != nil {
			return err
		}
	} else if err := validateHotfixKubernetes(input.Kubernetes, input.ReleaseFullname); err != nil {
		return err
	}
	if (input.PlanVersion == 0 && input.Kubernetes.FrozenNonAPIWorkloadDigest != "") ||
		(input.PlanVersion == 2 && !hotfixSHA256.MatchString(input.Kubernetes.FrozenNonAPIWorkloadDigest)) ||
		(input.PlanVersion == 3 && !hotfixSHA256.MatchString(input.Kubernetes.FrozenNonControllerDigest)) {
		return fmt.Errorf("hotfix non-API workload evidence is invalid")
	}
	liveImageRef := input.Kubernetes.APIImageRef
	if input.PlanVersion == 3 {
		liveImageRef = input.Kubernetes.ControllerImageRef
	}
	if liveImageRef != controlPlaneHotfixInputHybridImage(input) {
		return fmt.Errorf("hotfix Kubernetes evidence does not bind the live image")
	}
	if input.PlanVersion == 2 && !hotfixImageIDMatchesProvenance(input.TargetAPIImageID, input.Provenance) {
		return fmt.Errorf("hotfix target image ID does not bind provenance")
	}
	if input.PlanVersion == 3 && !hotfixImageIDMatchesProvenance(input.TargetControllerImageID, input.Provenance) {
		return fmt.Errorf("hotfix Controller target image ID does not bind provenance")
	}
	return validateHotfixLease(input.Lease, input.Namespace, input.ReleaseFullname)
}

func validateHotfixProvenance(value ControlPlaneHotfixProvenance, source, image string) error {
	if !positiveDecimal(value.BuildRunID) || value.BuildRunAttempt != 1 || !validHotfixToken(value.ArtifactName) || !hotfixSHA256.MatchString(value.ArtifactDigest) || !hotfixSHA256.MatchString(value.IndexDigest) || !hotfixSHA256.MatchString(value.PlatformManifestDigest) || !hotfixSHA256.MatchString(value.ConfigDigest) || value.OCIRevision != source || !value.Verified {
		return fmt.Errorf("hotfix provenance is invalid")
	}
	repository, digest, ok := splitImmutableImage(image)
	if !ok || repository != value.Repository || digest != value.IndexDigest {
		return fmt.Errorf("hotfix provenance does not bind the live immutable image")
	}
	return nil
}

func validateHotfixKubernetes(value ControlPlaneHotfixKubernetesEvidence, fullname string) error {
	if value.APIName != fullname+"-api" || value.APIUID == "" || value.APIResourceVersion == "" || value.APIGeneration < 1 || value.APIObservedGeneration != value.APIGeneration || !hotfixSHA256.MatchString(value.APITemplateDigest) || value.APIImageRef == "" || value.APIImageID == "" || !hotfixSHA256.MatchString(value.APIHealthDigest) || value.APIReplicas != 2 || value.APIReady != 2 || value.APIUpdated != 2 || value.APIAvailable != 2 || value.APIUnavailable != 0 || value.ServiceName != fullname || value.ServiceUID == "" || value.ServiceResourceVersion == "" || !hotfixSHA256.MatchString(value.ServiceSelectorDigest) || value.EndpointSliceName == "" || value.EndpointSliceUID == "" || value.EndpointSliceResourceVersion == "" || value.EndpointServiceName != value.ServiceName || !hotfixSHA256.MatchString(value.EndpointBindingDigest) || value.ReadyServingEndpoints != 2 {
		return fmt.Errorf("hotfix Kubernetes evidence is not an exact healthy API cohort")
	}
	return nil
}

func validateHotfixControllerKubernetes(value ControlPlaneHotfixKubernetesEvidence, fullname string) error {
	if value.ControllerName != fullname+"-controller" || value.ControllerUID == "" || value.ControllerResourceVersion == "" || value.ControllerGeneration < 1 || value.ControllerObservedGeneration != value.ControllerGeneration || !hotfixSHA256.MatchString(value.ControllerTemplateDigest) || value.ControllerImageRef != controlPlaneControllerM16HybridImageV3 || value.ControllerImageID == "" || value.ControllerReplicas != 2 || value.ControllerReady != 2 || value.ControllerUpdated != 2 || value.ControllerAvailable != 2 || value.ControllerUnavailable != 0 || value.ControllerLeaderLeaseName != fullname+"-controller" || value.ControllerLeaderLeaseUID == "" || value.ControllerLeaderLeaseVersion == "" || value.ControllerLeaderHolder == "" || !hotfixSHA256.MatchString(value.ControllerMetricsDigest) || !hotfixSHA256.MatchString(value.ControllerLKGDigest) || !hotfixSHA256.MatchString(value.FrozenNonControllerDigest) {
		return fmt.Errorf("hotfix Kubernetes evidence is not an exact healthy Controller cohort")
	}
	return nil
}

func validateHotfixLease(value ControlPlaneHotfixLeaseEvidence, namespace, fullname string) error {
	if value.Namespace != namespace || value.Name != fullname+"-control-plane-db-backup" || value.UID == "" || value.ResourceVersion == "" || value.HolderIdentity != "" || value.RecoveryRequired {
		return fmt.Errorf("hotfix Lease evidence is not an exact reusable shared Lease")
	}
	return nil
}

func verifyHotfixTransition(base, target, hybrid []byte, input ControlPlaneHotfixAdoptionInput) error {
	baseObjects, err := decodeHotfixObjects(base)
	if err != nil {
		return err
	}
	targetObjects, err := decodeHotfixObjects(target)
	if err != nil {
		return err
	}
	hybridObjects, err := decodeHotfixObjects(hybrid)
	if err != nil {
		return err
	}
	if len(baseObjects) != controlPlaneHotfixManifestObjects || len(baseObjects) != len(targetObjects) || len(baseObjects) != len(hybridObjects) {
		return fmt.Errorf("hotfix render object inventory drifted")
	}
	workloadName := input.Kubernetes.APIName
	if input.PlanVersion == 3 {
		workloadName = input.Kubernetes.ControllerName
	}
	b, err := exactHotfixDeployment(baseObjects, input.Namespace, workloadName)
	if err != nil {
		return err
	}
	t, err := exactHotfixDeployment(targetObjects, input.Namespace, workloadName)
	if err != nil {
		return err
	}
	h, err := exactHotfixDeployment(hybridObjects, input.Namespace, workloadName)
	if err != nil {
		return err
	}
	maxSurge := int64(1)
	if input.PlanVersion == 3 {
		maxSurge = 2
	}
	if err := verifyHotfixDeploymentPolicy(b, maxSurge); err != nil {
		return err
	}
	bSource, err := hotfixSource(b)
	if err != nil {
		return err
	}
	tSource, err := hotfixSource(t)
	if err != nil {
		return err
	}
	hSource, err := hotfixSource(h)
	if err != nil {
		return err
	}
	readImage := hotfixImage
	setImage := setHotfixImage
	if input.PlanVersion == 3 {
		readImage = hotfixControllerImage
		setImage = setHotfixControllerImage
	}
	bImage, err := readImage(b)
	if err != nil {
		return err
	}
	tImage, err := readImage(t)
	if err != nil {
		return err
	}
	hImage, err := readImage(h)
	if err != nil {
		return err
	}
	if input.PlanVersion == 0 {
		if bSource != input.CurrentSource || tSource != input.AdoptedSource || hSource != input.CurrentSource {
			return fmt.Errorf("hotfix source annotation transition is invalid")
		}
		if tImage != input.LiveImageRef || hImage != input.LiveImageRef {
			return fmt.Errorf("hotfix target changes the live immutable image")
		}
	} else {
		targetImage := input.TargetAPIImageRef
		hybridImage := input.LiveHybridAPIImageRef
		if input.PlanVersion == 3 {
			targetImage = input.TargetControllerImageRef
			hybridImage = input.LiveHybridControllerImageRef
		}
		if tSource != input.AdoptedSource || hSource != input.CurrentSource || tImage != targetImage || hImage != hybridImage {
			return fmt.Errorf("hotfix source or image transition is invalid")
		}
	}
	bc := cloneHotfixObject(b)
	tc := cloneHotfixObject(t)
	hc := cloneHotfixObject(h)
	if err := setHotfixSource(tc, bSource); err != nil {
		return err
	}
	if err := setImage(tc, bImage); err != nil {
		return err
	}
	if !reflectHotfixEqual(bc, tc) {
		return fmt.Errorf("hotfix target changes a third API pointer")
	}
	if err := setHotfixSource(hc, bSource); err != nil {
		return err
	}
	if err := setImage(hc, bImage); err != nil {
		return err
	}
	if !reflectHotfixEqual(bc, hc) {
		return fmt.Errorf("hotfix compensation changes a non-image pointer")
	}
	delete(baseObjects, hotfixObjectKey(b))
	delete(targetObjects, hotfixObjectKey(t))
	delete(hybridObjects, hotfixObjectKey(h))
	if !reflectHotfixEqual(baseObjects, targetObjects) || !reflectHotfixEqual(baseObjects, hybridObjects) {
		return fmt.Errorf("hotfix transition changes a non-API object")
	}
	return nil
}

func controlPlaneHotfixInputTargetImage(input ControlPlaneHotfixAdoptionInput) string {
	if input.PlanVersion == 3 {
		return input.TargetControllerImageRef
	}
	if input.PlanVersion == 2 {
		return input.TargetAPIImageRef
	}
	return input.LiveImageRef
}

func controlPlaneHotfixInputHybridImage(input ControlPlaneHotfixAdoptionInput) string {
	if input.PlanVersion == 3 {
		return input.LiveHybridControllerImageRef
	}
	if input.PlanVersion == 2 {
		return input.LiveHybridAPIImageRef
	}
	return input.LiveImageRef
}

func controlPlaneHotfixTargetImage(plan ControlPlaneHotfixAdoptionPlan) string {
	if plan.PlanVersion == 3 {
		return plan.TargetControllerImageRef
	}
	if plan.PlanVersion == 2 {
		return plan.TargetAPIImageRef
	}
	return plan.LiveImageRef
}

func controlPlaneHotfixHybridImage(plan ControlPlaneHotfixAdoptionPlan) string {
	if plan.PlanVersion == 3 {
		return plan.LiveHybridControllerImageRef
	}
	if plan.PlanVersion == 2 {
		return plan.LiveHybridAPIImageRef
	}
	return plan.LiveImageRef
}

func hotfixImageIDMatchesProvenance(imageID string, provenance ControlPlaneHotfixProvenance) bool {
	if imageID == "" || !hotfixSHA256.MatchString(provenance.IndexDigest) {
		return false
	}
	return strings.HasSuffix(imageID, "@"+provenance.IndexDigest) || strings.HasSuffix(imageID, "://"+provenance.IndexDigest)
}

type hotfixObjects map[string]map[string]any

func canonicalSecretFreeHotfixManifest(data []byte) ([]byte, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var filtered bytes.Buffer
	first := true
	for {
		var value map[string]any
		err := decoder.Decode(&value)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if len(value) == 0 || value["kind"] == "Secret" {
			continue
		}
		encoded, err := yaml.Marshal(value)
		if err != nil {
			return nil, err
		}
		if !first {
			filtered.WriteString("---\n")
		}
		first = false
		filtered.Write(encoded)
	}
	return canonicalHotfixManifest(filtered.Bytes())
}

func canonicalHotfixManifest(data []byte) ([]byte, error) {
	objects, err := decodeHotfixObjects(data)
	if err != nil {
		return nil, err
	}
	return encodeHotfixObjects(objects)
}

func hotfixManifestTemplateDigest(data []byte, namespace, name string) (string, error) {
	objects, err := decodeHotfixObjects(data)
	if err != nil {
		return "", err
	}
	deployment, err := exactHotfixDeployment(objects, namespace, name)
	if err != nil {
		return "", err
	}
	template, err := hotfixPodTemplate(deployment)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(template)
	if err != nil {
		return "", fmt.Errorf("encode API pod template: %w", err)
	}
	return hotfixDigest(encoded), nil
}

func decodeHotfixObjects(data []byte) (hotfixObjects, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	objects := hotfixObjects{}
	for {
		var value map[string]any
		err := decoder.Decode(&value)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if len(value) == 0 {
			continue
		}
		kind, _ := value["kind"].(string)
		api, _ := value["apiVersion"].(string)
		metadata, _ := value["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		namespace, _ := metadata["namespace"].(string)
		if api == "" || kind == "" || name == "" || kind == "Secret" {
			return nil, fmt.Errorf("manifest contains an invalid or secret object")
		}
		key := api + "|" + kind + "|" + namespace + "|" + name
		if _, exists := objects[key]; exists {
			return nil, fmt.Errorf("manifest contains duplicate object %s", key)
		}
		objects[key] = value
	}
	if len(objects) == 0 {
		return nil, fmt.Errorf("manifest is empty")
	}
	return objects, nil
}

func encodeHotfixObjects(objects hotfixObjects) ([]byte, error) {
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out bytes.Buffer
	for i, key := range keys {
		data, err := yaml.Marshal(objects[key])
		if err != nil {
			return nil, err
		}
		if i > 0 {
			out.WriteString("---\n")
		}
		out.Write(data)
	}
	return out.Bytes(), nil
}

func exactHotfixDeployment(objects hotfixObjects, namespace, name string) (map[string]any, error) {
	var found map[string]any
	for _, o := range objects {
		if o["apiVersion"] == "apps/v1" && o["kind"] == "Deployment" {
			m, _ := o["metadata"].(map[string]any)
			ns, _ := m["namespace"].(string)
			n, _ := m["name"].(string)
			if n == name && (ns == namespace || ns == "") {
				if found != nil {
					return nil, fmt.Errorf("hotfix API deployment is ambiguous")
				}
				found = o
			}
		}
	}
	if found == nil {
		return nil, fmt.Errorf("hotfix API deployment is missing")
	}
	return found, nil
}

func verifyHotfixDeploymentPolicy(deployment map[string]any, maxSurge int64) error {
	spec, ok := deployment["spec"].(map[string]any)
	if !ok || integerHotfixValue(spec["replicas"]) != 2 {
		return fmt.Errorf("hotfix API replica policy is invalid")
	}
	strategy, ok := spec["strategy"].(map[string]any)
	if !ok || stringValue(strategy["type"]) != "RollingUpdate" {
		return fmt.Errorf("hotfix API rollout strategy is invalid")
	}
	rolling, ok := strategy["rollingUpdate"].(map[string]any)
	if !ok || integerHotfixValue(rolling["maxUnavailable"]) != 0 || integerHotfixValue(rolling["maxSurge"]) != maxSurge {
		return fmt.Errorf("hotfix API rollout availability policy is invalid")
	}
	return nil
}

func integerHotfixValue(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	default:
		return -1
	}
}

func hotfixObjectKey(o map[string]any) string {
	m := o["metadata"].(map[string]any)
	return o["apiVersion"].(string) + "|" + o["kind"].(string) + "|" + stringValue(m["namespace"]) + "|" + stringValue(m["name"])
}
func hotfixPodTemplate(o map[string]any) (map[string]any, error) {
	spec, ok := o["spec"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("API deployment spec is invalid")
	}
	template, ok := spec["template"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("API pod template is invalid")
	}
	return template, nil
}
func hotfixSource(o map[string]any) (string, error) {
	template, err := hotfixPodTemplate(o)
	if err != nil {
		return "", err
	}
	metadata, ok := template["metadata"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("API pod metadata is invalid")
	}
	annotations, ok := metadata["annotations"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("API pod annotations are invalid")
	}
	source := stringValue(annotations["fugue.pro/source-commit"])
	if !hotfixSHA.MatchString(source) {
		return "", fmt.Errorf("API source annotation is invalid")
	}
	return source, nil
}
func setHotfixSource(o map[string]any, value string) error {
	if !hotfixSHA.MatchString(value) {
		return fmt.Errorf("API source annotation target is invalid")
	}
	template, err := hotfixPodTemplate(o)
	if err != nil {
		return err
	}
	metadata, ok := template["metadata"].(map[string]any)
	if !ok {
		return fmt.Errorf("API pod metadata is invalid")
	}
	annotations, ok := metadata["annotations"].(map[string]any)
	if !ok {
		return fmt.Errorf("API pod annotations are invalid")
	}
	annotations["fugue.pro/source-commit"] = value
	return nil
}
func hotfixImage(o map[string]any) (string, error) {
	template, err := hotfixPodTemplate(o)
	if err != nil {
		return "", err
	}
	spec, ok := template["spec"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("API pod spec is invalid")
	}
	containers, ok := spec["containers"].([]any)
	if !ok {
		return "", fmt.Errorf("API containers are invalid")
	}
	image := ""
	count := 0
	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if !ok {
			return "", fmt.Errorf("API container is invalid")
		}
		if container["name"] == "api" {
			image = stringValue(container["image"])
			count++
		}
	}
	if count != 1 || image == "" {
		return "", fmt.Errorf("API must have exactly one owned image")
	}
	return image, nil
}
func setHotfixImage(o map[string]any, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("API image target is invalid")
	}
	template, err := hotfixPodTemplate(o)
	if err != nil {
		return err
	}
	spec, ok := template["spec"].(map[string]any)
	if !ok {
		return fmt.Errorf("API pod spec is invalid")
	}
	containers, ok := spec["containers"].([]any)
	if !ok {
		return fmt.Errorf("API containers are invalid")
	}
	count := 0
	for _, raw := range containers {
		c, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("API container is invalid")
		}
		if c["name"] == "api" {
			c["image"] = value
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("API must have exactly one owned container")
	}
	return nil
}

func hotfixControllerImage(o map[string]any) (string, error) {
	template, err := hotfixPodTemplate(o)
	if err != nil {
		return "", err
	}
	spec, ok := template["spec"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("Controller pod spec is invalid")
	}
	containers, ok := spec["containers"].([]any)
	if !ok {
		return "", fmt.Errorf("Controller containers are invalid")
	}
	image := ""
	count := 0
	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if !ok {
			return "", fmt.Errorf("Controller container is invalid")
		}
		if container["name"] == "controller" {
			image = stringValue(container["image"])
			count++
		}
	}
	if count != 1 || image == "" {
		return "", fmt.Errorf("Controller must have exactly one owned image")
	}
	return image, nil
}

func setHotfixControllerImage(o map[string]any, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("Controller image target is invalid")
	}
	template, err := hotfixPodTemplate(o)
	if err != nil {
		return err
	}
	spec, ok := template["spec"].(map[string]any)
	if !ok {
		return fmt.Errorf("Controller pod spec is invalid")
	}
	containers, ok := spec["containers"].([]any)
	if !ok {
		return fmt.Errorf("Controller containers are invalid")
	}
	count := 0
	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("Controller container is invalid")
		}
		if container["name"] == "controller" {
			container["image"] = value
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("Controller must have exactly one owned container")
	}
	return nil
}

func cloneHotfixObject(v map[string]any) map[string]any {
	data, _ := json.Marshal(v)
	var out map[string]any
	_ = json.Unmarshal(data, &out)
	return out
}
func reflectHotfixEqual(a, b any) bool {
	aa, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(aa, bb)
}
func stringValue(v any) string { s, _ := v.(string); return s }
func splitImmutableImage(value string) (string, string, bool) {
	i := strings.LastIndex(value, "@sha256:")
	if i < 1 {
		return "", "", false
	}
	d := value[i+1:]
	return value[:i], d, hotfixSHA256.MatchString(d)
}
func positiveDecimal(value string) bool {
	n, err := strconv.ParseInt(value, 10, 64)
	return err == nil && n > 0 && strconv.FormatInt(n, 10) == value
}
func validHotfixToken(value string) bool {
	return len(value) >= 16 && len(value) <= 256 && !strings.ContainsAny(value, "\r\n\x00")
}
func hotfixDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func controlPlaneHotfixPlanDigest(plan ControlPlaneHotfixAdoptionPlan) string {
	plan.Digest = ""
	data, _ := json.Marshal(plan)
	return hotfixDigest(data)
}
func controlPlaneHotfixWALDigest(wal ControlPlaneHotfixAdoptionWAL) string {
	wal.Digest = ""
	data, _ := json.Marshal(wal)
	return hotfixDigest(data)
}

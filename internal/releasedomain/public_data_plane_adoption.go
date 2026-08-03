package releasedomain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	PublicDataPlaneAdoptionAPIVersion = "release-domain.fugue.dev/v1"
	PublicDataPlaneAdoptionIntentKind = "PublicDataPlaneHelmAdoptionIntent"
	PublicDataPlaneAdoptionPlanKind   = "PublicDataPlaneHelmAdoptionPlan"
	PublicDataPlaneAdoptionPolicy     = "public-data-plane-helm-adoption-v1"

	PublicDataPlaneAdoptionEnvelopeAPIVersion = "release-domain-transaction.fugue.dev/v1"
	PublicDataPlaneAdoptionEnvelopeKind       = "PublicDataPlaneHelmAdoptionTransaction"
	PublicDataPlaneAdoptionBaselineKind       = "PublicDataPlaneHelmAdoptionBaseline"
	PublicDataPlaneAdoptionTraceKind          = "PublicDataPlaneHelmAdoptionExecutionTrace"
	PublicDataPlaneAdoptionRecoveryWALKind    = "PublicDataPlaneHelmAdoptionRecoveryWAL"

	publicDataPlaneEdgeImagePath         = "/spec/template/spec/containers/0/image"
	publicDataPlaneEdgeIdentityImagePath = "/spec/template/spec/initContainers/0/image"
	publicDataPlaneEdgeIdentityContainer = "edge-workload-identity"
)

// PublicDataPlaneAdoptionMemberEvidence binds one member of an exact
// front/worker-a/worker-b public data-plane cohort without retaining raw
// Kubernetes objects in the public transaction.
type PublicDataPlaneAdoptionMemberEvidence struct {
	Name                  string `json:"name"`
	Slot                  string `json:"slot,omitempty"`
	UIDDigest             string `json:"uidDigest"`
	ResourceVersionDigest string `json:"resourceVersionDigest"`
	Generation            int64  `json:"generation"`
	ObservedGeneration    int64  `json:"observedGeneration"`
	Desired               int64  `json:"desired"`
	Current               int64  `json:"current"`
	Ready                 int64  `json:"ready"`
	Available             int64  `json:"available"`
	Updated               int64  `json:"updated"`
	Unavailable           int64  `json:"unavailable"`
	Misscheduled          int64  `json:"misscheduled"`
	EdgeImageDigest       string `json:"edgeImageDigest,omitempty"`
	CaddyImageDigest      string `json:"caddyImageDigest,omitempty"`
	SpecDigest            string `json:"specDigest"`
}

type PublicDataPlaneAdoptionGroupEvidence struct {
	Base         string                                `json:"base"`
	Serving      bool                                  `json:"serving"`
	AdoptionSlot string                                `json:"adoptionSlot"`
	Front        PublicDataPlaneAdoptionMemberEvidence `json:"front"`
	WorkerA      PublicDataPlaneAdoptionMemberEvidence `json:"workerA"`
	WorkerB      PublicDataPlaneAdoptionMemberEvidence `json:"workerB"`
}

type PublicDataPlaneAdoptionRecordEvidence struct {
	Name                  string `json:"name"`
	UIDDigest             string `json:"uidDigest"`
	ResourceVersionDigest string `json:"resourceVersionDigest"`
	ReleaseID             string `json:"releaseId"`
	GitSHA                string `json:"gitSha"`
	ActiveSlotsDigest     string `json:"activeSlotsDigest"`
	DaemonSetsDigest      string `json:"daemonSetsDigest"`
	EdgeResourcesDigest   string `json:"edgeResourcesDigest"`
	CaddyResourcesDigest  string `json:"caddyResourcesDigest"`
}

type PublicDataPlaneAdoptionPatch struct {
	WorkloadName string `json:"workloadName"`
	Container    string `json:"container"`
	ImageRef     string `json:"imageRef"`
}

// PublicDataPlaneAdoptionChecksumReconciliation binds one historical rollout
// checksum to the deterministic value emitted by the current server render.
// Stage1 restores the historical value so the Helm transaction changes only
// the authorized edge image pointers.
type PublicDataPlaneAdoptionChecksumReconciliation struct {
	WorkloadName  string `json:"workloadName"`
	Annotation    string `json:"annotation"`
	BaseValue     string `json:"baseValue"`
	RenderedValue string `json:"renderedValue"`
}

// PublicDataPlaneAdoptionIntent is derived only from one Kubernetes List
// snapshot and the public release record inside that same list.
type PublicDataPlaneAdoptionIntent struct {
	APIVersion         string                                 `json:"apiVersion"`
	Kind               string                                 `json:"kind"`
	Policy             string                                 `json:"policy"`
	SourceCommit       string                                 `json:"sourceCommit"`
	ReleaseName        string                                 `json:"releaseName"`
	ReleaseNamespace   string                                 `json:"releaseNamespace"`
	ReleaseFullname    string                                 `json:"releaseFullname"`
	SnapshotDigest     string                                 `json:"snapshotDigest"`
	Record             PublicDataPlaneAdoptionRecordEvidence  `json:"record"`
	Groups             []PublicDataPlaneAdoptionGroupEvidence `json:"groups"`
	Patches            []PublicDataPlaneAdoptionPatch         `json:"patches"`
	AdoptionSlot       string                                 `json:"adoptionSlot"`
	TargetEdgeImageRef string                                 `json:"targetEdgeImageRef"`
	Digest             string                                 `json:"digest"`
}

type PublicDataPlaneStage2Handoff struct {
	RequiredBaseRevision       string `json:"requiredBaseRevision"`
	RequiredBaseManifestDigest string `json:"requiredBaseManifestDigest"`
	Stage1PlanDigest           string `json:"stage1PlanDigest"`
}

// PublicDataPlaneAdoptionPlan binds the exact Helm revision pair, immutable
// manifests, observed-live witness, and one-shot Kubernetes snapshot.
type PublicDataPlaneAdoptionPlan struct {
	APIVersion                         string                                          `json:"apiVersion"`
	Kind                               string                                          `json:"kind"`
	Policy                             string                                          `json:"policy"`
	ExpectedDomain                     Domain                                          `json:"expectedDomain"`
	SourceCommit                       string                                          `json:"sourceCommit"`
	ReleaseName                        string                                          `json:"releaseName"`
	ReleaseNamespace                   string                                          `json:"releaseNamespace"`
	BaseRevision                       string                                          `json:"baseRevision"`
	TargetRevision                     string                                          `json:"targetRevision"`
	OwnershipDigest                    string                                          `json:"ownershipDigest"`
	ValuesDigest                       string                                          `json:"valuesDigest"`
	SecretLookupWitnessDigest          string                                          `json:"secretLookupWitnessDigest"`
	SecretRenderWitnessDigest          string                                          `json:"secretRenderWitnessDigest"`
	SecretPayloadHMAC                  string                                          `json:"secretPayloadHmac"`
	BaseManifestDigest                 string                                          `json:"baseManifestDigest"`
	ServerRenderedTargetDigest         string                                          `json:"serverRenderedTargetManifestDigest"`
	RepeatedServerRenderedTargetDigest string                                          `json:"repeatedServerRenderedTargetManifestDigest"`
	TargetManifestDigest               string                                          `json:"targetManifestDigest"`
	RepeatedTargetDigest               string                                          `json:"repeatedTargetManifestDigest"`
	ObservedLiveDigest                 string                                          `json:"observedLiveDigest"`
	SnapshotDigest                     string                                          `json:"snapshotDigest"`
	RestoreSnapshotDigest              string                                          `json:"restoreSnapshotDigest"`
	Intent                             PublicDataPlaneAdoptionIntent                   `json:"intent"`
	ChecksumReconciliations            []PublicDataPlaneAdoptionChecksumReconciliation `json:"checksumReconciliations"`
	Rendered                           RenderedClassification                          `json:"rendered"`
	Stage2                             PublicDataPlaneStage2Handoff                    `json:"stage2"`
	Digest                             string                                          `json:"digest"`
}

type PublicDataPlaneAdoptionTransactionEnvelope struct {
	APIVersion     string                      `json:"apiVersion"`
	Kind           string                      `json:"kind"`
	PlanDigest     string                      `json:"planDigest"`
	ExpectedDomain Domain                      `json:"expectedDomain"`
	Plan           PublicDataPlaneAdoptionPlan `json:"plan"`
}

type PublicDataPlaneAdoptionBaseline struct {
	APIVersion       string                                `json:"apiVersion"`
	Kind             string                                `json:"kind"`
	Policy           string                                `json:"policy"`
	SourceCommit     string                                `json:"sourceCommit"`
	HelmRevision     string                                `json:"helmRevision"`
	ManifestDigest   string                                `json:"manifestDigest"`
	Stage1PlanDigest string                                `json:"stage1PlanDigest"`
	Record           PublicDataPlaneAdoptionRecordEvidence `json:"record"`
	Digest           string                                `json:"digest"`
}

type PublicDataPlaneAdoptionTraceEvent struct {
	Sequence int    `json:"sequence"`
	Phase    string `json:"phase"`
	At       string `json:"at"`
}

type PublicDataPlaneAdoptionExecutionTrace struct {
	APIVersion string                              `json:"apiVersion"`
	Kind       string                              `json:"kind"`
	Policy     string                              `json:"policy"`
	PlanDigest string                              `json:"planDigest"`
	Events     []PublicDataPlaneAdoptionTraceEvent `json:"events"`
	Digest     string                              `json:"digest"`
}

type PublicDataPlaneAdoptionRecoveryMember struct {
	Name         string `json:"name"`
	UIDDigest    string `json:"uidDigest"`
	EdgeImageRef string `json:"edgeImageRef"`
	SpecDigest   string `json:"specDigest"`
}

type PublicDataPlaneAdoptionRecoveryWAL struct {
	APIVersion        string                                  `json:"apiVersion"`
	Kind              string                                  `json:"kind"`
	Policy            string                                  `json:"policy"`
	PlanDigest        string                                  `json:"planDigest"`
	TransactionDigest string                                  `json:"transactionDigest"`
	RestoreDigest     string                                  `json:"restoreDigest"`
	SourceCommit      string                                  `json:"sourceCommit"`
	BaseRevision      string                                  `json:"baseRevision"`
	TargetRevision    string                                  `json:"targetRevision"`
	LeaseNamespace    string                                  `json:"leaseNamespace"`
	LeaseName         string                                  `json:"leaseName"`
	LeaseOwner        string                                  `json:"leaseOwner"`
	LeaseTokenDigest  string                                  `json:"leaseTokenDigest"`
	OriginRunID       string                                  `json:"originRunId"`
	OriginRunAttempt  int                                     `json:"originRunAttempt"`
	Phase             string                                  `json:"phase"`
	Sequence          int                                     `json:"sequence"`
	ApplyAttempts     int                                     `json:"applyAttempts"`
	RestoreAttempts   int                                     `json:"restoreAttempts"`
	BaselineDigest    string                                  `json:"baselineDigest,omitempty"`
	UpdatedAt         string                                  `json:"updatedAt"`
	Members           []PublicDataPlaneAdoptionRecoveryMember `json:"members"`
	Digest            string                                  `json:"digest"`
}

type PublicDataPlaneAdoptionInput struct {
	Ownership                   []byte
	Values                      []byte
	BaseManifest                []byte
	TargetManifest              []byte
	RepeatedTarget              []byte
	ObservedLive                []byte
	KubernetesSnapshot          []byte
	SecretLookupWitness         []byte
	BaseSecretRenderWitness     []byte
	TargetSecretRenderWitness   []byte
	RepeatedSecretRenderWitness []byte
	SourceCommit                string
	ReleaseName                 string
	ReleaseNamespace            string
	ReleaseFullname             string
	BaseRevision                string
	TargetRevision              string
	Bindings                    map[string]string
}

type publicDataPlaneSnapshot struct {
	canonical []byte
	record    PublicDataPlaneAdoptionRecordEvidence
	groups    []PublicDataPlaneAdoptionGroupEvidence
	patches   []PublicDataPlaneAdoptionPatch
	slot      string
	imageRef  string
	rawSpecs  map[string]map[string]any
	rawUIDs   map[string]string
}

type publicDataPlaneSnapshotMember struct {
	evidence PublicDataPlaneAdoptionMemberEvidence
	raw      map[string]any
	labels   map[string]string
	edgeRef  string
	caddyRef string
}

type publicDataPlaneRestoreMember struct {
	Name         string `json:"name"`
	UID          string `json:"uid"`
	EdgeImageRef string `json:"edgeImageRef"`
	SpecDigest   string `json:"specDigest"`
}

type publicDataPlaneRestoreDocument struct {
	APIVersion string                         `json:"apiVersion"`
	Kind       string                         `json:"kind"`
	Policy     string                         `json:"policy"`
	Members    []publicDataPlaneRestoreMember `json:"members"`
}

type PublicDataPlaneRestorePatch struct {
	Name  string `json:"name"`
	UID   string `json:"uid"`
	Patch []any  `json:"patch"`
}

func BuildPublicDataPlaneAdoptionIntent(
	snapshot []byte,
	sourceCommit, releaseName, releaseNamespace, releaseFullname string,
) (PublicDataPlaneAdoptionIntent, error) {
	if err := validateTrustedGitCommit(sourceCommit, "public data-plane adoption source commit"); err != nil {
		return PublicDataPlaneAdoptionIntent{}, err
	}
	for label, value := range map[string]string{
		"release name": releaseName, "release namespace": releaseNamespace, "release fullname": releaseFullname,
	} {
		if !validContractText(value, 253) {
			return PublicDataPlaneAdoptionIntent{}, fmt.Errorf("public data-plane adoption %s is invalid", label)
		}
	}
	parsed, err := parsePublicDataPlaneSnapshot(snapshot, releaseName, releaseNamespace, releaseFullname)
	if err != nil {
		return PublicDataPlaneAdoptionIntent{}, err
	}
	intent := PublicDataPlaneAdoptionIntent{
		APIVersion: PublicDataPlaneAdoptionAPIVersion, Kind: PublicDataPlaneAdoptionIntentKind,
		Policy: PublicDataPlaneAdoptionPolicy, SourceCommit: sourceCommit,
		ReleaseName: releaseName, ReleaseNamespace: releaseNamespace, ReleaseFullname: releaseFullname,
		SnapshotDigest: digestBytesSHA256(parsed.canonical), Record: parsed.record,
		Groups: parsed.groups, Patches: parsed.patches, AdoptionSlot: parsed.slot,
		TargetEdgeImageRef: parsed.imageRef,
	}
	intent.Digest = publicDataPlaneAdoptionIntentDigest(intent)
	if err := VerifyPublicDataPlaneAdoptionIntent(intent); err != nil {
		return PublicDataPlaneAdoptionIntent{}, err
	}
	return intent, nil
}

func VerifyPublicDataPlaneAdoptionIntent(intent PublicDataPlaneAdoptionIntent) error {
	if intent.APIVersion != PublicDataPlaneAdoptionAPIVersion || intent.Kind != PublicDataPlaneAdoptionIntentKind ||
		intent.Policy != PublicDataPlaneAdoptionPolicy {
		return fmt.Errorf("public data-plane adoption intent identity is unsupported")
	}
	if err := validateTrustedGitCommit(intent.SourceCommit, "public data-plane adoption intent source commit"); err != nil {
		return err
	}
	for label, value := range map[string]string{
		"snapshot": intent.SnapshotDigest, "intent": intent.Digest,
	} {
		if err := validateCanonicalSHA256Digest(value, "public data-plane adoption "+label+" digest"); err != nil {
			return err
		}
	}
	if intent.AdoptionSlot != "a" && intent.AdoptionSlot != "b" {
		return fmt.Errorf("public data-plane adoption slot is invalid")
	}
	if err := validatePublicDataPlaneImageRef(intent.TargetEdgeImageRef, true); err != nil {
		return err
	}
	if len(intent.Groups) == 0 || len(intent.Patches) == 0 {
		return fmt.Errorf("public data-plane adoption intent is empty")
	}
	groups := append([]PublicDataPlaneAdoptionGroupEvidence(nil), intent.Groups...)
	sort.Slice(groups, func(i, j int) bool { return groups[i].Base < groups[j].Base })
	if !reflect.DeepEqual(groups, intent.Groups) {
		return fmt.Errorf("public data-plane adoption groups are not canonical")
	}
	patches := append([]PublicDataPlaneAdoptionPatch(nil), intent.Patches...)
	sort.Slice(patches, func(i, j int) bool { return patches[i].WorkloadName < patches[j].WorkloadName })
	if !reflect.DeepEqual(patches, intent.Patches) {
		return fmt.Errorf("public data-plane adoption patches are not canonical")
	}
	seen := map[string]struct{}{}
	for _, patch := range intent.Patches {
		if !validContractText(patch.WorkloadName, 253) || patch.Container != "edge" ||
			patch.ImageRef != intent.TargetEdgeImageRef {
			return fmt.Errorf("public data-plane adoption patch is invalid")
		}
		if _, exists := seen[patch.WorkloadName]; exists {
			return fmt.Errorf("public data-plane adoption patch is duplicated")
		}
		seen[patch.WorkloadName] = struct{}{}
	}
	if publicDataPlaneAdoptionIntentDigest(intent) != intent.Digest {
		return fmt.Errorf("public data-plane adoption intent digest mismatch")
	}
	return nil
}

func BuildPublicDataPlaneAdoptionPlan(input PublicDataPlaneAdoptionInput) (PublicDataPlaneAdoptionPlan, []byte, error) {
	if err := validatePublicDataPlaneAdoptionRevisionPair(input.BaseRevision, input.TargetRevision); err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, err
	}
	if len(bytes.TrimSpace(input.Values)) == 0 || len(input.Values) > maxRenderedManifestBytes {
		return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption Helm values are invalid")
	}
	lookupWitness, err := DecodePublicDataPlaneSecretLookupWitness(input.SecretLookupWitness)
	if err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption secret lookup witness: %w", err)
	}
	baseSecretWitness, err := DecodePublicDataPlaneSecretRenderWitness(input.BaseSecretRenderWitness, true)
	if err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption base secret render witness: %w", err)
	}
	targetSecretWitness, err := DecodePublicDataPlaneSecretRenderWitness(input.TargetSecretRenderWitness, true)
	if err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption target secret render witness: %w", err)
	}
	repeatedSecretWitness, err := DecodePublicDataPlaneSecretRenderWitness(input.RepeatedSecretRenderWitness, true)
	if err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption repeated secret render witness: %w", err)
	}
	if lookupWitness.ReleaseName != input.ReleaseName || lookupWitness.ReleaseNamespace != input.ReleaseNamespace ||
		baseSecretWitness.ReleaseNamespace != input.ReleaseNamespace {
		return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption secret witness release identity mismatch")
	}
	expectedSecretNames := []string{
		input.ReleaseFullname + "-config", input.ReleaseFullname + "-control-plane-postgres-app",
		input.ReleaseFullname + "-platform-component-identity",
	}
	sort.Strings(expectedSecretNames)
	for index, member := range lookupWitness.Members {
		if member.Name != expectedSecretNames[index] {
			return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption secret witness object set is not the generated chart set")
		}
	}
	if err := VerifyPublicDataPlaneSecretWitnessBinding(
		lookupWitness, baseSecretWitness, targetSecretWitness, repeatedSecretWitness,
	); err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption secret witness binding: %w", err)
	}
	spec, err := LoadOwnership(bytes.NewReader(input.Ownership))
	if err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption ownership: %w", err)
	}
	if err := spec.ValidateBindings(input.Bindings); err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption bindings: %w", err)
	}
	for label, manifest := range map[string][]byte{
		"base": input.BaseManifest, "target": input.TargetManifest,
		"repeated target": input.RepeatedTarget, "observed live": input.ObservedLive,
	} {
		canonical, canonicalErr := CanonicalizeRenderedManifest(manifest, spec, input.ReleaseNamespace)
		if canonicalErr != nil || !bytes.Equal(canonical, manifest) {
			return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption %s manifest is not canonical", label)
		}
	}
	if !bytes.Equal(input.TargetManifest, input.RepeatedTarget) {
		return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption repeated target differs")
	}
	if err := VerifyObservedLiveImageManifest(input.BaseManifest, input.ObservedLive, input.Ownership, input.ReleaseNamespace); err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption observed live witness: %w", err)
	}
	intent, err := BuildPublicDataPlaneAdoptionIntent(
		input.KubernetesSnapshot, input.SourceCommit, input.ReleaseName, input.ReleaseNamespace, input.ReleaseFullname,
	)
	if err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, err
	}
	parsedSnapshot, err := parsePublicDataPlaneSnapshot(
		input.KubernetesSnapshot, input.ReleaseName, input.ReleaseNamespace, input.ReleaseFullname,
	)
	if err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, err
	}
	normalizedTarget, reconciliations, err := normalizePublicDataPlaneAdoptionChecksums(
		input.BaseManifest, input.TargetManifest, spec, input.ReleaseNamespace, input.Bindings, intent,
	)
	if err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, err
	}
	normalizedRepeated, repeatedReconciliations, err := normalizePublicDataPlaneAdoptionChecksums(
		input.BaseManifest, input.RepeatedTarget, spec, input.ReleaseNamespace, input.Bindings, intent,
	)
	if err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, err
	}
	if !bytes.Equal(normalizedTarget, normalizedRepeated) || !reflect.DeepEqual(reconciliations, repeatedReconciliations) {
		return PublicDataPlaneAdoptionPlan{}, nil, fmt.Errorf("public data-plane adoption repeated checksum reconciliation differs")
	}
	normalizedInput := input
	normalizedInput.TargetManifest = normalizedTarget
	normalizedInput.RepeatedTarget = normalizedRepeated
	if err := verifyPublicDataPlaneAdoptionManifests(normalizedInput, intent, spec); err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, err
	}
	restore, err := marshalPublicDataPlaneRestoreSnapshot(parsedSnapshot)
	if err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, err
	}
	rendered := ClassifyRendered(input.BaseManifest, normalizedTarget, spec, RenderedOptions{
		DefaultNamespace: input.ReleaseNamespace, Bindings: input.Bindings, IgnoreHelmTestHooks: false,
	})
	plan := PublicDataPlaneAdoptionPlan{
		APIVersion: PublicDataPlaneAdoptionAPIVersion, Kind: PublicDataPlaneAdoptionPlanKind,
		Policy: PublicDataPlaneAdoptionPolicy, ExpectedDomain: DomainAuthoritativeDNS,
		SourceCommit: input.SourceCommit, ReleaseName: input.ReleaseName,
		ReleaseNamespace: input.ReleaseNamespace, BaseRevision: input.BaseRevision,
		TargetRevision: input.TargetRevision, OwnershipDigest: digestBytesSHA256(input.Ownership),
		ValuesDigest:                       digestBytesSHA256(input.Values),
		SecretLookupWitnessDigest:          lookupWitness.Digest,
		SecretRenderWitnessDigest:          baseSecretWitness.Digest,
		SecretPayloadHMAC:                  baseSecretWitness.PayloadHMAC,
		BaseManifestDigest:                 digestBytesSHA256(input.BaseManifest),
		ServerRenderedTargetDigest:         digestBytesSHA256(input.TargetManifest),
		RepeatedServerRenderedTargetDigest: digestBytesSHA256(input.RepeatedTarget),
		TargetManifestDigest:               digestBytesSHA256(normalizedTarget),
		RepeatedTargetDigest:               digestBytesSHA256(normalizedRepeated),
		ObservedLiveDigest:                 digestBytesSHA256(input.ObservedLive),
		SnapshotDigest:                     intent.SnapshotDigest, RestoreSnapshotDigest: digestBytesSHA256(restore),
		Intent: intent, ChecksumReconciliations: reconciliations, Rendered: rendered,
	}
	plan.Stage2 = PublicDataPlaneStage2Handoff{
		RequiredBaseRevision: input.TargetRevision, RequiredBaseManifestDigest: plan.TargetManifestDigest,
	}
	plan.Digest = publicDataPlaneAdoptionPlanDigest(plan)
	plan.Stage2.Stage1PlanDigest = plan.Digest
	plan.Digest = publicDataPlaneAdoptionPlanDigest(plan)
	plan.Stage2.Stage1PlanDigest = plan.Digest
	// Stage2PlanDigest is self-referential if included in the plan digest. Freeze
	// it to the final plan digest by excluding that one field from the digest.
	if err := VerifyPublicDataPlaneAdoptionPlan(plan); err != nil {
		return PublicDataPlaneAdoptionPlan{}, nil, err
	}
	return plan, restore, nil
}

func VerifyPublicDataPlaneAdoptionPlan(plan PublicDataPlaneAdoptionPlan) error {
	if plan.APIVersion != PublicDataPlaneAdoptionAPIVersion || plan.Kind != PublicDataPlaneAdoptionPlanKind ||
		plan.Policy != PublicDataPlaneAdoptionPolicy || plan.ExpectedDomain != DomainAuthoritativeDNS {
		return fmt.Errorf("public data-plane adoption plan identity is unsupported")
	}
	if err := VerifyPublicDataPlaneAdoptionIntent(plan.Intent); err != nil {
		return err
	}
	if plan.SourceCommit != plan.Intent.SourceCommit || plan.ReleaseName != plan.Intent.ReleaseName ||
		plan.ReleaseNamespace != plan.Intent.ReleaseNamespace || plan.SnapshotDigest != plan.Intent.SnapshotDigest {
		return fmt.Errorf("public data-plane adoption plan intent binding mismatch")
	}
	if err := validatePublicDataPlaneAdoptionRevisionPair(plan.BaseRevision, plan.TargetRevision); err != nil {
		return err
	}
	for label, digest := range map[string]string{
		"ownership": plan.OwnershipDigest, "values": plan.ValuesDigest, "base manifest": plan.BaseManifestDigest,
		"secret lookup witness": plan.SecretLookupWitnessDigest, "secret render witness": plan.SecretRenderWitnessDigest,
		"server-rendered target manifest":          plan.ServerRenderedTargetDigest,
		"repeated server-rendered target manifest": plan.RepeatedServerRenderedTargetDigest,
		"target manifest":                          plan.TargetManifestDigest, "repeated target manifest": plan.RepeatedTargetDigest,
		"observed live": plan.ObservedLiveDigest, "snapshot": plan.SnapshotDigest,
		"restore snapshot": plan.RestoreSnapshotDigest, "plan": plan.Digest,
	} {
		if err := validateCanonicalSHA256Digest(digest, "public data-plane adoption "+label+" digest"); err != nil {
			return err
		}
	}
	if len(plan.SecretPayloadHMAC) != len("hmac-sha256:")+64 || !strings.HasPrefix(plan.SecretPayloadHMAC, "hmac-sha256:") {
		return fmt.Errorf("public data-plane adoption secret payload HMAC is invalid")
	}
	hmacHex := strings.TrimPrefix(plan.SecretPayloadHMAC, "hmac-sha256:")
	if hmacHex != strings.ToLower(hmacHex) {
		return fmt.Errorf("public data-plane adoption secret payload HMAC is invalid")
	}
	if _, err := hex.DecodeString(hmacHex); err != nil {
		return fmt.Errorf("public data-plane adoption secret payload HMAC is invalid")
	}
	if plan.ServerRenderedTargetDigest != plan.RepeatedServerRenderedTargetDigest ||
		plan.ServerRenderedTargetDigest == plan.TargetManifestDigest ||
		plan.TargetManifestDigest != plan.RepeatedTargetDigest || plan.Stage2.RequiredBaseRevision != plan.TargetRevision ||
		plan.Stage2.RequiredBaseManifestDigest != plan.TargetManifestDigest ||
		plan.Stage2.Stage1PlanDigest != plan.Digest {
		return fmt.Errorf("public data-plane adoption Stage2 handoff mismatch")
	}
	if err := verifyPublicDataPlaneChecksumReconciliations(plan.Intent, plan.ChecksumReconciliations); err != nil {
		return err
	}
	if !isExactTransactionDomain(plan.Rendered.Domains, DomainAuthoritativeDNS) || len(plan.Rendered.Unknown) != 0 ||
		len(plan.Rendered.Evidence) == 0 {
		return fmt.Errorf("public data-plane adoption rendered evidence is not exact authoritative-dns")
	}
	for _, item := range plan.Rendered.Evidence {
		if item.Ignored || !isExactTransactionDomain(item.Domains, DomainAuthoritativeDNS) ||
			!publicDataPlaneBoundEdgeImagePaths(item.Paths) {
			return fmt.Errorf("public data-plane adoption contains non-image or non-authoritative rendered evidence")
		}
	}
	if publicDataPlaneAdoptionPlanDigest(plan) != plan.Digest {
		return fmt.Errorf("public data-plane adoption plan digest mismatch")
	}
	return nil
}

func NewPublicDataPlaneAdoptionTransaction(plan PublicDataPlaneAdoptionPlan) (PublicDataPlaneAdoptionTransactionEnvelope, error) {
	if err := VerifyPublicDataPlaneAdoptionPlan(plan); err != nil {
		return PublicDataPlaneAdoptionTransactionEnvelope{}, err
	}
	return PublicDataPlaneAdoptionTransactionEnvelope{
		APIVersion: PublicDataPlaneAdoptionEnvelopeAPIVersion, Kind: PublicDataPlaneAdoptionEnvelopeKind,
		PlanDigest: plan.Digest, ExpectedDomain: DomainAuthoritativeDNS, Plan: plan,
	}, nil
}

func VerifyPublicDataPlaneAdoptionTransaction(envelope PublicDataPlaneAdoptionTransactionEnvelope) error {
	if envelope.APIVersion != PublicDataPlaneAdoptionEnvelopeAPIVersion || envelope.Kind != PublicDataPlaneAdoptionEnvelopeKind ||
		envelope.ExpectedDomain != DomainAuthoritativeDNS || envelope.PlanDigest != envelope.Plan.Digest {
		return fmt.Errorf("public data-plane adoption transaction identity mismatch")
	}
	return VerifyPublicDataPlaneAdoptionPlan(envelope.Plan)
}

func AppendPublicDataPlaneAdoptionTrace(
	trace PublicDataPlaneAdoptionExecutionTrace,
	planDigest, phase, at string,
) (PublicDataPlaneAdoptionExecutionTrace, error) {
	if trace.APIVersion == "" {
		trace = PublicDataPlaneAdoptionExecutionTrace{
			APIVersion: PublicDataPlaneAdoptionEnvelopeAPIVersion,
			Kind:       PublicDataPlaneAdoptionTraceKind, Policy: PublicDataPlaneAdoptionPolicy,
			PlanDigest: planDigest,
		}
	} else if err := VerifyPublicDataPlaneAdoptionTrace(trace); err != nil {
		return PublicDataPlaneAdoptionExecutionTrace{}, err
	}
	if trace.PlanDigest != planDigest {
		return PublicDataPlaneAdoptionExecutionTrace{}, fmt.Errorf("public data-plane adoption trace plan mismatch")
	}
	trace.Events = append(trace.Events, PublicDataPlaneAdoptionTraceEvent{
		Sequence: len(trace.Events) + 1, Phase: phase, At: at,
	})
	trace.Digest = publicDataPlaneAdoptionTraceDigest(trace)
	if err := VerifyPublicDataPlaneAdoptionTrace(trace); err != nil {
		return PublicDataPlaneAdoptionExecutionTrace{}, err
	}
	return trace, nil
}

func VerifyPublicDataPlaneAdoptionTrace(trace PublicDataPlaneAdoptionExecutionTrace) error {
	if trace.APIVersion != PublicDataPlaneAdoptionEnvelopeAPIVersion || trace.Kind != PublicDataPlaneAdoptionTraceKind ||
		trace.Policy != PublicDataPlaneAdoptionPolicy || len(trace.Events) == 0 {
		return fmt.Errorf("public data-plane adoption trace identity is invalid")
	}
	if err := validateCanonicalSHA256Digest(trace.PlanDigest, "public data-plane adoption trace plan digest"); err != nil {
		return err
	}
	transitions := map[string]map[string]bool{
		"":                          {"prepared": true},
		"prepared":                  {"lease-acquired": true},
		"lease-acquired":            {"prewrite-verified": true, "lease-released": true},
		"prewrite-verified":         {"fence-armed": true},
		"fence-armed":               {"apply-started": true},
		"apply-started":             {"apply-succeeded": true, "apply-failed": true},
		"apply-succeeded":           {"baseline-finalized": true, "apply-verification-failed": true},
		"apply-failed":              {"restore-started": true},
		"apply-verification-failed": {"restore-started": true},
		"restore-started":           {"restore-succeeded": true, "restore-failed": true},
		"restore-succeeded":         {"recovery-fenced": true},
		"restore-failed":            {"recovery-fenced": true},
		"baseline-finalized":        {"lease-released": true},
	}
	previousPhase := ""
	var previousTime time.Time
	applyCount, restoreCount := 0, 0
	for index, event := range trace.Events {
		if event.Sequence != index+1 || !transitions[previousPhase][event.Phase] {
			return fmt.Errorf("public data-plane adoption trace transition is invalid")
		}
		parsed, err := time.Parse(time.RFC3339Nano, event.At)
		if err != nil || (!previousTime.IsZero() && parsed.Before(previousTime)) {
			return fmt.Errorf("public data-plane adoption trace time is invalid")
		}
		if event.Phase == "apply-started" {
			applyCount++
		}
		if event.Phase == "restore-started" {
			restoreCount++
		}
		previousPhase, previousTime = event.Phase, parsed
	}
	if applyCount > 1 || restoreCount > 1 || restoreCount > applyCount {
		return fmt.Errorf("public data-plane adoption trace violates exactly-once execution")
	}
	if publicDataPlaneAdoptionTraceDigest(trace) != trace.Digest {
		return fmt.Errorf("public data-plane adoption trace digest mismatch")
	}
	return nil
}

func NewPublicDataPlaneAdoptionRecoveryWAL(
	envelope PublicDataPlaneAdoptionTransactionEnvelope,
	transaction, restore []byte,
	leaseNamespace, leaseName, leaseOwner, leaseToken, at string,
	originRunID string,
	originRunAttempt int,
) (PublicDataPlaneAdoptionRecoveryWAL, error) {
	if err := VerifyPublicDataPlaneAdoptionTransaction(envelope); err != nil {
		return PublicDataPlaneAdoptionRecoveryWAL{}, err
	}
	document, err := decodePublicDataPlaneRestoreSnapshot(restore)
	if err != nil {
		return PublicDataPlaneAdoptionRecoveryWAL{}, err
	}
	if len(transaction)+len(restore) > 96<<10 {
		return PublicDataPlaneAdoptionRecoveryWAL{}, fmt.Errorf("public data-plane adoption recovery artifacts exceed bounded size")
	}
	if !validContractText(leaseNamespace, 253) || !validContractText(leaseName, 253) ||
		!validContractText(leaseOwner, 253) || !validContractText(leaseToken, 253) {
		return PublicDataPlaneAdoptionRecoveryWAL{}, fmt.Errorf("public data-plane adoption recovery Lease identity is invalid")
	}
	originRunValue, originRunErr := strconv.ParseUint(originRunID, 10, 63)
	if originRunErr != nil || originRunValue == 0 || strconv.FormatUint(originRunValue, 10) != originRunID || originRunAttempt != 1 {
		return PublicDataPlaneAdoptionRecoveryWAL{}, fmt.Errorf("public data-plane adoption recovery origin run is invalid")
	}
	parsedAt, err := time.Parse(time.RFC3339Nano, at)
	if err != nil || parsedAt.IsZero() {
		return PublicDataPlaneAdoptionRecoveryWAL{}, fmt.Errorf("public data-plane adoption recovery time is invalid")
	}
	members := make([]PublicDataPlaneAdoptionRecoveryMember, 0, len(document.Members))
	for _, member := range document.Members {
		members = append(members, PublicDataPlaneAdoptionRecoveryMember{
			Name: member.Name, UIDDigest: digestPublicDataPlaneText(member.UID),
			EdgeImageRef: member.EdgeImageRef, SpecDigest: member.SpecDigest,
		})
	}
	wal := PublicDataPlaneAdoptionRecoveryWAL{
		APIVersion: PublicDataPlaneAdoptionEnvelopeAPIVersion, Kind: PublicDataPlaneAdoptionRecoveryWALKind,
		Policy: PublicDataPlaneAdoptionPolicy, PlanDigest: envelope.Plan.Digest,
		TransactionDigest: digestBytesSHA256(transaction), RestoreDigest: digestBytesSHA256(restore),
		SourceCommit: envelope.Plan.SourceCommit, BaseRevision: envelope.Plan.BaseRevision,
		TargetRevision: envelope.Plan.TargetRevision, LeaseNamespace: leaseNamespace, LeaseName: leaseName,
		LeaseOwner: leaseOwner, LeaseTokenDigest: digestPublicDataPlaneText(leaseToken),
		OriginRunID: originRunID, OriginRunAttempt: originRunAttempt,
		Phase: "lease-acquired", Sequence: 1, UpdatedAt: at, Members: members,
	}
	wal.Digest = publicDataPlaneAdoptionRecoveryWALDigest(wal)
	if err := VerifyPublicDataPlaneAdoptionRecoveryArtifacts(wal, transaction, restore); err != nil {
		return PublicDataPlaneAdoptionRecoveryWAL{}, err
	}
	return wal, nil
}

func AdvancePublicDataPlaneAdoptionRecoveryWAL(
	wal PublicDataPlaneAdoptionRecoveryWAL,
	leaseOwner, leaseToken, phase, at, baselineDigest string,
) (PublicDataPlaneAdoptionRecoveryWAL, error) {
	if err := VerifyPublicDataPlaneAdoptionRecoveryWAL(wal); err != nil {
		return PublicDataPlaneAdoptionRecoveryWAL{}, err
	}
	if wal.LeaseOwner != leaseOwner || wal.LeaseTokenDigest != digestPublicDataPlaneText(leaseToken) {
		return PublicDataPlaneAdoptionRecoveryWAL{}, fmt.Errorf("public data-plane adoption recovery Lease owner mismatch")
	}
	transitions := map[string]map[string]bool{
		"lease-acquired":            {"fence-armed": true},
		"fence-armed":               {"apply-started": true, "restore-started": true, "aborted-before-apply": true},
		"apply-started":             {"apply-failed": true, "apply-succeeded": true, "restore-started": true},
		"apply-failed":              {"restore-started": true},
		"apply-succeeded":           {"apply-verification-failed": true, "baseline-finalized": true, "restore-started": true},
		"apply-verification-failed": {"restore-started": true},
		"restore-started":           {"restore-failed": true, "restore-succeeded": true, "restore-succeeded-awaiting-helm-compensation": true},
		"restore-failed":            {},
		"restore-succeeded":         {},
		"restore-succeeded-awaiting-helm-compensation": {},
		"baseline-finalized":                           {},
		"aborted-before-apply":                         {},
	}
	if !transitions[wal.Phase][phase] {
		return PublicDataPlaneAdoptionRecoveryWAL{}, fmt.Errorf("public data-plane adoption recovery transition is invalid")
	}
	parsed, err := time.Parse(time.RFC3339Nano, at)
	previous, previousErr := time.Parse(time.RFC3339Nano, wal.UpdatedAt)
	if err != nil || previousErr != nil || parsed.Before(previous) {
		return PublicDataPlaneAdoptionRecoveryWAL{}, fmt.Errorf("public data-plane adoption recovery time is invalid")
	}
	if phase == "apply-started" {
		wal.ApplyAttempts++
	}
	if phase == "restore-started" {
		wal.RestoreAttempts++
	}
	if phase == "baseline-finalized" {
		if err := validateCanonicalSHA256Digest(baselineDigest, "public data-plane adoption recovery baseline digest"); err != nil {
			return PublicDataPlaneAdoptionRecoveryWAL{}, err
		}
		wal.BaselineDigest = baselineDigest
	} else if baselineDigest != "" {
		return PublicDataPlaneAdoptionRecoveryWAL{}, fmt.Errorf("public data-plane adoption recovery baseline is out of phase")
	}
	wal.Phase, wal.Sequence, wal.UpdatedAt = phase, wal.Sequence+1, at
	wal.Digest = publicDataPlaneAdoptionRecoveryWALDigest(wal)
	if err := VerifyPublicDataPlaneAdoptionRecoveryWAL(wal); err != nil {
		return PublicDataPlaneAdoptionRecoveryWAL{}, err
	}
	return wal, nil
}

func VerifyPublicDataPlaneAdoptionRecoveryArtifacts(
	wal PublicDataPlaneAdoptionRecoveryWAL,
	transaction, restore []byte,
) error {
	if err := VerifyPublicDataPlaneAdoptionRecoveryWAL(wal); err != nil {
		return err
	}
	if digestBytesSHA256(transaction) != wal.TransactionDigest || digestBytesSHA256(restore) != wal.RestoreDigest {
		return fmt.Errorf("public data-plane adoption recovery artifact digest mismatch")
	}
	var envelope PublicDataPlaneAdoptionTransactionEnvelope
	decoder := json.NewDecoder(bytes.NewReader(transaction))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("public data-plane adoption recovery transaction is invalid")
	}
	if err := VerifyPublicDataPlaneAdoptionTransaction(envelope); err != nil || envelope.Plan.Digest != wal.PlanDigest {
		return fmt.Errorf("public data-plane adoption recovery transaction binding mismatch")
	}
	document, err := decodePublicDataPlaneRestoreSnapshot(restore)
	if err != nil || len(document.Members) != len(wal.Members) {
		return fmt.Errorf("public data-plane adoption recovery restore binding mismatch")
	}
	for index, member := range document.Members {
		bound := wal.Members[index]
		if bound.Name != member.Name || bound.UIDDigest != digestPublicDataPlaneText(member.UID) ||
			bound.EdgeImageRef != member.EdgeImageRef || bound.SpecDigest != member.SpecDigest {
			return fmt.Errorf("public data-plane adoption recovery member binding mismatch")
		}
	}
	return nil
}

func VerifyPublicDataPlaneAdoptionRecoveryWAL(wal PublicDataPlaneAdoptionRecoveryWAL) error {
	if wal.APIVersion != PublicDataPlaneAdoptionEnvelopeAPIVersion || wal.Kind != PublicDataPlaneAdoptionRecoveryWALKind ||
		wal.Policy != PublicDataPlaneAdoptionPolicy || wal.Sequence < 1 || len(wal.Members) == 0 || len(wal.Members) > 16 {
		return fmt.Errorf("public data-plane adoption recovery WAL identity is invalid")
	}
	for label, digest := range map[string]string{
		"plan": wal.PlanDigest, "transaction": wal.TransactionDigest, "restore": wal.RestoreDigest,
		"Lease token": wal.LeaseTokenDigest, "WAL": wal.Digest,
	} {
		if err := validateCanonicalSHA256Digest(digest, "public data-plane adoption recovery "+label+" digest"); err != nil {
			return err
		}
	}
	if err := validateTrustedGitCommit(wal.SourceCommit, "public data-plane adoption recovery source commit"); err != nil {
		return err
	}
	if err := validatePublicDataPlaneAdoptionRevisionPair(wal.BaseRevision, wal.TargetRevision); err != nil {
		return err
	}
	if !validContractText(wal.LeaseNamespace, 253) || !validContractText(wal.LeaseName, 253) || !validContractText(wal.LeaseOwner, 253) {
		return fmt.Errorf("public data-plane adoption recovery Lease identity is invalid")
	}
	if _, err := time.Parse(time.RFC3339Nano, wal.UpdatedAt); err != nil {
		return fmt.Errorf("public data-plane adoption recovery timestamp is invalid")
	}
	if wal.ApplyAttempts < 0 || wal.ApplyAttempts > 1 || wal.RestoreAttempts < 0 || wal.RestoreAttempts > 1 ||
		wal.RestoreAttempts > wal.ApplyAttempts+1 {
		return fmt.Errorf("public data-plane adoption recovery attempts violate exactly-once")
	}
	validPhases := map[string]bool{
		"lease-acquired": true,
		"fence-armed":    true, "apply-started": true, "apply-failed": true, "apply-succeeded": true,
		"apply-verification-failed": true, "restore-started": true, "restore-failed": true,
		"restore-succeeded": true, "baseline-finalized": true,
		"restore-succeeded-awaiting-helm-compensation": true,
		"aborted-before-apply":                         true,
	}
	if !validPhases[wal.Phase] {
		return fmt.Errorf("public data-plane adoption recovery phase is invalid")
	}
	if (wal.Phase == "lease-acquired" || wal.Phase == "fence-armed" || wal.Phase == "aborted-before-apply") &&
		(wal.ApplyAttempts != 0 || wal.RestoreAttempts != 0) {
		return fmt.Errorf("public data-plane adoption recovery phase counters mismatch")
	}
	if wal.Phase != "lease-acquired" && wal.Phase != "fence-armed" && wal.Phase != "aborted-before-apply" && wal.Phase != "restore-started" &&
		wal.Phase != "restore-succeeded" && wal.Phase != "restore-failed" &&
		wal.Phase != "restore-succeeded-awaiting-helm-compensation" && wal.ApplyAttempts != 1 {
		return fmt.Errorf("public data-plane adoption recovery apply counter does not match phase")
	}
	if (wal.Phase == "restore-started" || wal.Phase == "restore-succeeded" || wal.Phase == "restore-failed" ||
		wal.Phase == "restore-succeeded-awaiting-helm-compensation") && wal.RestoreAttempts != 1 {
		return fmt.Errorf("public data-plane adoption recovery restore counter does not match phase")
	}
	originRunValue, originRunErr := strconv.ParseUint(wal.OriginRunID, 10, 63)
	if originRunErr != nil || originRunValue == 0 || strconv.FormatUint(originRunValue, 10) != wal.OriginRunID || wal.OriginRunAttempt != 1 {
		return fmt.Errorf("public data-plane adoption recovery origin run is invalid")
	}
	if wal.Phase == "baseline-finalized" {
		if err := validateCanonicalSHA256Digest(wal.BaselineDigest, "public data-plane adoption recovery baseline digest"); err != nil {
			return err
		}
	} else if wal.BaselineDigest != "" {
		return fmt.Errorf("public data-plane adoption recovery baseline is out of phase")
	}
	for index, member := range wal.Members {
		if !validContractText(member.Name, 253) || validatePublicDataPlaneImageRef(member.EdgeImageRef, false) != nil {
			return fmt.Errorf("public data-plane adoption recovery member is invalid")
		}
		for label, digest := range map[string]string{"UID": member.UIDDigest, "spec": member.SpecDigest} {
			if err := validateCanonicalSHA256Digest(digest, "public data-plane adoption recovery member "+label+" digest"); err != nil {
				return err
			}
		}
		if index > 0 && wal.Members[index-1].Name >= member.Name {
			return fmt.Errorf("public data-plane adoption recovery members are not canonical")
		}
	}
	if publicDataPlaneAdoptionRecoveryWALDigest(wal) != wal.Digest {
		return fmt.Errorf("public data-plane adoption recovery WAL digest mismatch")
	}
	return nil
}

const (
	publicDataPlaneFrontChecksumAnnotation  = "checksum/edge-blue-green-front"
	publicDataPlaneWorkerChecksumAnnotation = "checksum/edge-blue-green-worker"
)

func expectedPublicDataPlaneChecksumAnnotations(intent PublicDataPlaneAdoptionIntent) map[string]string {
	expected := make(map[string]string, len(intent.Groups)*3)
	for _, group := range intent.Groups {
		expected[group.Front.Name] = publicDataPlaneFrontChecksumAnnotation
		expected[group.WorkerA.Name] = publicDataPlaneWorkerChecksumAnnotation
		expected[group.WorkerB.Name] = publicDataPlaneWorkerChecksumAnnotation
	}
	return expected
}

func validPublicDataPlaneChecksum(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func verifyPublicDataPlaneChecksumReconciliations(
	intent PublicDataPlaneAdoptionIntent,
	reconciliations []PublicDataPlaneAdoptionChecksumReconciliation,
) error {
	expected := expectedPublicDataPlaneChecksumAnnotations(intent)
	for _, group := range intent.Groups {
		if group.Front.Name != group.Base+"-front" || group.Front.Slot != "" ||
			group.WorkerA.Name != group.Base+"-worker-a" || group.WorkerA.Slot != "a" ||
			group.WorkerB.Name != group.Base+"-worker-b" || group.WorkerB.Slot != "b" {
			return fmt.Errorf("public data-plane adoption checksum cohort identity is invalid")
		}
	}
	if len(expected) != len(intent.Groups)*3 || len(reconciliations) != len(expected) || len(reconciliations) == 0 {
		return fmt.Errorf("public data-plane adoption checksum reconciliation set is incomplete")
	}
	seen := make(map[string]struct{}, len(reconciliations))
	previous := ""
	for _, reconciliation := range reconciliations {
		annotation, ok := expected[reconciliation.WorkloadName]
		if !ok || reconciliation.Annotation != annotation ||
			!validContractText(reconciliation.WorkloadName, 253) ||
			!validPublicDataPlaneChecksum(reconciliation.BaseValue) ||
			!validPublicDataPlaneChecksum(reconciliation.RenderedValue) ||
			reconciliation.BaseValue == reconciliation.RenderedValue {
			return fmt.Errorf("public data-plane adoption checksum reconciliation is invalid")
		}
		if previous != "" && previous >= reconciliation.WorkloadName {
			return fmt.Errorf("public data-plane adoption checksum reconciliations are not canonical")
		}
		if _, duplicate := seen[reconciliation.WorkloadName]; duplicate {
			return fmt.Errorf("public data-plane adoption checksum reconciliation is duplicated")
		}
		seen[reconciliation.WorkloadName] = struct{}{}
		previous = reconciliation.WorkloadName
	}
	return nil
}

func publicDataPlaneMutableTemplateAnnotations(object manifestObject) (map[string]any, error) {
	metadata, ok := nestedManifestMap(object.Object, "spec", "template", "metadata")
	if !ok {
		return nil, fmt.Errorf("public data-plane adoption template metadata is missing")
	}
	annotations, ok := metadata["annotations"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("public data-plane adoption template annotations are invalid")
	}
	return annotations, nil
}

func normalizePublicDataPlaneAdoptionChecksums(
	base, serverTarget []byte,
	spec *OwnershipSpec,
	namespace string,
	bindings map[string]string,
	intent PublicDataPlaneAdoptionIntent,
) ([]byte, []PublicDataPlaneAdoptionChecksumReconciliation, error) {
	baseObjects, baseUnknown := decodeManifest(base, spec, namespace, "adoption-checksum-base")
	targetObjects, targetUnknown := decodeManifest(serverTarget, spec, namespace, "adoption-checksum-target")
	if len(baseUnknown)+len(targetUnknown) != 0 {
		return nil, nil, fmt.Errorf("public data-plane adoption checksum manifests are incomplete")
	}
	baseIndex, baseDuplicates := indexManifestObjects(baseObjects, "adoption-checksum-base")
	targetIndex, targetDuplicates := indexManifestObjects(targetObjects, "adoption-checksum-target")
	if len(baseDuplicates)+len(targetDuplicates) != 0 || len(baseIndex) != len(targetIndex) {
		return nil, nil, fmt.Errorf("public data-plane adoption checksum manifest object set drifted")
	}

	expected := expectedPublicDataPlaneChecksumAnnotations(intent)
	reconciliations := make([]PublicDataPlaneAdoptionChecksumReconciliation, 0, len(expected))
	for name, annotation := range expected {
		key := identityKey(ObjectIdentity{
			APIGroup: "apps", Version: "v1", Kind: "DaemonSet", Namespace: namespace, Name: name,
		})
		baseObject, baseOK := baseIndex[key]
		targetObject, targetOK := targetIndex[key]
		if !baseOK || !targetOK {
			return nil, nil, fmt.Errorf("public data-plane adoption checksum workload %s is missing", name)
		}
		baseAnnotations, err := publicDataPlaneMutableTemplateAnnotations(baseObject)
		if err != nil {
			return nil, nil, err
		}
		targetAnnotations, err := publicDataPlaneMutableTemplateAnnotations(targetObject)
		if err != nil {
			return nil, nil, err
		}
		baseValue, baseString := baseAnnotations[annotation].(string)
		renderedValue, renderedString := targetAnnotations[annotation].(string)
		if !baseString || !renderedString || !validPublicDataPlaneChecksum(baseValue) ||
			!validPublicDataPlaneChecksum(renderedValue) || baseValue == renderedValue {
			return nil, nil, fmt.Errorf("public data-plane adoption checksum %s/%s is not an exact historical representation drift", name, annotation)
		}
		targetAnnotations[annotation] = baseValue
		targetIndex[key] = targetObject
		reconciliations = append(reconciliations, PublicDataPlaneAdoptionChecksumReconciliation{
			WorkloadName: name, Annotation: annotation, BaseValue: baseValue, RenderedValue: renderedValue,
		})
	}
	sort.Slice(reconciliations, func(i, j int) bool {
		return reconciliations[i].WorkloadName < reconciliations[j].WorkloadName
	})
	if err := verifyPublicDataPlaneChecksumReconciliations(intent, reconciliations); err != nil {
		return nil, nil, err
	}
	normalized, err := encodePublicDataPlaneManifestObjects(targetIndex)
	if err != nil {
		return nil, nil, err
	}
	if err := verifyPublicDataPlaneAdoptionOnlyImageDelta(base, normalized, spec, namespace, bindings, intent); err != nil {
		return nil, nil, err
	}
	return normalized, reconciliations, nil
}

func restorePublicDataPlaneAdoptionChecksums(
	canonical []byte,
	spec *OwnershipSpec,
	namespace string,
	reconciliations []PublicDataPlaneAdoptionChecksumReconciliation,
) ([]byte, error) {
	objects, unknown := decodeManifest(canonical, spec, namespace, "adoption-checksum-restore")
	if len(unknown) != 0 {
		return nil, manifestEvidenceError(unknown)
	}
	indexed, duplicates := indexManifestObjects(objects, "adoption-checksum-restore")
	if len(duplicates) != 0 {
		return nil, manifestEvidenceError(duplicates)
	}
	for _, reconciliation := range reconciliations {
		key := identityKey(ObjectIdentity{
			APIGroup: "apps", Version: "v1", Kind: "DaemonSet",
			Namespace: namespace, Name: reconciliation.WorkloadName,
		})
		object, ok := indexed[key]
		if !ok {
			return nil, fmt.Errorf("public data-plane adoption checksum workload %s is missing", reconciliation.WorkloadName)
		}
		annotations, err := publicDataPlaneMutableTemplateAnnotations(object)
		if err != nil {
			return nil, err
		}
		value, ok := annotations[reconciliation.Annotation].(string)
		if !ok || value != reconciliation.RenderedValue {
			return nil, fmt.Errorf("public data-plane adoption rendered checksum drifted for %s", reconciliation.WorkloadName)
		}
		annotations[reconciliation.Annotation] = reconciliation.BaseValue
		indexed[key] = object
	}
	return encodePublicDataPlaneManifestObjects(indexed)
}

// RenderPublicDataPlaneAdoptionTarget is the Stage1 post-renderer. It changes
// only the two image pointers of the Edge artifact selected by the signed
// adoption intent and returns
// the same canonical representation consumed by the release classifier.
func RenderPublicDataPlaneAdoptionTarget(
	rendered, ownership []byte,
	namespace string,
	intent PublicDataPlaneAdoptionIntent,
) ([]byte, error) {
	if err := VerifyPublicDataPlaneAdoptionIntent(intent); err != nil {
		return nil, err
	}
	if namespace != intent.ReleaseNamespace {
		return nil, fmt.Errorf("public data-plane adoption post-render namespace mismatch")
	}
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		return nil, fmt.Errorf("public data-plane adoption ownership: %w", err)
	}
	canonical, _, err := CanonicalizePublicDataPlaneSecretFreeManifest(rendered, spec, namespace, nil)
	if err != nil {
		return nil, err
	}
	return renderPublicDataPlaneAdoptionTargetCanonical(canonical, spec, namespace, intent)
}

func renderPublicDataPlaneAdoptionTargetCanonical(
	canonical []byte, spec *OwnershipSpec, namespace string, intent PublicDataPlaneAdoptionIntent,
) ([]byte, error) {
	objects, unknown := decodeManifest(canonical, spec, namespace, "adoption-post-render")
	if len(unknown) != 0 {
		return nil, manifestEvidenceError(unknown)
	}
	indexed, duplicates := indexManifestObjects(objects, "adoption-post-render")
	if len(duplicates) != 0 {
		return nil, manifestEvidenceError(duplicates)
	}
	for _, patch := range intent.Patches {
		key := identityKey(ObjectIdentity{
			APIGroup: "apps", Version: "v1", Kind: "DaemonSet",
			Namespace: namespace, Name: patch.WorkloadName,
		})
		object, ok := indexed[key]
		if !ok {
			return nil, fmt.Errorf("public data-plane adoption post-render worker %s is missing", patch.WorkloadName)
		}
		images, err := publicDataPlaneWorkerImageSet(object.Object, patch.WorkloadName, "spec", "template", "spec")
		if err != nil || patch.Container != "edge" {
			return nil, fmt.Errorf("public data-plane adoption post-render worker %s edge pointer is invalid", patch.WorkloadName)
		}
		images.edge["image"] = patch.ImageRef
		images.identity["image"] = patch.ImageRef
	}
	return encodePublicDataPlaneManifestObjects(indexed)
}

// RenderPublicDataPlaneAdoptionTransactionTarget is the only post-renderer
// permitted at the Stage1 Helm boundary. It proves the actual Helm input is
// the authorized base and the exact patched output is the authorized target.
func RenderPublicDataPlaneAdoptionTransactionTarget(
	rendered, ownership []byte,
	namespace string,
	envelope PublicDataPlaneAdoptionTransactionEnvelope,
	secretHMACKey []byte,
) ([]byte, error) {
	if err := VerifyPublicDataPlaneAdoptionTransaction(envelope); err != nil {
		return nil, err
	}
	if namespace != envelope.Plan.ReleaseNamespace {
		return nil, fmt.Errorf("public data-plane adoption transaction post-render namespace mismatch")
	}
	if digestBytesSHA256(ownership) != envelope.Plan.OwnershipDigest {
		return nil, fmt.Errorf("public data-plane adoption transaction ownership mismatch")
	}
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		return nil, err
	}
	secretFree, secretWitness, err := CanonicalizePublicDataPlaneSecretFreeManifest(rendered, spec, namespace, secretHMACKey)
	if err != nil {
		return nil, err
	}
	serverTarget, err := renderPublicDataPlaneAdoptionTargetCanonical(
		secretFree, spec, namespace, envelope.Plan.Intent,
	)
	if err != nil {
		return nil, err
	}
	if digestBytesSHA256(serverTarget) != envelope.Plan.ServerRenderedTargetDigest {
		return nil, fmt.Errorf("public data-plane adoption transaction server-rendered target drifted")
	}
	reconciledBase, err := restorePublicDataPlaneAdoptionChecksums(
		secretFree, spec, namespace, envelope.Plan.ChecksumReconciliations,
	)
	if err != nil {
		return nil, err
	}
	if digestBytesSHA256(reconciledBase) != envelope.Plan.BaseManifestDigest ||
		secretWitness.Digest != envelope.Plan.SecretRenderWitnessDigest ||
		secretWitness.PayloadHMAC != envelope.Plan.SecretPayloadHMAC {
		return nil, fmt.Errorf("public data-plane adoption transaction Helm input drifted")
	}
	rawCanonical, err := CanonicalizeRenderedManifest(rendered, spec, namespace)
	if err != nil {
		return nil, err
	}
	reconciledRaw, err := restorePublicDataPlaneAdoptionChecksums(
		rawCanonical, spec, namespace, envelope.Plan.ChecksumReconciliations,
	)
	if err != nil {
		return nil, err
	}
	rawTarget, err := renderPublicDataPlaneAdoptionTargetCanonical(reconciledRaw, spec, namespace, envelope.Plan.Intent)
	if err != nil {
		return nil, err
	}
	target, targetWitness, err := CanonicalizePublicDataPlaneSecretFreeManifest(rawTarget, spec, namespace, secretHMACKey)
	if err != nil {
		return nil, err
	}
	if digestBytesSHA256(target) != envelope.Plan.TargetManifestDigest ||
		targetWitness.Digest != envelope.Plan.SecretRenderWitnessDigest ||
		targetWitness.PayloadHMAC != envelope.Plan.SecretPayloadHMAC {
		return nil, fmt.Errorf("public data-plane adoption transaction Helm output drifted")
	}
	return rawTarget, nil
}

// VerifyPublicDataPlaneAdoptionPrewrite re-derives the complete plan from one
// fresh snapshot immediately before Helm is allowed to cross its write
// boundary. Both the public plan and private restore witness must be identical.
func VerifyPublicDataPlaneAdoptionPrewrite(
	plan PublicDataPlaneAdoptionPlan,
	restore []byte,
	input PublicDataPlaneAdoptionInput,
) error {
	if err := VerifyPublicDataPlaneAdoptionPlan(plan); err != nil {
		return err
	}
	freshPlan, freshRestore, err := BuildPublicDataPlaneAdoptionPlan(input)
	if err != nil {
		return fmt.Errorf("public data-plane adoption prewrite reconstruction: %w", err)
	}
	persistedPlan, err := json.Marshal(plan)
	if err != nil {
		return fmt.Errorf("public data-plane adoption persisted plan is invalid")
	}
	freshPersistedPlan, err := json.Marshal(freshPlan)
	if err != nil {
		return fmt.Errorf("public data-plane adoption fresh plan is invalid")
	}
	if !bytes.Equal(freshPersistedPlan, persistedPlan) {
		return fmt.Errorf("public data-plane adoption prewrite canonical plan drifted")
	}
	if !bytes.Equal(freshRestore, restore) {
		return fmt.Errorf("public data-plane adoption prewrite restore witness drifted")
	}
	return nil
}

// PublicDataPlaneAdoptionRestorePatches returns fail-closed UID-tested JSON
// patches. These are consumed only by the authoritative-dns adapter after the
// single Stage1 Helm apply fails; they are not a general Helm rollback.
func PublicDataPlaneAdoptionRestorePatches(restore []byte) ([]PublicDataPlaneRestorePatch, error) {
	document, err := decodePublicDataPlaneRestoreSnapshot(restore)
	if err != nil {
		return nil, err
	}
	patches := make([]PublicDataPlaneRestorePatch, 0, len(document.Members))
	for _, member := range document.Members {
		patches = append(patches, PublicDataPlaneRestorePatch{
			Name: member.Name,
			UID:  member.UID,
			Patch: []any{
				map[string]any{"op": "test", "path": "/metadata/uid", "value": member.UID},
				map[string]any{"op": "replace", "path": publicDataPlaneEdgeImagePath, "value": member.EdgeImageRef},
				map[string]any{"op": "replace", "path": publicDataPlaneEdgeIdentityImagePath, "value": member.EdgeImageRef},
			},
		})
	}
	return patches, nil
}

// VerifyPublicDataPlaneAdoptionRestore permits Kubernetes identity counters to
// advance, but requires the same objects and exact pre-stage specs.
func VerifyPublicDataPlaneAdoptionRestore(
	restore, freshSnapshot []byte,
	releaseName, namespace, fullname string,
) error {
	document, err := decodePublicDataPlaneRestoreSnapshot(restore)
	if err != nil {
		return err
	}
	fresh, err := parsePublicDataPlaneSnapshot(freshSnapshot, releaseName, namespace, fullname)
	if err != nil {
		return fmt.Errorf("public data-plane adoption restored snapshot: %w", err)
	}
	for _, member := range document.Members {
		if fresh.rawUIDs[member.Name] != member.UID {
			return fmt.Errorf("public data-plane adoption restore UID drifted for %s", member.Name)
		}
		spec, ok := fresh.rawSpecs[member.Name]
		encoded, encodeErr := json.Marshal(spec)
		if !ok || encodeErr != nil || digestBytesSHA256(encoded) != member.SpecDigest {
			return fmt.Errorf("public data-plane adoption restore spec mismatch for %s", member.Name)
		}
	}
	return nil
}

func VerifyPublicDataPlaneAdoptionRecoveryCandidate(
	restore, freshSnapshot []byte,
	namespace, targetEdgeImageRef string,
) error {
	document, err := decodePublicDataPlaneRestoreSnapshot(restore)
	if err != nil {
		return err
	}
	if err := validatePublicDataPlaneImageRef(targetEdgeImageRef, true); err != nil {
		return err
	}
	decoder := json.NewDecoder(io.LimitReader(bytes.NewReader(freshSnapshot), maxRenderedManifestBytes+1))
	decoder.UseNumber()
	var list map[string]any
	if err := decoder.Decode(&list); err != nil || list["kind"] != "List" {
		return fmt.Errorf("public data-plane recovery candidate snapshot is invalid")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("public data-plane recovery candidate snapshot contains trailing data")
	}
	items, ok := list["items"].([]any)
	if !ok {
		return fmt.Errorf("public data-plane recovery candidate snapshot is invalid")
	}
	indexed := map[string]map[string]any{}
	for _, raw := range items {
		item, itemOK := raw.(map[string]any)
		metadata, metadataOK := item["metadata"].(map[string]any)
		if !itemOK || !metadataOK || item["apiVersion"] != "apps/v1" || item["kind"] != "DaemonSet" ||
			metadata["namespace"] != namespace {
			continue
		}
		name, _ := metadata["name"].(string)
		if _, duplicate := indexed[name]; duplicate {
			return fmt.Errorf("public data-plane recovery candidate is duplicated")
		}
		indexed[name] = item
	}
	for _, member := range document.Members {
		item, exists := indexed[member.Name]
		if !exists {
			return fmt.Errorf("public data-plane recovery candidate %s is missing", member.Name)
		}
		metadata := item["metadata"].(map[string]any)
		uid, _ := metadata["uid"].(string)
		if uid != member.UID || metadata["deletionTimestamp"] != nil {
			return fmt.Errorf("public data-plane recovery candidate %s identity drifted", member.Name)
		}
		spec, ok := item["spec"].(map[string]any)
		if !ok {
			return fmt.Errorf("public data-plane recovery candidate %s spec is invalid", member.Name)
		}
		clone := cloneManifestMap(spec)
		images, imageErr := publicDataPlaneWorkerImageSet(clone, member.Name, "template", "spec")
		if imageErr != nil || (images.edgeRef != member.EdgeImageRef && images.edgeRef != targetEdgeImageRef) {
			return fmt.Errorf("public data-plane recovery candidate %s edge image is outside the transaction", member.Name)
		}
		images.edge["image"] = member.EdgeImageRef
		images.identity["image"] = member.EdgeImageRef
		encoded, encodeErr := json.Marshal(clone)
		if encodeErr != nil || digestBytesSHA256(encoded) != member.SpecDigest {
			return fmt.Errorf("public data-plane recovery candidate %s non-image spec drifted", member.Name)
		}
	}
	return nil
}

// FinalizePublicDataPlaneAdoptionBaseline verifies the new Helm release and a
// fresh observed snapshot before producing the immutable Stage2 handoff.
func FinalizePublicDataPlaneAdoptionBaseline(
	plan PublicDataPlaneAdoptionPlan,
	currentRevision string,
	currentManifest, freshSnapshot, freshObserved, ownership []byte,
) (PublicDataPlaneAdoptionBaseline, error) {
	if err := VerifyPublicDataPlaneAdoptionPlan(plan); err != nil {
		return PublicDataPlaneAdoptionBaseline{}, err
	}
	if currentRevision != plan.TargetRevision || digestBytesSHA256(currentManifest) != plan.TargetManifestDigest {
		return PublicDataPlaneAdoptionBaseline{}, fmt.Errorf("public data-plane adoption final Helm release mismatch")
	}
	if err := VerifyObservedLiveImageManifest(
		currentManifest, freshObserved, ownership, plan.ReleaseNamespace,
	); err != nil {
		return PublicDataPlaneAdoptionBaseline{}, fmt.Errorf("public data-plane adoption final observed witness: %w", err)
	}
	freshIntent, err := BuildPublicDataPlaneAdoptionIntent(
		freshSnapshot, plan.SourceCommit, plan.Intent.ReleaseName,
		plan.Intent.ReleaseNamespace, plan.Intent.ReleaseFullname,
	)
	if err != nil {
		return PublicDataPlaneAdoptionBaseline{}, err
	}
	if freshIntent.Record != plan.Intent.Record || freshIntent.AdoptionSlot != plan.Intent.AdoptionSlot ||
		freshIntent.TargetEdgeImageRef != plan.Intent.TargetEdgeImageRef ||
		!reflect.DeepEqual(freshIntent.Patches, plan.Intent.Patches) {
		return PublicDataPlaneAdoptionBaseline{}, fmt.Errorf("public data-plane adoption final record or cohort drifted")
	}
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		return PublicDataPlaneAdoptionBaseline{}, err
	}
	canonical, err := CanonicalizeRenderedManifest(currentManifest, spec, plan.ReleaseNamespace)
	if err != nil || !bytes.Equal(canonical, currentManifest) {
		return PublicDataPlaneAdoptionBaseline{}, fmt.Errorf("public data-plane adoption final manifest is not canonical")
	}
	objects, unknown := decodeManifest(freshObserved, spec, plan.ReleaseNamespace, "adoption-final-observed")
	if len(unknown) != 0 {
		return PublicDataPlaneAdoptionBaseline{}, manifestEvidenceError(unknown)
	}
	indexed, duplicates := indexManifestObjects(objects, "adoption-final-observed")
	if len(duplicates) != 0 {
		return PublicDataPlaneAdoptionBaseline{}, manifestEvidenceError(duplicates)
	}
	for _, patch := range plan.Intent.Patches {
		key := identityKey(ObjectIdentity{
			APIGroup: "apps", Version: "v1", Kind: "DaemonSet",
			Namespace: plan.ReleaseNamespace, Name: patch.WorkloadName,
		})
		object, exists := indexed[key]
		if !exists {
			return PublicDataPlaneAdoptionBaseline{}, fmt.Errorf("public data-plane adoption final worker %s is missing", patch.WorkloadName)
		}
		edge, _, imageErr := publicDataPlaneWorkerImages(object)
		if imageErr != nil || edge != patch.ImageRef {
			return PublicDataPlaneAdoptionBaseline{}, fmt.Errorf("public data-plane adoption final worker %s image mismatch", patch.WorkloadName)
		}
	}
	return NewPublicDataPlaneStage2Baseline(plan)
}

func VerifyPublicDataPlaneStage2Baseline(
	baseline PublicDataPlaneAdoptionBaseline,
	currentRevision string,
	currentManifest []byte,
) error {
	if baseline.APIVersion != PublicDataPlaneAdoptionAPIVersion || baseline.Kind != PublicDataPlaneAdoptionBaselineKind ||
		baseline.Policy != PublicDataPlaneAdoptionPolicy {
		return fmt.Errorf("public data-plane Stage2 baseline identity is unsupported")
	}
	if baseline.HelmRevision != currentRevision || baseline.ManifestDigest != digestBytesSHA256(currentManifest) {
		return fmt.Errorf("public data-plane Stage2 baseline does not match the live Helm base")
	}
	if err := validateCanonicalSHA256Digest(baseline.Stage1PlanDigest, "public data-plane Stage1 plan digest"); err != nil {
		return err
	}
	if publicDataPlaneAdoptionBaselineDigest(baseline) != baseline.Digest {
		return fmt.Errorf("public data-plane Stage2 baseline digest mismatch")
	}
	return nil
}

func VerifyPublicDataPlaneAdoptionRecoveryBase(
	envelope PublicDataPlaneAdoptionTransactionEnvelope,
	currentRevision string,
	currentManifest []byte,
) error {
	if err := VerifyPublicDataPlaneAdoptionTransaction(envelope); err != nil {
		return err
	}
	if currentRevision != envelope.Plan.BaseRevision ||
		digestBytesSHA256(currentManifest) != envelope.Plan.BaseManifestDigest {
		return fmt.Errorf("public data-plane adoption recovery Helm metadata is not the Stage1 base")
	}
	return nil
}

func VerifyPublicDataPlaneStage2Handoff(
	baseline PublicDataPlaneAdoptionBaseline,
	trace PublicDataPlaneAdoptionExecutionTrace,
	currentRevision string,
	currentManifest []byte,
) error {
	if err := VerifyPublicDataPlaneStage2Baseline(baseline, currentRevision, currentManifest); err != nil {
		return err
	}
	if err := VerifyPublicDataPlaneAdoptionTrace(trace); err != nil {
		return err
	}
	last := trace.Events[len(trace.Events)-1]
	if last.Phase != "lease-released" || trace.PlanDigest != baseline.Stage1PlanDigest {
		return fmt.Errorf("public data-plane Stage2 handoff is not a completed Stage1 transaction")
	}
	return nil
}

func NewPublicDataPlaneStage2Baseline(plan PublicDataPlaneAdoptionPlan) (PublicDataPlaneAdoptionBaseline, error) {
	if err := VerifyPublicDataPlaneAdoptionPlan(plan); err != nil {
		return PublicDataPlaneAdoptionBaseline{}, err
	}
	baseline := PublicDataPlaneAdoptionBaseline{
		APIVersion: PublicDataPlaneAdoptionAPIVersion, Kind: PublicDataPlaneAdoptionBaselineKind,
		Policy: PublicDataPlaneAdoptionPolicy, SourceCommit: plan.SourceCommit,
		HelmRevision: plan.TargetRevision, ManifestDigest: plan.TargetManifestDigest,
		Stage1PlanDigest: plan.Digest, Record: plan.Intent.Record,
	}
	baseline.Digest = publicDataPlaneAdoptionBaselineDigest(baseline)
	return baseline, nil
}

func verifyPublicDataPlaneAdoptionOnlyImageDelta(
	baseManifest, targetManifest []byte,
	spec *OwnershipSpec,
	namespace string,
	bindings map[string]string,
	intent PublicDataPlaneAdoptionIntent,
) error {
	rendered := ClassifyRendered(baseManifest, targetManifest, spec, RenderedOptions{
		DefaultNamespace: namespace, Bindings: bindings,
	})
	if !isExactTransactionDomain(rendered.Domains, DomainAuthoritativeDNS) || len(rendered.Unknown) != 0 ||
		len(rendered.Evidence) == 0 {
		return fmt.Errorf("public data-plane adoption target is not exact authoritative-dns")
	}
	allowed := map[string]struct{}{}
	for _, patch := range intent.Patches {
		allowed[patch.WorkloadName] = struct{}{}
	}
	for _, evidence := range rendered.Evidence {
		if evidence.Ignored || !isExactTransactionDomain(evidence.Domains, DomainAuthoritativeDNS) ||
			!publicDataPlaneBoundEdgeImagePaths(evidence.Paths) {
			return fmt.Errorf("public data-plane adoption target changes a non-edge-image field")
		}
		const prefix = "apps/v1 DaemonSet "
		if !strings.HasPrefix(evidence.Subject, prefix+namespace+"/") {
			return fmt.Errorf("public data-plane adoption target changes an unexpected object")
		}
		name := strings.TrimPrefix(evidence.Subject, prefix+namespace+"/")
		if _, ok := allowed[name]; !ok {
			return fmt.Errorf("public data-plane adoption target changes an unrecorded worker")
		}
		delete(allowed, name)
	}
	if len(allowed) != 0 || len(rendered.Evidence) != len(intent.Patches) {
		return fmt.Errorf("public data-plane adoption target image patch set is incomplete")
	}

	baseObjects, baseUnknown := decodeManifest(baseManifest, spec, namespace, "adoption-image-base")
	targetObjects, targetUnknown := decodeManifest(targetManifest, spec, namespace, "adoption-image-target")
	baseIndex, baseDuplicates := indexManifestObjects(baseObjects, "adoption-image-base")
	targetIndex, targetDuplicates := indexManifestObjects(targetObjects, "adoption-image-target")
	if len(baseUnknown)+len(targetUnknown)+len(baseDuplicates)+len(targetDuplicates) != 0 || len(baseIndex) != len(targetIndex) {
		return fmt.Errorf("public data-plane adoption image manifests are incomplete")
	}
	for _, patch := range intent.Patches {
		key := identityKey(ObjectIdentity{
			APIGroup: "apps", Version: "v1", Kind: "DaemonSet", Namespace: namespace, Name: patch.WorkloadName,
		})
		baseObject, baseOK := baseIndex[key]
		targetObject, targetOK := targetIndex[key]
		if !baseOK || !targetOK {
			return fmt.Errorf("public data-plane adoption image worker %s is missing", patch.WorkloadName)
		}
		baseImages, baseErr := publicDataPlaneWorkerImageSet(baseObject.Object, patch.WorkloadName, "spec", "template", "spec")
		targetImages, targetErr := publicDataPlaneWorkerImageSet(targetObject.Object, patch.WorkloadName, "spec", "template", "spec")
		if baseErr != nil || targetErr != nil {
			return fmt.Errorf("public data-plane adoption image worker %s container shape is invalid", patch.WorkloadName)
		}
		if patch.Container != "edge" || targetImages.edgeRef != patch.ImageRef {
			return fmt.Errorf("public data-plane adoption image worker %s edge pointer is invalid", patch.WorkloadName)
		}
		targetImages.edge["image"] = baseImages.edgeRef
		targetImages.identity["image"] = baseImages.edgeRef
		targetIndex[key] = targetObject
	}
	reversed, err := encodePublicDataPlaneManifestObjects(targetIndex)
	if err != nil || !bytes.Equal(reversed, baseManifest) {
		return fmt.Errorf("public data-plane adoption target is not byte-equivalent after reversing exact edge images")
	}
	return nil
}

func verifyPublicDataPlaneAdoptionManifests(
	input PublicDataPlaneAdoptionInput,
	intent PublicDataPlaneAdoptionIntent,
	spec *OwnershipSpec,
) error {
	if err := verifyPublicDataPlaneAdoptionOnlyImageDelta(
		input.BaseManifest, input.TargetManifest, spec, input.ReleaseNamespace, input.Bindings, intent,
	); err != nil {
		return err
	}
	allowed := map[string]string{}
	for _, patch := range intent.Patches {
		allowed[patch.WorkloadName] = patch.ImageRef
	}
	baseObjects, baseUnknown := decodeManifest(input.BaseManifest, spec, input.ReleaseNamespace, "adoption-base")
	targetObjects, targetUnknown := decodeManifest(input.TargetManifest, spec, input.ReleaseNamespace, "adoption-target")
	observedObjects, observedUnknown := decodeManifest(input.ObservedLive, spec, input.ReleaseNamespace, "adoption-observed")
	if len(baseUnknown)+len(targetUnknown)+len(observedUnknown) != 0 {
		return fmt.Errorf("public data-plane adoption manifest objects are incomplete")
	}
	baseIndex, baseDuplicate := indexManifestObjects(baseObjects, "adoption-base")
	targetIndex, targetDuplicate := indexManifestObjects(targetObjects, "adoption-target")
	observedIndex, observedDuplicate := indexManifestObjects(observedObjects, "adoption-observed")
	if len(baseDuplicate)+len(targetDuplicate)+len(observedDuplicate) != 0 {
		return fmt.Errorf("public data-plane adoption manifest identities are duplicated")
	}
	for _, group := range intent.Groups {
		members := []PublicDataPlaneAdoptionMemberEvidence{group.WorkerA, group.WorkerB}
		for _, member := range members {
			key := identityKey(ObjectIdentity{APIGroup: "apps", Version: "v1", Kind: "DaemonSet", Namespace: input.ReleaseNamespace, Name: member.Name})
			base, baseOK := baseIndex[key]
			target, targetOK := targetIndex[key]
			observed, observedOK := observedIndex[key]
			if !baseOK || !targetOK || !observedOK {
				return fmt.Errorf("public data-plane adoption worker %s is missing from a manifest", member.Name)
			}
			baseEdge, baseCaddy, err := publicDataPlaneWorkerImages(base)
			if err != nil {
				return err
			}
			targetEdge, targetCaddy, err := publicDataPlaneWorkerImages(target)
			if err != nil {
				return err
			}
			observedEdge, observedCaddy, err := publicDataPlaneWorkerImages(observed)
			if err != nil {
				return err
			}
			if baseCaddy != targetCaddy || baseCaddy != observedCaddy ||
				digestPublicDataPlaneText(baseCaddy) != member.CaddyImageDigest {
				return fmt.Errorf("public data-plane adoption worker %s caddy image drifted", member.Name)
			}
			expectedTarget, patched := allowed[member.Name]
			if patched {
				if targetEdge != expectedTarget {
					return fmt.Errorf("public data-plane adoption worker %s target image mismatch", member.Name)
				}
			} else if targetEdge != baseEdge {
				return fmt.Errorf("public data-plane adoption worker %s changed outside the adoption set", member.Name)
			}
			if group.Serving && member.Slot == group.AdoptionSlot {
				if observedEdge != intent.TargetEdgeImageRef {
					return fmt.Errorf("public data-plane adoption active worker %s is not already at target", member.Name)
				}
			} else if !group.Serving {
				markerValue := observed.Annotations[DisabledPublicEdgeWorkerObservationAnnotation]
				marker, markerErr := parseDisabledPublicEdgeWorkerObservation(markerValue, true)
				if markerErr != nil || marker.Generation != member.Generation || marker.ObservedGeneration != member.ObservedGeneration ||
					marker.DesiredNumberScheduled != 0 || marker.CurrentNumberScheduled != 0 || marker.NumberReady != 0 ||
					marker.NumberAvailable != 0 || marker.UpdatedNumberScheduled != 0 || marker.NumberUnavailable != 0 ||
					marker.NumberMisscheduled != 0 {
					return fmt.Errorf("public data-plane adoption disabled worker %s marker mismatch", member.Name)
				}
			}
		}
	}
	return nil
}

func parsePublicDataPlaneSnapshot(data []byte, releaseName, namespace, fullname string) (publicDataPlaneSnapshot, error) {
	decoder := json.NewDecoder(io.LimitReader(bytes.NewReader(data), maxRenderedManifestBytes+1))
	decoder.UseNumber()
	var document map[string]any
	if err := decoder.Decode(&document); err != nil {
		return publicDataPlaneSnapshot{}, fmt.Errorf("decode public data-plane snapshot: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane snapshot contains trailing data")
	}
	items, ok := document["items"].([]any)
	if !ok || (document["kind"] != nil && document["kind"] != "List") {
		return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane snapshot is not a Kubernetes List")
	}
	canonicalItems := make([]map[string]any, 0, len(items))
	var record map[string]any
	members := map[string]publicDataPlaneSnapshotMember{}
	for _, itemValue := range items {
		item, ok := itemValue.(map[string]any)
		if !ok {
			return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane snapshot item is invalid")
		}
		metadata, _ := item["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		itemNamespace, _ := metadata["namespace"].(string)
		if itemNamespace == "" {
			itemNamespace = namespace
		}
		if itemNamespace != namespace {
			continue
		}
		if item["apiVersion"] == "v1" && item["kind"] == "ConfigMap" && name == fullname+"-public-data-plane-release" {
			if record != nil {
				return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane release record is duplicated")
			}
			record = item
			canonicalItems = append(canonicalItems, item)
			continue
		}
		if item["apiVersion"] != "apps/v1" || item["kind"] != "DaemonSet" {
			continue
		}
		labels, err := stringMap(metadata["labels"], "public data-plane DaemonSet labels")
		if err != nil {
			continue
		}
		if labels["app.kubernetes.io/instance"] != releaseName || labels["fugue.io/rollout-subsystem"] != "public-data-plane" {
			continue
		}
		mode := labels["fugue.io/rollout-mode"]
		if mode != "node-local-blue-green-front" && mode != "node-local-blue-green-worker" {
			continue
		}
		if _, exists := members[name]; exists {
			return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane DaemonSet %s is duplicated", name)
		}
		member, err := parsePublicDataPlaneSnapshotMember(item, releaseName, namespace)
		if err != nil {
			return publicDataPlaneSnapshot{}, err
		}
		members[name] = member
		canonicalItems = append(canonicalItems, item)
	}
	if record == nil {
		return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane release record is missing")
	}
	recordEvidence, activeSlots, recordBases, err := parsePublicDataPlaneReleaseRecord(record, releaseName, fullname)
	if err != nil {
		return publicDataPlaneSnapshot{}, err
	}
	groups := make([]PublicDataPlaneAdoptionGroupEvidence, 0, len(recordBases))
	patches := make([]PublicDataPlaneAdoptionPatch, 0, len(recordBases))
	servingSlots := map[string]struct{}{}
	targetRefs := map[string]struct{}{}
	disabledGroups := 0
	rawSpecs := map[string]map[string]any{}
	rawUIDs := map[string]string{}
	for _, base := range recordBases {
		front, frontOK := members[base+"-front"]
		workerA, aOK := members[base+"-worker-a"]
		workerB, bOK := members[base+"-worker-b"]
		if !frontOK || !aOK || !bOK {
			return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane group %s is incomplete", base)
		}
		serving := front.evidence.Desired > 0
		slot := activeSlots[base]
		if serving {
			if slot != "a" && slot != "b" {
				return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane serving group %s has no active slot", base)
			}
			servingSlots[slot] = struct{}{}
			active := workerA
			if slot == "b" {
				active = workerB
			}
			if active.evidence.Desired <= 0 || active.evidence.Updated != active.evidence.Desired {
				return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane active worker %s is not fully updated", active.evidence.Name)
			}
			annotations, err := publicDataPlaneTemplateAnnotations(active.raw)
			if err != nil || annotations["fugue.io/public-data-plane-release-id"] != recordEvidence.ReleaseID ||
				annotations["fugue.io/public-data-plane-release-mode"] != "node-local-blue-green-worker" {
				return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane active worker %s release binding drifted", active.evidence.Name)
			}
			if err := validatePublicDataPlaneImageRef(active.edgeRef, true); err != nil {
				return publicDataPlaneSnapshot{}, err
			}
			targetRefs[active.edgeRef] = struct{}{}
		} else {
			if slot != "" {
				return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane disabled group %s has an active slot", base)
			}
			if base != fullname+"-edge-dynamic" || front.evidence.Desired != 0 ||
				!publicDataPlaneMemberIsZero(workerA.evidence) || !publicDataPlaneMemberIsZero(workerB.evidence) {
				return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane disabled group %s is not the exact dynamic zero cohort", base)
			}
			disabledGroups++
		}
		groups = append(groups, PublicDataPlaneAdoptionGroupEvidence{
			Base: base, Serving: serving, AdoptionSlot: slot,
			Front: front.evidence, WorkerA: workerA.evidence, WorkerB: workerB.evidence,
		})
		for _, member := range []publicDataPlaneSnapshotMember{front, workerA, workerB} {
			rawSpecs[member.evidence.Name] = member.raw["spec"].(map[string]any)
			metadata := member.raw["metadata"].(map[string]any)
			rawUIDs[member.evidence.Name], _ = metadata["uid"].(string)
		}
	}
	if len(activeSlots) != len(recordBases)-disabledGroups || disabledGroups != 1 || len(servingSlots) != 1 || len(targetRefs) != 1 {
		return publicDataPlaneSnapshot{}, fmt.Errorf("public data-plane adoption cohort is not one complete serving slot and one disabled dynamic group")
	}
	var adoptionSlot, targetRef string
	for value := range servingSlots {
		adoptionSlot = value
	}
	for value := range targetRefs {
		targetRef = value
	}
	for index := range groups {
		groups[index].AdoptionSlot = adoptionSlot
		worker := groups[index].WorkerA
		if adoptionSlot == "b" {
			worker = groups[index].WorkerB
		}
		patches = append(patches, PublicDataPlaneAdoptionPatch{WorkloadName: worker.Name, Container: "edge", ImageRef: targetRef})
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].Base < groups[j].Base })
	sort.Slice(patches, func(i, j int) bool { return patches[i].WorkloadName < patches[j].WorkloadName })
	sort.Slice(canonicalItems, func(i, j int) bool {
		left, _ := canonicalItems[i]["metadata"].(map[string]any)["name"].(string)
		right, _ := canonicalItems[j]["metadata"].(map[string]any)["name"].(string)
		return fmt.Sprint(canonicalItems[i]["kind"], "/", left) < fmt.Sprint(canonicalItems[j]["kind"], "/", right)
	})
	canonical, err := json.Marshal(map[string]any{"apiVersion": "v1", "kind": "List", "items": canonicalItems})
	if err != nil {
		return publicDataPlaneSnapshot{}, err
	}
	return publicDataPlaneSnapshot{
		canonical: canonical, record: recordEvidence, groups: groups, patches: patches,
		slot: adoptionSlot, imageRef: targetRef, rawSpecs: rawSpecs, rawUIDs: rawUIDs,
	}, nil
}

func parsePublicDataPlaneReleaseRecord(
	record map[string]any,
	releaseName, fullname string,
) (PublicDataPlaneAdoptionRecordEvidence, map[string]string, []string, error) {
	metadata, ok := record["metadata"].(map[string]any)
	if !ok {
		return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record metadata is invalid")
	}
	uid, _ := metadata["uid"].(string)
	rv, _ := metadata["resourceVersion"].(string)
	if !validContractText(uid, 253) || !validContractText(rv, 253) || metadata["deletionTimestamp"] != nil {
		return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record identity is invalid")
	}
	labels, err := stringMap(metadata["labels"], "public data-plane release record labels")
	if err != nil || !reflect.DeepEqual(labels, map[string]string{
		"app.kubernetes.io/instance":  releaseName,
		"app.kubernetes.io/component": "public-data-plane-release",
		"fugue.io/rollout-subsystem":  "public-data-plane",
	}) {
		return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record labels are not exact")
	}
	data, err := stringMap(record["data"], "public data-plane release record data")
	if err != nil {
		return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, err
	}
	core := []string{"release_id", "mode", "active_slots", "daemonsets", "edge_resources", "caddy_resources", "git_sha", "recorded_at"}
	allowed := map[string]struct{}{}
	for _, key := range core {
		allowed[key] = struct{}{}
	}
	runtimeKeys := []string{"runtime_cgroup_memory_max", "runtime_cgroup_patched_at", "runtime_cgroup_patched_nodes"}
	runtimeCount := 0
	for _, key := range runtimeKeys {
		allowed[key] = struct{}{}
		if _, exists := data[key]; exists {
			runtimeCount++
		}
	}
	if runtimeCount != 0 && runtimeCount != len(runtimeKeys) {
		return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record runtime extension is partial")
	}
	for key := range data {
		if _, exists := allowed[key]; !exists {
			return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record data schema is not exact")
		}
	}
	for _, key := range core {
		if _, exists := data[key]; !exists {
			return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record data schema is incomplete")
		}
	}
	if data["mode"] != "node-local-blue-green" || validateTrustedGitCommit(data["git_sha"], "public data-plane release record git SHA") != nil ||
		!strings.HasSuffix(data["release_id"], "-"+data["git_sha"]) || !strings.HasPrefix(data["release_id"], "pdp-") {
		return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record identity is invalid")
	}
	if _, err := time.Parse(time.RFC3339, data["recorded_at"]); err != nil {
		return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record timestamp is invalid")
	}
	activeSlots := map[string]string{}
	if err := json.Unmarshal([]byte(data["active_slots"]), &activeSlots); err != nil || len(activeSlots) == 0 {
		return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record active slots are invalid")
	}
	for base, slot := range activeSlots {
		if !validContractText(base, 253) || (slot != "a" && slot != "b") {
			return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record active slots are invalid")
		}
	}
	bases := strings.Split(data["daemonsets"], ",")
	if len(bases) == 0 {
		return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record groups are empty")
	}
	seen := map[string]struct{}{}
	for _, base := range bases {
		if !validContractText(base, 253) || !strings.HasPrefix(base, fullname+"-edge") {
			return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record group is invalid")
		}
		if _, duplicate := seen[base]; duplicate {
			return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record group is duplicated")
		}
		seen[base] = struct{}{}
	}
	sort.Strings(bases)
	for _, field := range []string{"edge_resources", "caddy_resources"} {
		var value map[string]any
		if err := json.Unmarshal([]byte(data[field]), &value); err != nil || value == nil {
			return PublicDataPlaneAdoptionRecordEvidence{}, nil, nil, fmt.Errorf("public data-plane release record %s is invalid", field)
		}
	}
	return PublicDataPlaneAdoptionRecordEvidence{
		Name: fullname + "-public-data-plane-release", UIDDigest: digestPublicDataPlaneText(uid),
		ResourceVersionDigest: digestPublicDataPlaneText(rv), ReleaseID: data["release_id"], GitSHA: data["git_sha"],
		ActiveSlotsDigest:    digestPublicDataPlaneText(data["active_slots"]),
		DaemonSetsDigest:     digestPublicDataPlaneText(data["daemonsets"]),
		EdgeResourcesDigest:  digestPublicDataPlaneText(data["edge_resources"]),
		CaddyResourcesDigest: digestPublicDataPlaneText(data["caddy_resources"]),
	}, activeSlots, bases, nil
}

func parsePublicDataPlaneSnapshotMember(
	item map[string]any,
	releaseName, namespace string,
) (publicDataPlaneSnapshotMember, error) {
	metadata, ok := item["metadata"].(map[string]any)
	if !ok {
		return publicDataPlaneSnapshotMember{}, fmt.Errorf("public data-plane DaemonSet metadata is invalid")
	}
	name, _ := metadata["name"].(string)
	uid, _ := metadata["uid"].(string)
	rv, _ := metadata["resourceVersion"].(string)
	generation, generationOK := manifestInteger(metadata["generation"])
	if !validContractText(name, 253) || !validContractText(uid, 253) || !validContractText(rv, 253) ||
		!generationOK || generation <= 0 || metadata["deletionTimestamp"] != nil {
		return publicDataPlaneSnapshotMember{}, fmt.Errorf("public data-plane DaemonSet %s identity is invalid", name)
	}
	labels, err := stringMap(metadata["labels"], "public data-plane DaemonSet labels")
	if err != nil || labels["app.kubernetes.io/instance"] != releaseName || labels["app.kubernetes.io/managed-by"] != "Helm" ||
		labels["fugue.io/rollout-subsystem"] != "public-data-plane" || labels["helm.sh/chart"] == "" {
		return publicDataPlaneSnapshotMember{}, fmt.Errorf("public data-plane DaemonSet %s ownership is invalid", name)
	}
	status, ok := item["status"].(map[string]any)
	if !ok {
		return publicDataPlaneSnapshotMember{}, fmt.Errorf("public data-plane DaemonSet %s status is invalid", name)
	}
	observed, ok := manifestInteger(status["observedGeneration"])
	if !ok || observed != generation {
		return publicDataPlaneSnapshotMember{}, fmt.Errorf("public data-plane DaemonSet %s is not observed", name)
	}
	counts := map[string]int64{}
	for _, key := range []string{"desiredNumberScheduled", "currentNumberScheduled", "numberReady", "numberAvailable", "updatedNumberScheduled", "numberUnavailable", "numberMisscheduled"} {
		value := int64(0)
		if raw, exists := status[key]; exists {
			parsed, valid := manifestInteger(raw)
			if !valid || parsed < 0 {
				return publicDataPlaneSnapshotMember{}, fmt.Errorf("public data-plane DaemonSet %s %s is invalid", name, key)
			}
			value = parsed
		}
		counts[key] = value
	}
	if counts["currentNumberScheduled"] != counts["desiredNumberScheduled"] ||
		counts["numberReady"] != counts["desiredNumberScheduled"] ||
		counts["numberAvailable"] != counts["desiredNumberScheduled"] ||
		counts["numberUnavailable"] != 0 || counts["numberMisscheduled"] != 0 {
		return publicDataPlaneSnapshotMember{}, fmt.Errorf("public data-plane DaemonSet %s is not fully ready", name)
	}
	spec, ok := item["spec"].(map[string]any)
	if !ok {
		return publicDataPlaneSnapshotMember{}, fmt.Errorf("public data-plane DaemonSet %s spec is invalid", name)
	}
	mode := labels["fugue.io/rollout-mode"]
	slot := ""
	edgeRef, caddyRef := "", ""
	if mode == "node-local-blue-green-worker" {
		slot = labels["fugue.io/edge-slot"]
		if slot != "a" && slot != "b" {
			return publicDataPlaneSnapshotMember{}, fmt.Errorf("public data-plane worker %s slot is invalid", name)
		}
		images, imageErr := publicDataPlaneWorkerImageSet(item, name, "spec", "template", "spec")
		if imageErr != nil {
			return publicDataPlaneSnapshotMember{}, imageErr
		}
		edgeRef, caddyRef = images.edgeRef, images.caddyRef
	}
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return publicDataPlaneSnapshotMember{}, err
	}
	evidence := PublicDataPlaneAdoptionMemberEvidence{
		Name: name, Slot: slot, UIDDigest: digestPublicDataPlaneText(uid), ResourceVersionDigest: digestPublicDataPlaneText(rv),
		Generation: generation, ObservedGeneration: observed,
		Desired: counts["desiredNumberScheduled"], Current: counts["currentNumberScheduled"],
		Ready: counts["numberReady"], Available: counts["numberAvailable"], Updated: counts["updatedNumberScheduled"],
		Unavailable: counts["numberUnavailable"], Misscheduled: counts["numberMisscheduled"],
		SpecDigest: digestBytesSHA256(specBytes),
	}
	if edgeRef != "" {
		evidence.EdgeImageDigest = digestPublicDataPlaneText(edgeRef)
		evidence.CaddyImageDigest = digestPublicDataPlaneText(caddyRef)
	}
	return publicDataPlaneSnapshotMember{evidence: evidence, raw: item, labels: labels, edgeRef: edgeRef, caddyRef: caddyRef}, nil
}

func publicDataPlaneMemberIsZero(member PublicDataPlaneAdoptionMemberEvidence) bool {
	return member.Desired == 0 && member.Current == 0 && member.Ready == 0 && member.Available == 0 &&
		member.Updated == 0 && member.Unavailable == 0 && member.Misscheduled == 0
}

func publicDataPlaneTemplateAnnotations(item map[string]any) (map[string]string, error) {
	metadata, ok := nestedManifestMap(item, "spec", "template", "metadata")
	if !ok {
		return nil, fmt.Errorf("public data-plane template metadata is missing")
	}
	return stringMap(metadata["annotations"], "public data-plane template annotations")
}

func publicDataPlaneNestedSlice(root map[string]any, path ...string) ([]any, bool) {
	var current any = root
	for _, key := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[key]
		if !ok {
			return nil, false
		}
	}
	result, ok := current.([]any)
	return result, ok
}

type publicDataPlaneWorkerImagePointers struct {
	edge, caddy, identity map[string]any
	edgeRef, caddyRef     string
}

func publicDataPlaneWorkerImageSet(root map[string]any, name string, podSpecPath ...string) (publicDataPlaneWorkerImagePointers, error) {
	containers, ok := publicDataPlaneNestedSlice(root, append(append([]string(nil), podSpecPath...), "containers")...)
	if !ok || len(containers) != 2 {
		return publicDataPlaneWorkerImagePointers{}, fmt.Errorf("public data-plane worker %s must have edge and caddy containers", name)
	}
	initContainers, ok := publicDataPlaneNestedSlice(root, append(append([]string(nil), podSpecPath...), "initContainers")...)
	if !ok || len(initContainers) != 1 {
		return publicDataPlaneWorkerImagePointers{}, fmt.Errorf("public data-plane worker %s must have one Edge identity init container", name)
	}
	edge, edgeOK := containers[0].(map[string]any)
	caddy, caddyOK := containers[1].(map[string]any)
	identity, identityOK := initContainers[0].(map[string]any)
	edgeRef, _ := edge["image"].(string)
	caddyRef, _ := caddy["image"].(string)
	identityRef, _ := identity["image"].(string)
	if !edgeOK || !caddyOK || !identityOK || edge["name"] != "edge" || caddy["name"] != "caddy" ||
		identity["name"] != publicDataPlaneEdgeIdentityContainer {
		return publicDataPlaneWorkerImagePointers{}, fmt.Errorf("public data-plane worker %s container order is invalid", name)
	}
	if validatePublicDataPlaneImageRef(edgeRef, false) != nil || validatePublicDataPlaneImageRef(caddyRef, false) != nil ||
		validatePublicDataPlaneImageRef(identityRef, false) != nil || identityRef != edgeRef {
		return publicDataPlaneWorkerImagePointers{}, fmt.Errorf("public data-plane worker %s Edge image pointers are not bound", name)
	}
	return publicDataPlaneWorkerImagePointers{
		edge: edge, caddy: caddy, identity: identity, edgeRef: edgeRef, caddyRef: caddyRef,
	}, nil
}

func publicDataPlaneWorkerImages(object manifestObject) (string, string, error) {
	images, err := publicDataPlaneWorkerImageSet(object.Object, object.Identity.Name, "spec", "template", "spec")
	if err != nil {
		return "", "", err
	}
	return images.edgeRef, images.caddyRef, nil
}

func publicDataPlaneBoundEdgeImagePaths(paths []string) bool {
	if len(paths) != 2 {
		return false
	}
	want := map[string]struct{}{
		publicDataPlaneEdgeImagePath:         {},
		publicDataPlaneEdgeIdentityImagePath: {},
	}
	for _, path := range paths {
		if _, ok := want[path]; !ok {
			return false
		}
		delete(want, path)
	}
	return len(want) == 0
}

func validatePublicDataPlaneImageRef(value string, immutable bool) error {
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n\t ") {
		return fmt.Errorf("public data-plane image reference is invalid")
	}
	if immutable {
		parts := strings.Split(value, "@sha256:")
		if len(parts) != 2 || parts[0] == "" || len(parts[1]) != 64 {
			return fmt.Errorf("public data-plane active image is not immutable")
		}
		if _, err := hex.DecodeString(parts[1]); err != nil || strings.ToLower(parts[1]) != parts[1] {
			return fmt.Errorf("public data-plane active image digest is invalid")
		}
	}
	return nil
}

func marshalPublicDataPlaneRestoreSnapshot(snapshot publicDataPlaneSnapshot) ([]byte, error) {
	members := make([]publicDataPlaneRestoreMember, 0, len(snapshot.patches))
	for _, patch := range snapshot.patches {
		name := patch.WorkloadName
		spec := snapshot.rawSpecs[name]
		images, imageErr := publicDataPlaneWorkerImageSet(spec, name, "template", "spec")
		if imageErr != nil {
			return nil, imageErr
		}
		encoded, err := json.Marshal(spec)
		if err != nil {
			return nil, err
		}
		members = append(members, publicDataPlaneRestoreMember{
			Name: name, UID: snapshot.rawUIDs[name], EdgeImageRef: images.edgeRef, SpecDigest: digestBytesSHA256(encoded),
		})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	return json.Marshal(publicDataPlaneRestoreDocument{
		APIVersion: PublicDataPlaneAdoptionAPIVersion, Kind: "PublicDataPlaneHelmAdoptionRestoreSnapshot",
		Policy: PublicDataPlaneAdoptionPolicy, Members: members,
	})
}

func decodePublicDataPlaneRestoreSnapshot(data []byte) (publicDataPlaneRestoreDocument, error) {
	decoder := json.NewDecoder(io.LimitReader(bytes.NewReader(data), maxRenderedManifestBytes+1))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var document publicDataPlaneRestoreDocument
	if err := decoder.Decode(&document); err != nil {
		return publicDataPlaneRestoreDocument{}, fmt.Errorf("decode public data-plane restore snapshot: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return publicDataPlaneRestoreDocument{}, fmt.Errorf("public data-plane restore snapshot contains trailing data")
	}
	if document.APIVersion != PublicDataPlaneAdoptionAPIVersion ||
		document.Kind != "PublicDataPlaneHelmAdoptionRestoreSnapshot" ||
		document.Policy != PublicDataPlaneAdoptionPolicy || len(document.Members) == 0 {
		return publicDataPlaneRestoreDocument{}, fmt.Errorf("public data-plane restore snapshot identity is invalid")
	}
	names := make([]string, 0, len(document.Members))
	for _, member := range document.Members {
		if !validContractText(member.Name, 253) || !validContractText(member.UID, 253) ||
			validatePublicDataPlaneImageRef(member.EdgeImageRef, false) != nil {
			return publicDataPlaneRestoreDocument{}, fmt.Errorf("public data-plane restore member identity is invalid")
		}
		if err := validateCanonicalSHA256Digest(member.SpecDigest, "public data-plane restore spec digest"); err != nil {
			return publicDataPlaneRestoreDocument{}, fmt.Errorf("public data-plane restore member %s digest mismatch", member.Name)
		}
		names = append(names, member.Name)
	}
	sortedNames := append([]string(nil), names...)
	sort.Strings(sortedNames)
	if !reflect.DeepEqual(names, sortedNames) {
		return publicDataPlaneRestoreDocument{}, fmt.Errorf("public data-plane restore members are not canonical")
	}
	for index := 1; index < len(names); index++ {
		if names[index] == names[index-1] {
			return publicDataPlaneRestoreDocument{}, fmt.Errorf("public data-plane restore member is duplicated")
		}
	}
	return document, nil
}

func encodePublicDataPlaneManifestObjects(indexed map[string]manifestObject) ([]byte, error) {
	identities := make([]string, 0, len(indexed))
	for identity := range indexed {
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	var output bytes.Buffer
	for index, identity := range identities {
		root, err := canonicalManifestNode(normalizedObject(indexed[identity]))
		if err != nil {
			return nil, err
		}
		var document bytes.Buffer
		encoder := yaml.NewEncoder(&document)
		encoder.SetIndent(2)
		if err := encoder.Encode(root); err != nil {
			_ = encoder.Close()
			return nil, err
		}
		if err := encoder.Close(); err != nil {
			return nil, err
		}
		if index != 0 {
			output.WriteString("---\n")
		}
		output.Write(document.Bytes())
		if output.Len() > maxRenderedManifestBytes {
			return nil, fmt.Errorf("public data-plane adoption target exceeds manifest limit")
		}
	}
	return append([]byte(nil), output.Bytes()...), nil
}

func validatePublicDataPlaneAdoptionRevisionPair(base, target string) error {
	baseValue, err := strconv.ParseUint(base, 10, 31)
	if err != nil || baseValue == 0 || strconv.FormatUint(baseValue, 10) != base {
		return fmt.Errorf("public data-plane adoption base revision is invalid")
	}
	targetValue, err := strconv.ParseUint(target, 10, 31)
	if err != nil || targetValue != baseValue+1 || strconv.FormatUint(targetValue, 10) != target {
		return fmt.Errorf("public data-plane adoption target revision must immediately follow base")
	}
	return nil
}

func digestPublicDataPlaneText(value string) string { return digestBytesSHA256([]byte(value)) }

func publicDataPlaneAdoptionIntentDigest(intent PublicDataPlaneAdoptionIntent) string {
	clone := intent
	clone.Digest = ""
	encoded, err := json.Marshal(clone)
	if err != nil {
		panic(err)
	}
	return digestBytesSHA256(encoded)
}

func publicDataPlaneAdoptionPlanDigest(plan PublicDataPlaneAdoptionPlan) string {
	clone := plan
	clone.Digest = ""
	clone.Stage2.Stage1PlanDigest = ""
	encoded, err := json.Marshal(clone)
	if err != nil {
		panic(err)
	}
	return digestBytesSHA256(encoded)
}

func publicDataPlaneAdoptionBaselineDigest(baseline PublicDataPlaneAdoptionBaseline) string {
	clone := baseline
	clone.Digest = ""
	encoded, err := json.Marshal(clone)
	if err != nil {
		panic(err)
	}
	return digestBytesSHA256(encoded)
}

func publicDataPlaneAdoptionTraceDigest(trace PublicDataPlaneAdoptionExecutionTrace) string {
	clone := trace
	clone.Digest = ""
	encoded, err := json.Marshal(clone)
	if err != nil {
		panic(err)
	}
	return digestBytesSHA256(encoded)
}

func publicDataPlaneAdoptionRecoveryWALDigest(wal PublicDataPlaneAdoptionRecoveryWAL) string {
	clone := wal
	clone.Digest = ""
	encoded, err := json.Marshal(clone)
	if err != nil {
		panic(err)
	}
	return digestBytesSHA256(encoded)
}

// publicDataPlaneDigest is kept separate from rollback/activation digests so
// accidental schema reuse cannot silently create equivalent seals.
func publicDataPlaneDigest(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

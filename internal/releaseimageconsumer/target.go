package releaseimageconsumer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

const (
	lockProducer       = "fugue-release-image-lock"
	evidenceProducer   = "fugue-release-image-consumer-verifier"
	registryVerifyMode = "registry_manifest_config_and_layer_get"
	releaseWorkflow    = "deploy-control-plane"
	releasePlatform    = "linux/amd64"

	maxLockBytes                = 4 << 20
	maxHelmReleaseBytes         = 32 << 20
	maxManifestBytes            = 16 << 20
	maxManifestDocs             = 512
	maxExpandedObjects          = 4096
	maxManifestListDepth        = 8
	maxManifestContainers       = 4096
	maxYAMLNodes                = 500_000
	maxYAMLDepth                = 128
	maxYAMLKeyBytes             = 512
	maxJSONDepth                = 128
	maxJSONTokens               = 2_000_000
	maxImageCandidateBytes      = 512
	maxImageCandidatesPerScalar = 4096
	maxImageCandidates          = 500_000
	maxShellParseDepth          = 16
	maxShellLines               = 65_536
	maxShellHeredocs            = 4_096
)

var (
	digestPattern      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	gitRevisionPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
	kubeKindPattern    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,62}$`)
	dnsLabelPattern    = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
	tagPattern         = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
	repositoryPart     = regexp.MustCompile(`^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$`)
	registryLabel      = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$`)
	registryPort       = regexp.MustCompile(`^[1-9][0-9]{0,4}$`)
	positiveInteger    = regexp.MustCompile(`^[1-9][0-9]*$`)
	helmPathPattern    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*(?:\[(?:0|[1-9][0-9]*)\])*(?:\.[A-Za-z][A-Za-z0-9_-]*(?:\[(?:0|[1-9][0-9]*)\])*)*$`)
	edgeWorkerPath     = regexp.MustCompile(`^(?:edge\.image|edge\.blueGreen\.slots\.[ab]\.image|edge\.dynamic\.blueGreen\.slots\.[ab]\.image|edge\.groups\[(?:0|[1-9][0-9]*)\]\.(?:image|blueGreen\.slots\.[ab]\.image))$`)
	edgeFrontPath      = regexp.MustCompile(`^(?:edge\.blueGreen\.front\.image|edge\.dynamic\.blueGreen\.front\.image|edge\.groups\[(?:0|[1-9][0-9]*)\]\.blueGreen\.front\.image)$`)
	edgeSSHPath        = regexp.MustCompile(`^(?:edge\.sshFront\.image|edge\.groups\[(?:0|[1-9][0-9]*)\]\.sshFront\.image)$`)
	edgeDNSPath        = regexp.MustCompile(`^(?:dns\.image|dns\.groups\[(?:0|[1-9][0-9]*)\]\.image)$`)
	releaseOwner       = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$`)
	releaseRepository  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}$`)
)

var activationComponents = map[string]struct{}{
	"api": {}, "controller": {}, "drain_agent": {}, "edge": {},
	"image_cache": {}, "telemetry_agent": {},
}

var artifactComponents = map[string]struct{}{
	"api": {}, "app_ssh": {}, "controller": {}, "drain_agent": {},
	"edge": {}, "image_cache": {}, "telemetry_agent": {},
}

// Lock is the immutable release image selection produced before Helm rendering.
type Lock struct {
	SchemaVersion int          `json:"schema_version"`
	Producer      string       `json:"producer"`
	Release       LockRelease  `json:"release"`
	Artifacts     []Artifact   `json:"artifacts"`
	Activations   []Activation `json:"activations"`
	LockDigest    string       `json:"lock_digest"`
}

type LockRelease struct {
	Repository string `json:"repository"`
	Workflow   string `json:"workflow"`
	RunID      string `json:"run_id"`
	RunAttempt string `json:"run_attempt"`
	HeadSHA    string `json:"head_sha"`
	Platform   string `json:"platform"`
}

type Artifact struct {
	Component              string `json:"component"`
	Repository             string `json:"repository"`
	SourceTag              string `json:"source_tag"`
	TopDigest              string `json:"top_digest"`
	PlatformManifestDigest string `json:"platform_manifest_digest"`
	ConfigDigest           string `json:"config_digest"`
	OCIRevision            string `json:"oci_revision"`
	ImmutableRef           string `json:"immutable_ref"`
	Verification           string `json:"verification"`
}

type Activation struct {
	Component             string   `json:"component"`
	SourceMode            string   `json:"source_mode"`
	SourceTemplateRef     string   `json:"source_template_ref"`
	SelectedRef           string   `json:"selected_ref"`
	Repository            string   `json:"repository"`
	SourceTag             string   `json:"source_tag"`
	Digest                string   `json:"digest"`
	RuntimeManifestDigest string   `json:"runtime_manifest_digest"`
	PinState              string   `json:"pin_state"`
	MigrationAllowed      bool     `json:"migration_allowed"`
	HelmPath              string   `json:"helm_path"`
	Workload              Workload `json:"workload"`
}

type Workload struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Container string `json:"container"`
}

// TargetEvidence contains no rendered configuration values. It records only
// the immutable image projection proven against one exact Helm dry run.
type TargetEvidence struct {
	SchemaVersion       int                     `json:"schema_version"`
	Producer            string                  `json:"producer"`
	Verification        string                  `json:"verification"`
	LockDigest          string                  `json:"lock_digest"`
	HeadSHA             string                  `json:"head_sha"`
	Platform            string                  `json:"platform"`
	ReleaseName         string                  `json:"release_name"`
	ReleaseFullname     string                  `json:"release_fullname"`
	ReleaseNamespace    string                  `json:"release_namespace"`
	HelmReleaseVersion  int                     `json:"helm_release_version"`
	ApplyMethod         string                  `json:"apply_method"`
	ManifestDigest      string                  `json:"manifest_digest"`
	HookManifestsDigest string                  `json:"hook_manifests_digest"`
	ManagedRepositories []string                `json:"managed_repositories"`
	Bindings            []TargetEvidenceBinding `json:"bindings"`
	EvidenceDigest      string                  `json:"evidence_digest"`
}

type TargetEvidenceBinding struct {
	Component         string   `json:"component"`
	HelmPath          string   `json:"helm_path"`
	Workload          Workload `json:"workload"`
	Namespace         string   `json:"namespace"`
	SelectedRef       string   `json:"selected_ref"`
	Proof             string   `json:"proof"`
	CarrierPullPolicy string   `json:"carrier_pull_policy"`
}

// TargetExpectations binds untrusted lock/render files to values delivered by
// earlier fenced workflow steps rather than trusting self-described identity.
type TargetExpectations struct {
	LockDigest      string
	HeadSHA         string
	Repository      string
	RunID           string
	RunAttempt      string
	ReleaseFullname string
	ReleaseName     string
	Namespace       string
	LiveRevision    int
}

type helmRelease struct {
	Name             string
	Namespace        string
	Manifest         string
	HookManifests    []string
	PrePullImages    []string
	Version          int
	Status           string
	Description      string
	FirstDeployed    time.Time
	LastDeployed     time.Time
	ApplyMethod      string
	ComputedFullname string
	Groups           map[string][]string
	Topology         releaseTopology
}

type releaseTopology struct {
	RegistryGC      bool
	RegistryJanitor bool
	NodeJanitor     bool
	TopologyLabeler bool
	ImagePrePull    bool
	TelemetryAgent  bool
	ImageCache      bool
	EdgeDirect      bool
	EdgeBlueGreen   bool
	EdgeDynamic     bool
	EdgeSSHFront    bool
	DNS             bool
	MeshRecovery    bool
}

type expectedConsumer struct {
	Component   string
	HelmPath    string
	Workload    Workload
	SelectedRef string
	Proof       string
	PullPolicy  string
}

type manifestObject struct {
	APIVersion string
	Kind       string
	Namespace  string
	Name       string
	Raw        map[string]any
	Containers []manifestContainer
}

type manifestContainer struct {
	Name        string
	Image       string
	Init        bool
	Ephemeral   bool
	Command     []string
	Arguments   []string
	Environment []map[string]any
}

type commandArgvWord struct {
	value        string
	fromArgs     bool
	commandIndex int
}

type objectKey struct {
	Kind      string
	Namespace string
	Name      string
}

type bindingKey struct {
	Kind      string
	Namespace string
	Name      string
	Container string
}

type imageRef struct {
	Repository string
	Tag        string
	Digest     string
}

// VerifyTargetRender verifies every activation against the exact manifest in
// a Helm --dry-run --output json result, then scans the inverse relation so a
// managed repository cannot appear in an unclassified workload container.
func VerifyTargetRender(lockPath, helmReleaseJSONPath, repeatedHelmReleaseJSONPath string, expected TargetExpectations) (TargetEvidence, error) {
	if !validDNSLabel(expected.ReleaseFullname) {
		return TargetEvidence{}, fmt.Errorf("release fullname must be a lowercase DNS label")
	}
	if !validDNSLabel(expected.ReleaseName) || !validDNSLabel(expected.Namespace) || expected.LiveRevision < 1 || !digestPattern.MatchString(expected.LockDigest) || !gitRevisionPattern.MatchString(expected.HeadSHA) || !validReleaseRepository(expected.Repository) || !positiveInteger.MatchString(expected.RunID) || !positiveInteger.MatchString(expected.RunAttempt) {
		return TargetEvidence{}, fmt.Errorf("expected release name, namespace, and live revision are invalid")
	}
	lockBytes, err := readLimitedFile(lockPath, maxLockBytes, "release image lock")
	if err != nil {
		return TargetEvidence{}, err
	}
	lock, err := decodeAndValidateLock(lockBytes)
	if err != nil {
		return TargetEvidence{}, fmt.Errorf("validate release image lock: %w", err)
	}
	if lock.LockDigest != expected.LockDigest || lock.Release.HeadSHA != expected.HeadSHA || lock.Release.Repository != expected.Repository || lock.Release.RunID != expected.RunID || lock.Release.RunAttempt != expected.RunAttempt {
		return TargetEvidence{}, fmt.Errorf("release image lock does not match externally fenced workflow identity")
	}
	helmInput, err := openSecureInput(helmReleaseJSONPath, "Helm release JSON")
	if err != nil {
		return TargetEvidence{}, err
	}
	defer helmInput.Close()
	repeatedInput, err := openSecureInput(repeatedHelmReleaseJSONPath, "repeated Helm release JSON")
	if err != nil {
		return TargetEvidence{}, err
	}
	defer repeatedInput.Close()
	if helmInput.sameIdentity(repeatedInput) {
		return TargetEvidence{}, fmt.Errorf("repeated Helm dry-run inputs must be different single-link files")
	}
	helmBytes, err := helmInput.read(maxHelmReleaseBytes)
	if err != nil {
		return TargetEvidence{}, err
	}
	helmResult, err := decodeHelmRelease(helmBytes)
	if err != nil {
		return TargetEvidence{}, err
	}
	repeatedBytes, err := repeatedInput.read(maxHelmReleaseBytes)
	if err != nil {
		return TargetEvidence{}, err
	}
	repeatedResult, err := decodeHelmRelease(repeatedBytes)
	if err != nil {
		return TargetEvidence{}, fmt.Errorf("repeated render: %w", err)
	}
	for label, result := range map[string]helmRelease{"target": helmResult, "repeated target": repeatedResult} {
		if result.Name != expected.ReleaseName || result.Namespace != expected.Namespace || result.Version != expected.LiveRevision+1 {
			return TargetEvidence{}, fmt.Errorf("%s Helm identity or revision does not match the fenced release", label)
		}
		if result.Status != "pending-upgrade" || result.Description != "Dry run complete" {
			return TargetEvidence{}, fmt.Errorf("%s is not a completed upgrade dry run", label)
		}
		if result.ComputedFullname != expected.ReleaseFullname {
			return TargetEvidence{}, fmt.Errorf("%s computed chart fullname %q does not match %q", label, result.ComputedFullname, expected.ReleaseFullname)
		}
		if len([]byte(result.Manifest)) > maxManifestBytes {
			return TargetEvidence{}, fmt.Errorf("%s Helm manifest exceeds %d bytes", label, maxManifestBytes)
		}
	}
	if !helmResult.FirstDeployed.Equal(repeatedResult.FirstDeployed) {
		return TargetEvidence{}, fmt.Errorf("repeated Helm dry runs do not share one original deployment timestamp")
	}
	if helmResult.LastDeployed.Equal(repeatedResult.LastDeployed) {
		return TargetEvidence{}, fmt.Errorf("repeated Helm dry runs must have distinct deployment timestamps")
	}
	if helmResult.Manifest != repeatedResult.Manifest || helmResult.ApplyMethod != repeatedResult.ApplyMethod || !equalStrings(helmResult.HookManifests, repeatedResult.HookManifests) || !equalStrings(helmResult.PrePullImages, repeatedResult.PrePullImages) || !equalStringMap(helmResult.Groups, repeatedResult.Groups) || helmResult.Topology != repeatedResult.Topology {
		return TargetEvidence{}, fmt.Errorf("repeated Helm dry runs produced different manifests, hooks, or apply semantics")
	}
	yamlLimits := &yamlBudget{}
	objects, err := decodeManifestWithBudget([]byte(helmResult.Manifest), helmResult.Namespace, yamlLimits)
	if err != nil {
		return TargetEvidence{}, err
	}
	hookObjects := make([]manifestObject, 0)
	for index, manifest := range helmResult.HookManifests {
		if len([]byte(manifest)) > maxManifestBytes {
			return TargetEvidence{}, fmt.Errorf("Helm hook %d manifest exceeds %d bytes", index, maxManifestBytes)
		}
		decoded, err := decodeManifestWithBudget([]byte(manifest), helmResult.Namespace, yamlLimits)
		if err != nil {
			return TargetEvidence{}, fmt.Errorf("Helm hook %d: %w", index, err)
		}
		if len(decoded) == 0 {
			return TargetEvidence{}, fmt.Errorf("Helm hook %d manifest contains no Kubernetes object", index)
		}
		hookObjects = append(hookObjects, decoded...)
	}

	evidence, err := verifyTargetObjects(lock, objects, hookObjects, expected.ReleaseFullname, helmResult.Namespace, helmResult.Groups, helmResult.PrePullImages, helmResult.Topology)
	if err != nil {
		return TargetEvidence{}, err
	}
	evidence.SchemaVersion = 1
	evidence.Producer = evidenceProducer
	evidence.Verification = "target_render"
	evidence.LockDigest = lock.LockDigest
	evidence.HeadSHA = lock.Release.HeadSHA
	evidence.Platform = lock.Release.Platform
	evidence.ReleaseName = helmResult.Name
	evidence.ReleaseFullname = expected.ReleaseFullname
	evidence.ReleaseNamespace = helmResult.Namespace
	evidence.HelmReleaseVersion = helmResult.Version
	evidence.ApplyMethod = helmResult.ApplyMethod
	evidence.ManifestDigest = digestBytes([]byte(helmResult.Manifest))
	evidence.HookManifestsDigest = digestStringSlice(helmResult.HookManifests)
	digest, err := targetEvidenceDigest(evidence)
	if err != nil {
		return TargetEvidence{}, err
	}
	evidence.EvidenceDigest = digest
	return evidence, nil
}

func verifyTargetObjects(lock Lock, objects, hookObjects []manifestObject, releaseFullname, namespace string, groups map[string][]string, prePullImages []string, topology releaseTopology) (TargetEvidence, error) {
	byKey := make(map[objectKey]manifestObject)
	for _, object := range objects {
		key := objectKey{Kind: object.Kind, Namespace: object.Namespace, Name: object.Name}
		if _, exists := byKey[key]; exists {
			return TargetEvidence{}, fmt.Errorf("rendered manifest contains duplicate object %s/%s/%s", key.Kind, key.Namespace, key.Name)
		}
		byKey[key] = object
	}
	if err := verifyImagePrePullConfigMap(byKey, releaseFullname, namespace, prePullImages, topology.ImagePrePull); err != nil {
		return TargetEvidence{}, err
	}

	managedSet := make(map[string]struct{})
	managedRepositorySpellings := make(map[string]string)
	managedDisplaySet := make(map[string]struct{})
	managedDigests := make(map[string]struct{})
	managedNamespaces := make(map[string]struct{})
	addManagedRepository := func(repository string) error {
		canonical := canonicalRepository(repository)
		if previous, exists := managedRepositorySpellings[canonical]; exists && previous != repository {
			return fmt.Errorf("release image lock uses multiple spellings for one managed repository")
		}
		managedRepositorySpellings[canonical] = repository
		managedSet[canonical] = struct{}{}
		managedDisplaySet[repository] = struct{}{}
		return nil
	}
	for _, artifact := range lock.Artifacts {
		if err := addManagedRepository(artifact.Repository); err != nil {
			return TargetEvidence{}, err
		}
		if namespace, ok := managedRepositoryNamespace(artifact.Repository, lock.Release.Repository); ok {
			managedNamespaces[namespace] = struct{}{}
		}
		managedDigests[artifact.TopDigest] = struct{}{}
		managedDigests[artifact.PlatformManifestDigest] = struct{}{}
	}
	for _, activation := range lock.Activations {
		if err := addManagedRepository(activation.Repository); err != nil {
			return TargetEvidence{}, err
		}
		if namespace, ok := managedRepositoryNamespace(activation.Repository, lock.Release.Repository); ok {
			managedNamespaces[namespace] = struct{}{}
		}
		if activation.Digest != "" {
			managedDigests[activation.Digest] = struct{}{}
		}
		if activation.RuntimeManifestDigest != "" {
			managedDigests[activation.RuntimeManifestDigest] = struct{}{}
		}
	}
	managed := sortedSet(managedDisplaySet)

	expected, err := buildExpectedConsumers(byKey, releaseFullname, namespace, groups, topology)
	if err != nil {
		return TargetEvidence{}, err
	}
	activations := make(map[string]Activation, len(lock.Activations))
	for _, activation := range lock.Activations {
		key := consumerProjectionKey(activation.Component, activation.HelmPath, activation.Workload)
		if _, duplicate := activations[key]; duplicate {
			return TargetEvidence{}, fmt.Errorf("duplicate activation consumer projection")
		}
		activations[key] = activation
	}
	if len(expected) != len(activations) {
		return TargetEvidence{}, fmt.Errorf("rendered authoritative consumer count %d does not equal activation count %d", len(expected), len(activations))
	}
	for key, consumer := range expected {
		activation, exists := activations[key]
		if !exists {
			return TargetEvidence{}, fmt.Errorf("rendered authoritative consumer %s/%s/%s at %s has no activation", consumer.Workload.Kind, consumer.Workload.Name, consumer.Workload.Container, consumer.HelmPath)
		}
		if !activationMatchesRenderedRef(activation, consumer.SelectedRef) {
			return TargetEvidence{}, fmt.Errorf("activation %s selected image does not equal its authoritative rendered consumer", consumer.HelmPath)
		}
	}
	for key, activation := range activations {
		if _, exists := expected[key]; !exists {
			return TargetEvidence{}, fmt.Errorf("activation %s for %s/%s/%s has no authoritative rendered consumer", activation.HelmPath, activation.Workload.Kind, activation.Workload.Name, activation.Workload.Container)
		}
	}

	classified := make(map[bindingKey]Activation)
	allowedIdentityOccurrences := make(map[string]int)
	allowedExecutableOccurrences := make(map[string]int)
	bindings := make([]TargetEvidenceBinding, 0, len(lock.Activations))
	dnsContainerNames := make(map[string]struct{})

	for _, activation := range lock.Activations {
		consumer := expected[consumerProjectionKey(activation.Component, activation.HelmPath, activation.Workload)]
		key := objectKey{Kind: activation.Workload.Kind, Namespace: namespace, Name: activation.Workload.Name}
		if activation.Workload.Kind == "Configuration" {
			key.Kind = "Deployment"
		}
		object, exists := byKey[key]
		if !exists {
			return TargetEvidence{}, fmt.Errorf("activation %s/%s container %s has no rendered %s", activation.Workload.Kind, activation.Workload.Name, activation.Workload.Container, key.Kind)
		}

		binding := TargetEvidenceBinding{
			Component:         activation.Component,
			HelmPath:          activation.HelmPath,
			Workload:          activation.Workload,
			Namespace:         namespace,
			SelectedRef:       consumer.SelectedRef,
			CarrierPullPolicy: "",
		}
		if activation.Workload.Kind == "Configuration" {
			pullPolicy, err := verifyDrainCarrier(object, activation)
			if err != nil {
				return TargetEvidence{}, err
			}
			binding.Proof = consumer.Proof
			if pullPolicy != consumer.PullPolicy {
				return TargetEvidence{}, fmt.Errorf("drain-agent carrier pull policy changed during verification")
			}
			binding.CarrierPullPolicy = pullPolicy
			allowedIdentityOccurrences[activation.Repository]++
			if activation.Digest != "" {
				allowedIdentityOccurrences[activation.Digest]++
			}
		} else {
			container, err := exactContainer(object, activation.Workload.Container)
			if err != nil {
				return TargetEvidence{}, fmt.Errorf("activation %s/%s: %w", activation.Workload.Kind, activation.Workload.Name, err)
			}
			if !activationMatchesRenderedRef(activation, container.Image) {
				return TargetEvidence{}, fmt.Errorf("activation %s/%s container %s selected image does not match its authoritative rendered image", activation.Workload.Kind, activation.Workload.Name, activation.Workload.Container)
			}
			binding.Proof = "pod_template_container_image"
			allowedIdentityOccurrences[container.Image]++
			bindingKey := bindingKey{Kind: activation.Workload.Kind, Namespace: namespace, Name: activation.Workload.Name, Container: activation.Workload.Container}
			if _, duplicate := classified[bindingKey]; duplicate {
				return TargetEvidence{}, fmt.Errorf("duplicate classified rendered workload container %s/%s/%s", activation.Workload.Kind, activation.Workload.Name, activation.Workload.Container)
			}
			classified[bindingKey] = activation
			if strings.HasPrefix(activation.HelmPath, "dns.") {
				if err := verifyDNSContainer(container, activation); err != nil {
					return TargetEvidence{}, err
				}
				dnsContainerNames[container.Name] = struct{}{}
			}
			executables, err := expectedFugueExecutables(container, activation)
			if err != nil {
				return TargetEvidence{}, err
			}
			for _, executable := range executables {
				allowedExecutableOccurrences[executable]++
			}
		}
		bindings = append(bindings, binding)
	}
	if len(dnsContainerNames) > 1 {
		return TargetEvidence{}, fmt.Errorf("rendered DNS activations use multiple semantic container names")
	}

	for _, object := range objects {
		for _, container := range object.Containers {
			parsed, err := parseImageRef(container.Image)
			if err != nil {
				return TargetEvidence{}, fmt.Errorf("rendered %s/%s container %s has invalid image reference: %w", object.Kind, object.Name, container.Name, err)
			}
			managedRepository := repositoryWithinManagedBoundary(parsed.Repository, managedSet, managedNamespaces)
			_, managedDigest := managedDigests[parsed.Digest]
			if !managedRepository && !managedDigest && !repositoryLooksLikeFugue(parsed.Repository) {
				continue
			}
			key := bindingKey{Kind: object.Kind, Namespace: object.Namespace, Name: object.Name, Container: container.Name}
			activation, ok := classified[key]
			if container.Init || container.Ephemeral || !ok {
				containerClass := "container"
				if container.Init {
					containerClass = "initContainer"
				} else if container.Ephemeral {
					containerClass = "ephemeralContainer"
				}
				return TargetEvidence{}, fmt.Errorf("managed or Fugue repository appears in unclassified %s %s/%s/%s", containerClass, object.Kind, object.Name, container.Name)
			}
			if !activationMatchesRenderedRef(activation, container.Image) {
				return TargetEvidence{}, fmt.Errorf("classified %s/%s/%s changed image during inverse scan", object.Kind, object.Name, container.Name)
			}
		}
	}
	for _, object := range hookObjects {
		if len(object.Containers) > 0 {
			for _, container := range object.Containers {
				parsed, err := parseImageRef(container.Image)
				if err != nil {
					return TargetEvidence{}, fmt.Errorf("Helm hook %s/%s has invalid image reference", object.Kind, object.Name)
				}
				if container.Name == "app-ssh" || repositoryLooksLikeAppSSH(parsed.Repository) || repositoryLooksLikeFugue(parsed.Repository) || isFugueConsumerContainerName(container.Name) || (len(container.Command) > 0 && strings.HasPrefix(container.Command[0], "/usr/local/bin/fugue-")) {
					return TargetEvidence{}, fmt.Errorf("unclassified Fugue consumer appears in Helm hook %s/%s", object.Kind, object.Name)
				}
				managedRepository := repositoryWithinManagedBoundary(parsed.Repository, managedSet, managedNamespaces)
				_, managedDigest := managedDigests[parsed.Digest]
				if managedRepository || managedDigest {
					return TargetEvidence{}, fmt.Errorf("managed repository %s appears in Helm hook %s/%s", parsed.Repository, object.Kind, object.Name)
				}
			}
		}
	}
	actualIdentityOccurrences := make(map[string]int)
	actualExecutableOccurrences := make(map[string]int)
	imageCandidates := 0
	for _, object := range objects {
		if err := collectManagedIdentityOccurrences(object.Raw, "", true, managedSet, managedDigests, managedNamespaces, actualIdentityOccurrences, actualExecutableOccurrences, &imageCandidates); err != nil {
			return TargetEvidence{}, err
		}
	}
	for _, object := range hookObjects {
		if err := collectManagedIdentityOccurrences(object.Raw, "", true, managedSet, managedDigests, managedNamespaces, actualIdentityOccurrences, actualExecutableOccurrences, &imageCandidates); err != nil {
			return TargetEvidence{}, err
		}
	}
	if !equalStringCounts(actualIdentityOccurrences, allowedIdentityOccurrences) {
		return TargetEvidence{}, fmt.Errorf("managed image identity appears outside its authoritative manifest locations")
	}
	if !equalStringCounts(actualExecutableOccurrences, allowedExecutableOccurrences) {
		return TargetEvidence{}, fmt.Errorf("Fugue executable appears outside its authoritative manifest locations")
	}

	sort.Slice(bindings, func(i, j int) bool {
		left, right := bindings[i], bindings[j]
		return strings.Join([]string{left.Component, left.HelmPath, left.Workload.Kind, left.Workload.Name, left.Workload.Container}, "\x00") <
			strings.Join([]string{right.Component, right.HelmPath, right.Workload.Kind, right.Workload.Name, right.Workload.Container}, "\x00")
	})
	return TargetEvidence{ManagedRepositories: managed, Bindings: bindings}, nil
}

func verifyImagePrePullConfigMap(objects map[objectKey]manifestObject, fullname, namespace string, images []string, enabled bool) error {
	key := objectKey{Kind: "ConfigMap", Namespace: namespace, Name: fullname + "-image-prepull"}
	object, exists := objects[key]
	if !enabled {
		if exists {
			return fmt.Errorf("image pre-pull ConfigMap is rendered while image pre-pull is disabled")
		}
		return nil
	}
	if !exists || object.APIVersion != "v1" {
		return fmt.Errorf("enabled image pre-pull ConfigMap is missing")
	}
	data, ok := object.Raw["data"].(map[string]any)
	if !ok || len(data) != 1 {
		return fmt.Errorf("image pre-pull ConfigMap data must contain only images")
	}
	rendered, ok := data["images"].(string)
	if !ok || rendered != strings.Join(images, "\n") {
		return fmt.Errorf("image pre-pull ConfigMap does not exactly match effective image values")
	}
	return nil
}

func buildExpectedConsumers(objects map[objectKey]manifestObject, fullname, namespace string, groups map[string][]string, topology releaseTopology) (map[string]expectedConsumer, error) {
	result := make(map[string]expectedConsumer)
	classifiedContainers := make(map[bindingKey]struct{})
	addPod := func(component, helmPath, kind, name, container string, enabled bool) error {
		key := objectKey{Kind: kind, Namespace: namespace, Name: name}
		object, exists := objects[key]
		if !exists {
			if enabled {
				return fmt.Errorf("required rendered consumer %s/%s is missing", kind, name)
			}
			return nil
		}
		if !enabled {
			return fmt.Errorf("disabled consumer %s/%s unexpectedly rendered", kind, name)
		}
		expectedAPIVersion := "apps/v1"
		if kind == "CronJob" {
			expectedAPIVersion = "batch/v1"
		}
		if object.APIVersion != expectedAPIVersion {
			return fmt.Errorf("rendered consumer %s/%s uses apiVersion %s, expected %s", kind, name, object.APIVersion, expectedAPIVersion)
		}
		renderedContainer, err := exactContainer(object, container)
		if err != nil {
			return fmt.Errorf("authoritative consumer %s/%s: %w", kind, name, err)
		}
		if _, err := parseImageRef(renderedContainer.Image); err != nil {
			return fmt.Errorf("authoritative consumer %s/%s container %s has an invalid image reference", kind, name, container)
		}
		workload := Workload{Kind: kind, Name: name, Container: container}
		consumer := expectedConsumer{Component: component, HelmPath: helmPath, Workload: workload, SelectedRef: renderedContainer.Image, Proof: "pod_template_container_image"}
		projection := consumerProjectionKey(component, helmPath, workload)
		if _, duplicate := result[projection]; duplicate {
			return fmt.Errorf("authoritative consumer projection is duplicated for %s", helmPath)
		}
		result[projection] = consumer
		classifiedContainers[bindingKey{Kind: kind, Namespace: namespace, Name: name, Container: container}] = struct{}{}
		return nil
	}
	addDNS := func(helmPath, name string, enabled bool) error {
		key := objectKey{Kind: "DaemonSet", Namespace: namespace, Name: name}
		object, exists := objects[key]
		if !exists {
			if enabled {
				return fmt.Errorf("required rendered DNS consumer %s is missing", name)
			}
			return nil
		}
		if !enabled {
			return fmt.Errorf("disabled DNS consumer %s unexpectedly rendered", name)
		}
		container, err := semanticDNSContainer(object)
		if err != nil {
			return fmt.Errorf("authoritative DNS consumer %s: %w", name, err)
		}
		return addPod("edge", helmPath, "DaemonSet", name, container.Name, true)
	}

	fixed := []struct {
		component string
		path      string
		kind      string
		name      string
		container string
		required  bool
	}{
		{"api", "api.image", "Deployment", fullname + "-api", "api", true},
		{"controller", "controller.image", "Deployment", fullname + "-controller", "controller", true},
		{"controller", "controller.image", "CronJob", fullname + "-registry-gc", "registry-gc", topology.RegistryGC},
		{"controller", "controller.image", "CronJob", fullname + "-registry-janitor", "registry-janitor", topology.RegistryJanitor},
		{"controller", "nodeJanitor.image", "DaemonSet", fullname + "-node-janitor", "node-janitor", topology.NodeJanitor},
		{"controller", "topologyLabeler.image", "DaemonSet", fullname + "-topology-labeler", "topology-labeler", topology.TopologyLabeler},
		{"controller", "imagePrePull.image", "DaemonSet", fullname + "-image-prepull", "image-prepull", topology.ImagePrePull},
		{"telemetry_agent", "observability.agent.image", "Deployment", fullname + "-telemetry-agent", "telemetry-agent", topology.TelemetryAgent},
		{"image_cache", "imageCache.image", "DaemonSet", fullname + "-image-cache", "image-cache", topology.ImageCache},
		{"edge", "edge.image", "DaemonSet", fullname + "-edge", "edge", topology.EdgeDirect},
		{"edge", "edge.blueGreen.front.image", "DaemonSet", fullname + "-edge-front", "edge-front", topology.EdgeBlueGreen},
		{"edge", "edge.blueGreen.slots.a.image", "DaemonSet", fullname + "-edge-worker-a", "edge", topology.EdgeBlueGreen},
		{"edge", "edge.blueGreen.slots.b.image", "DaemonSet", fullname + "-edge-worker-b", "edge", topology.EdgeBlueGreen},
		{"edge", "edge.dynamic.blueGreen.front.image", "DaemonSet", fullname + "-edge-dynamic-front", "edge-front", topology.EdgeDynamic},
		{"edge", "edge.dynamic.blueGreen.slots.a.image", "DaemonSet", fullname + "-edge-dynamic-worker-a", "edge", topology.EdgeDynamic},
		{"edge", "edge.dynamic.blueGreen.slots.b.image", "DaemonSet", fullname + "-edge-dynamic-worker-b", "edge", topology.EdgeDynamic},
		{"edge", "edge.sshFront.image", "DaemonSet", fullname + "-edge-ssh-front", "ssh-front", topology.EdgeSSHFront},
		{"edge", "meshRecovery.image", "DaemonSet", fullname + "-mesh-recovery", "mesh-recovery", topology.MeshRecovery},
	}
	for _, candidate := range fixed {
		if err := addPod(candidate.component, candidate.path, candidate.kind, candidate.name, candidate.container, candidate.required); err != nil {
			return nil, err
		}
	}

	controller := objects[objectKey{Kind: "Deployment", Namespace: namespace, Name: fullname + "-controller"}]
	drainRef, pullPolicy, err := readDrainCarrier(controller)
	if err != nil {
		return nil, err
	}
	drainWorkload := Workload{Kind: "Configuration", Name: fullname + "-controller", Container: "controller"}
	drain := expectedConsumer{Component: "drain_agent", HelmPath: "runtime.strictDrain.agent.image", Workload: drainWorkload, SelectedRef: drainRef, Proof: "controller_env_carrier", PullPolicy: pullPolicy}
	result[consumerProjectionKey(drain.Component, drain.HelmPath, drain.Workload)] = drain

	if err := addDNS("dns.image", fullname+"-dns", topology.DNS); err != nil {
		return nil, err
	}

	for family, names := range groups {
		for index, group := range names {
			if family == "edge" {
				candidates := []struct {
					path, name, container string
				}{
					{fmt.Sprintf("edge.groups[%d].image", index), fullname + "-edge-" + group, "edge"},
					{fmt.Sprintf("edge.groups[%d].blueGreen.front.image", index), fullname + "-edge-" + group + "-front", "edge-front"},
					{fmt.Sprintf("edge.groups[%d].blueGreen.slots.a.image", index), fullname + "-edge-" + group + "-worker-a", "edge"},
					{fmt.Sprintf("edge.groups[%d].blueGreen.slots.b.image", index), fullname + "-edge-" + group + "-worker-b", "edge"},
					{fmt.Sprintf("edge.groups[%d].sshFront.image", index), fullname + "-edge-" + group + "-ssh-front", "ssh-front"},
				}
				for _, candidate := range candidates {
					enabled := topology.EdgeDirect
					if strings.Contains(candidate.path, ".blueGreen.") {
						enabled = topology.EdgeBlueGreen
					} else if strings.Contains(candidate.path, ".sshFront.") {
						enabled = topology.EdgeSSHFront
					}
					if err := addPod("edge", candidate.path, "DaemonSet", candidate.name, candidate.container, enabled); err != nil {
						return nil, err
					}
				}
			} else if family == "dns" {
				if err := addDNS(fmt.Sprintf("dns.groups[%d].image", index), fullname+"-dns-"+group, topology.DNS); err != nil {
					return nil, err
				}
			}
		}
	}

	for _, object := range objects {
		for _, container := range object.Containers {
			parsed, err := parseImageRef(container.Image)
			if err != nil {
				return nil, fmt.Errorf("rendered %s/%s container %s has invalid image reference", object.Kind, object.Name, container.Name)
			}
			if container.Name == "app-ssh" || repositoryLooksLikeAppSSH(parsed.Repository) || (len(container.Command) > 0 && strings.HasPrefix(container.Command[0], "/usr/local/bin/fugue-app-ssh")) {
				return nil, fmt.Errorf("app_ssh is artifact-only but appears in rendered %s/%s", object.Kind, object.Name)
			}
			_, known := classifiedContainers[bindingKey{Kind: object.Kind, Namespace: object.Namespace, Name: object.Name, Container: container.Name}]
			if known && !container.Init && !container.Ephemeral {
				continue
			}
			if strings.HasPrefix(object.Name, fullname+"-edge-") && (container.Name == "edge" || container.Name == "edge-front" || container.Name == "ssh-front") {
				return nil, fmt.Errorf("unclassified authoritative edge consumer %s/%s/%s", object.Kind, object.Name, container.Name)
			}
			if strings.HasPrefix(object.Name, fullname+"-dns-") && len(container.Command) == 1 && container.Command[0] == "/usr/local/bin/fugue-dns" {
				return nil, fmt.Errorf("unclassified authoritative DNS consumer %s/%s/%s", object.Kind, object.Name, container.Name)
			}
			if strings.HasPrefix(object.Name, fullname+"-") && isFugueConsumerContainerName(container.Name) {
				return nil, fmt.Errorf("unclassified Fugue workload consumer %s/%s/%s", object.Kind, object.Name, container.Name)
			}
			if len(container.Command) > 0 && strings.HasPrefix(container.Command[0], "/usr/local/bin/fugue-") {
				return nil, fmt.Errorf("unclassified Fugue executable consumer %s/%s/%s", object.Kind, object.Name, container.Name)
			}
		}
	}
	return result, nil
}

func repositoryLooksLikeAppSSH(repository string) bool {
	return repositoryBasename(repository) == "fugue-app-ssh"
}

func repositoryLooksLikeFugue(repository string) bool {
	return strings.HasPrefix(repositoryBasename(repository), "fugue-")
}

func repositoryBasename(repository string) string {
	if slash := strings.LastIndex(repository, "/"); slash >= 0 {
		return repository[slash+1:]
	}
	return repository
}

func managedRepositoryNamespace(repository, releaseRepository string) (string, bool) {
	parts := strings.Split(repository, "/")
	if len(parts) < 2 {
		return "", false
	}
	canonicalParts := strings.Split(canonicalRepository(repository), "/")
	if len(canonicalParts) < 3 {
		return "", false
	}
	releaseOwner := strings.ToLower(strings.SplitN(releaseRepository, "/", 2)[0])
	if canonicalParts[1] == releaseOwner {
		return strings.Join(canonicalParts[:2], "/"), true
	}
	return strings.Join(canonicalParts[:len(canonicalParts)-1], "/"), true
}

func repositoryWithinManagedBoundary(repository string, managedRepositories, managedNamespaces map[string]struct{}) bool {
	canonical := canonicalRepository(repository)
	if _, managed := managedRepositories[canonical]; managed {
		return true
	}
	for offset := strings.IndexByte(canonical, '/'); offset >= 0; {
		if _, managed := managedNamespaces[canonical[:offset]]; managed {
			return true
		}
		next := strings.IndexByte(canonical[offset+1:], '/')
		if next < 0 {
			break
		}
		offset += next + 1
	}
	return false
}

func collectManagedIdentityOccurrences(value any, field string, fieldValue bool, managedRepositories, managedDigests, managedNamespaces map[string]struct{}, occurrences, executableOccurrences map[string]int, candidateCount *int) error {
	return collectManagedIdentityOccurrencesDepth(value, field, fieldValue, managedRepositories, managedDigests, managedNamespaces, occurrences, executableOccurrences, candidateCount, 0)
}

func collectManagedIdentityOccurrencesDepth(value any, field string, fieldValue bool, managedRepositories, managedDigests, managedNamespaces map[string]struct{}, occurrences, executableOccurrences map[string]int, candidateCount *int, embeddedJSONDepth int) error {
	if candidateCount == nil {
		return fmt.Errorf("managed image scan budget is required")
	}
	if embeddedJSONDepth > maxJSONDepth {
		return fmt.Errorf("manifest embedded JSON nesting exceeds limit")
	}
	switch current := value.(type) {
	case map[string]any:
		for _, key := range []string{"command", "entrypoint", "args"} {
			if values, ok := current[key].([]any); ok && len(values) > maxImageCandidatesPerScalar {
				return fmt.Errorf("manifest command argv exceeds item limit")
			}
		}
		siblingExecutables, err := siblingCommandArgumentExecutables(current)
		if err != nil {
			return err
		}
		for _, executable := range siblingExecutables {
			executableOccurrences[executable]++
		}
		siblingPulls, err := siblingCommandArgumentImagePulls(current, managedRepositories, managedDigests, managedNamespaces)
		if err != nil {
			return err
		}
		for _, identity := range siblingPulls {
			occurrences[identity]++
		}
		for key, child := range current {
			if err := collectManagedIdentityOccurrencesDepth(key, field, false, managedRepositories, managedDigests, managedNamespaces, occurrences, executableOccurrences, candidateCount, embeddedJSONDepth); err != nil {
				return err
			}
			childField := key
			if key == "value" {
				if name, ok := current["name"].(string); ok && imageIdentityField(name) {
					childField = name
				}
			}
			if err := collectManagedIdentityOccurrencesDepth(child, childField, true, managedRepositories, managedDigests, managedNamespaces, occurrences, executableOccurrences, candidateCount, embeddedJSONDepth); err != nil {
				return err
			}
		}
	case []any:
		if pullImageContextField(field) {
			seenPull := false
			for _, child := range current {
				text, ok := child.(string)
				if !ok {
					continue
				}
				candidates, err := boundedImageCandidates(text)
				if err != nil {
					return err
				}
				inheritedPull := seenPull
				for _, token := range candidates {
					if inheritedPull {
						if identity, managed := managedIdentityCandidate(token, true, true, managedRepositories, managedDigests, managedNamespaces); managed {
							occurrences[identity]++
						}
					}
					if token == "pull" {
						seenPull = true
					}
				}
			}
		}
		entrypoints, scriptIndex := commandArrayContexts(current, field)
		for index, child := range current {
			childField := field
			if _, entrypoint := entrypoints[index]; entrypoint {
				childField = "command_entrypoint"
			} else if index == scriptIndex {
				childField = "script"
			} else if len(entrypoints) > 0 {
				childField = "command_argument"
			}
			if err := collectManagedIdentityOccurrencesDepth(child, childField, fieldValue, managedRepositories, managedDigests, managedNamespaces, occurrences, executableOccurrences, candidateCount, embeddedJSONDepth); err != nil {
				return err
			}
		}
	case string:
		if fieldValue && imageReferenceValueField(field) {
			if _, err := parseImageRef(current); err != nil {
				return fmt.Errorf("manifest image field is not a strict image reference")
			}
		}
		if fieldValue && imageRepositoryValueField(field) && validateRepository(current) != nil {
			return fmt.Errorf("manifest image repository field is invalid")
		}
		trimmed := strings.TrimSpace(current)
		if len(trimmed) >= 2 && (trimmed[0] == '{' && trimmed[len(trimmed)-1] == '}' || trimmed[0] == '[' && trimmed[len(trimmed)-1] == ']') {
			if err := scanStrictJSON([]byte(trimmed)); err != nil {
				return fmt.Errorf("manifest embedded JSON scalar is invalid")
			}
			decoder := json.NewDecoder(strings.NewReader(trimmed))
			decoder.UseNumber()
			var embedded any
			if err := decoder.Decode(&embedded); err != nil {
				return fmt.Errorf("manifest embedded JSON scalar is invalid")
			}
			if err := collectManagedIdentityOccurrencesDepth(embedded, field, fieldValue, managedRepositories, managedDigests, managedNamespaces, occurrences, executableOccurrences, candidateCount, embeddedJSONDepth+1); err != nil {
				return err
			}
		}
		candidates, err := boundedImageCandidates(current)
		if err != nil {
			return err
		}
		if *candidateCount < 0 || *candidateCount > maxImageCandidates || len(candidates) > maxImageCandidates-*candidateCount {
			return fmt.Errorf("Helm manifests exceed managed image scan candidate limit")
		}
		*candidateCount += len(candidates)
		identityField := imageIdentityField(field)
		pullField := pullImageContextField(field)
		contextExecutables, err := commandContextFugueExecutables(current, field)
		if err != nil {
			return err
		}
		for _, executable := range contextExecutables {
			executableOccurrences[executable]++
		}
		if pullField {
			pullIdentities, err := shellContextManagedImagePulls(current, managedRepositories, managedDigests, managedNamespaces, candidateCount)
			if err != nil {
				return err
			}
			for _, identity := range pullIdentities {
				occurrences[identity]++
			}
		}
		for index, token := range candidates {
			if executable, ok := fugueExecutableIdentity(token, false); ok && !commandExecutionField(field) {
				executableOccurrences[executable]++
			}
			embeddedImageField := index > 0 && imageIdentityField(candidates[index-1])
			if identity, managed := managedIdentityCandidate(token, identityField, identityField || embeddedImageField, managedRepositories, managedDigests, managedNamespaces); managed {
				occurrences[identity]++
			}
		}
	}
	return nil
}

func boundedImageCandidates(value string) ([]string, error) {
	result := make([]string, 0)
	for index := 0; index < len(value); {
		start := index
		if value[index] == '/' {
			if index+1 >= len(value) || !imageCandidateStart(value[index+1]) {
				index++
				continue
			}
			index++
		} else if !imageCandidateStart(value[index]) {
			index++
			continue
		}
		for index < len(value) && imageCandidateCharacter(value[index]) {
			index++
		}
		if index-start > maxImageCandidateBytes {
			return nil, fmt.Errorf("manifest contains an oversized image-like scalar token")
		}
		result = append(result, value[start:index])
		if len(result) > maxImageCandidatesPerScalar {
			return nil, fmt.Errorf("manifest scalar contains too many image-like tokens")
		}
	}
	return result, nil
}

func imageCandidateStart(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '_'
}

func imageCandidateCharacter(value byte) bool {
	return imageCandidateStart(value) || value == '.' || value == ':' || value == '/' || value == '@' || value == '-'
}

func managedIdentityCandidate(value string, allowBareManaged, allowBareFugue bool, managedRepositories, managedDigests, managedNamespaces map[string]struct{}) (string, bool) {
	if managedIdentityToken(value, allowBareManaged, allowBareFugue, managedRepositories, managedDigests, managedNamespaces) {
		return value, true
	}
	return "", false
}

func managedIdentityToken(value string, allowBareManaged, allowBareFugue bool, managedRepositories, managedDigests, managedNamespaces map[string]struct{}) bool {
	if _, managed := managedDigests[value]; managed {
		return true
	}
	if marker := strings.LastIndex(value, "@"); marker >= 0 && marker+1 < len(value) {
		if _, managed := managedDigests[value[marker+1:]]; managed {
			return true
		}
	}
	if repository, err := repositoryBeforeDigestForDetection(value); err == nil {
		if repositoryWithinManagedBoundary(repository, managedRepositories, managedNamespaces) || repositoryLooksLikeFugue(repository) {
			return true
		}
	}
	if parsed, err := parseImageRefForDetection(value); err == nil {
		_, managedDigest := managedDigests[parsed.Digest]
		qualified := strings.Contains(parsed.Repository, "/")
		strongImageIdentity := parsed.Digest != "" || (parsed.Tag != "" && !endpointPortTag(parsed.Tag))
		managedRepository := repositoryWithinManagedBoundary(parsed.Repository, managedRepositories, managedNamespaces) && (qualified || allowBareManaged || strongImageIdentity)
		fugueRepository := repositoryLooksLikeFugue(parsed.Repository) && (qualified || allowBareFugue || strongImageIdentity)
		return managedDigest || managedRepository || fugueRepository
	}
	if validateRepository(value) != nil {
		return false
	}
	if repositoryWithinManagedBoundary(value, managedRepositories, managedNamespaces) && (strings.Contains(value, "/") || allowBareManaged) {
		return true
	}
	return (strings.Contains(value, "/") || allowBareFugue) && repositoryLooksLikeFugue(value)
}

func imageIdentityField(field string) bool {
	field = normalizedImageField(field)
	if imageReferenceValueField(field) || imageRepositoryValueField(field) {
		return true
	}
	switch field {
	case "digest", "image_digest", "imagedigest", "images", "ref", "reference":
		return true
	}
	if strings.HasSuffix(field, "_image_digest") || strings.HasSuffix(field, "imagedigest") || strings.HasSuffix(field, "_images") || strings.HasSuffix(field, "imageref") {
		return true
	}
	if !strings.Contains(field, "image") {
		return false
	}
	for _, suffix := range []string{"addr", "address", "base", "endpoint", "host", "path", "port", "socket", "uri", "url"} {
		if strings.HasSuffix(field, suffix) {
			return false
		}
	}
	return true
}

func imageReferenceValueField(field string) bool {
	field = normalizedImageField(field)
	return field == "image" || field == "image_ref" || field == "imageref" || strings.HasSuffix(field, "_image") || strings.HasSuffix(field, "image") || strings.HasSuffix(field, "imageref")
}

func imageRepositoryValueField(field string) bool {
	field = normalizedImageField(field)
	return field == "repository" || field == "repo" || field == "imagerepository" || strings.HasSuffix(field, "_image_repository") || strings.HasSuffix(field, "imagerepository")
}

func pullImageContextField(field string) bool {
	field = normalizedImageField(field)
	switch field {
	case "args", "command", "command_argument", "command_entrypoint", "entrypoint", "run", "script":
		return true
	default:
		return false
	}
}

func commandExecutionField(field string) bool {
	field = normalizedImageField(field)
	switch field {
	case "command", "command_entrypoint", "entrypoint", "run", "script":
		return true
	default:
		return false
	}
}

func commandArrayContexts(values []any, field string) (map[int]struct{}, int) {
	entrypoints := make(map[int]struct{})
	field = normalizedImageField(field)
	if field != "command" && field != "entrypoint" || len(values) == 0 {
		return entrypoints, -1
	}
	first, ok := values[0].(string)
	if !ok {
		return entrypoints, -1
	}
	entrypoints[0] = struct{}{}
	firstBase := path.Base(path.Clean(strings.TrimSpace(first)))
	if firstBase == "env" || firstBase == "nohup" || firstBase == "command" || firstBase == "exec" {
		for index := 1; index < len(values); index++ {
			argument, ok := values[index].(string)
			if !ok || strings.HasPrefix(argument, "-") || strings.Contains(argument, "=") {
				continue
			}
			entrypoints[index] = struct{}{}
			break
		}
	}
	if firstBase == "bash" || firstBase == "dash" || firstBase == "sh" || firstBase == "zsh" {
		for index := 1; index+1 < len(values); index++ {
			option, ok := values[index].(string)
			if ok && strings.HasPrefix(option, "-") && strings.Contains(option, "c") {
				return entrypoints, index + 1
			}
		}
	}
	return entrypoints, -1
}

func siblingCommandArgumentExecutables(value map[string]any) ([]string, error) {
	result := make([]string, 0)
	for _, commandKey := range []string{"command", "entrypoint"} {
		executables, err := commandArgumentExecutables(value, commandKey)
		if err != nil {
			return nil, err
		}
		result = append(result, executables...)
	}
	return result, nil
}

func commandArgumentExecutables(value map[string]any, commandKey string) ([]string, error) {
	command, commandOK := value[commandKey].([]any)
	arguments, argumentsOK := value["args"].([]any)
	if !argumentsOK {
		arguments = nil
	}
	if !commandOK || len(command) == 0 {
		return nil, nil
	}
	entrypoints, scriptIndex := commandArrayContexts(command, commandKey)
	argv := make([]commandArgvWord, 0, len(command)+len(arguments))
	for index, raw := range command {
		word, ok := raw.(string)
		if !ok {
			return nil, nil
		}
		argv = append(argv, commandArgvWord{value: word, commandIndex: index})
	}
	for _, raw := range arguments {
		word, ok := raw.(string)
		if !ok {
			return nil, nil
		}
		argv = append(argv, commandArgvWord{value: word, fromArgs: true, commandIndex: -1})
	}

	index := 0
	for index < len(argv) {
		word := strings.TrimSpace(argv[index].value)
		base := path.Base(path.Clean(word))
		if shellExecutableName(base) {
			for option := index + 1; option < len(argv); option++ {
				if strings.HasPrefix(argv[option].value, "-") && strings.Contains(argv[option].value, "c") && option+1 < len(argv) {
					if !argv[option+1].fromArgs && argv[option+1].commandIndex == scriptIndex {
						return nil, nil
					}
					return commandContextFugueExecutables(argv[option+1].value, "script")
				}
			}
			return nil, nil
		}
		if executable, ok := fugueExecutableIdentity(word, true); ok {
			if !argv[index].fromArgs {
				if _, alreadyClassified := entrypoints[argv[index].commandIndex]; alreadyClassified {
					return nil, nil
				}
			}
			return []string{executable}, nil
		}

		switch base {
		case "env":
			for option := index + 1; option < len(argv); option++ {
				value := argv[option].value
				if value == "-S" || value == "--split-string" {
					if option+1 < len(argv) {
						return commandContextFugueExecutables(argv[option+1].value, "script")
					}
					return nil, nil
				}
				if strings.HasPrefix(value, "--split-string=") {
					return commandContextFugueExecutables(strings.TrimPrefix(value, "--split-string="), "script")
				}
				if strings.HasPrefix(value, "-S") && len(value) > 2 {
					return commandContextFugueExecutables(value[2:], "script")
				}
				if !strings.HasPrefix(value, "-") {
					break
				}
			}
			index = skipArgvOptions(argv, index+1, map[string]struct{}{"-C": {}, "--chdir": {}, "-S": {}, "--split-string": {}, "-u": {}, "--unset": {}})
			for index < len(argv) && strings.Contains(argv[index].value, "=") {
				index++
			}
		case "command", "nohup", "setsid":
			index = skipArgvOptions(argv, index+1, nil)
		case "busybox", "toybox":
			index = skipArgvOptions(argv, index+1, nil)
		case "exec":
			index = skipArgvOptions(argv, index+1, map[string]struct{}{"-a": {}})
		case "tini":
			index = skipArgvOptions(argv, index+1, map[string]struct{}{"-e": {}, "-p": {}})
		case "dumb-init":
			index = skipArgvOptions(argv, index+1, map[string]struct{}{"--rewrite": {}})
		case "timeout":
			index = skipArgvOptions(argv, index+1, map[string]struct{}{"-k": {}, "--kill-after": {}, "-s": {}, "--signal": {}})
			if index < len(argv) {
				index++
			}
		case "nice":
			index = skipArgvOptions(argv, index+1, map[string]struct{}{"-n": {}, "--adjustment": {}})
		case "chroot":
			index = skipArgvOptions(argv, index+1, map[string]struct{}{"--groups": {}, "--userspec": {}})
			if index < len(argv) {
				index++
			}
		case "sudo":
			index = skipArgvOptions(argv, index+1, map[string]struct{}{"-C": {}, "-D": {}, "-g": {}, "-h": {}, "-p": {}, "-R": {}, "-T": {}, "-u": {}, "--chdir": {}, "--group": {}, "--host": {}, "--prompt": {}, "--role": {}, "--type": {}, "--user": {}})
		case "time":
			index = skipArgvOptions(argv, index+1, map[string]struct{}{"-f": {}, "-o": {}, "--format": {}, "--output": {}})
		default:
			return nil, nil
		}
	}
	return nil, nil
}

func shellExecutableName(value string) bool {
	switch value {
	case "ash", "bash", "dash", "ksh", "sh", "zsh":
		return true
	default:
		return false
	}
}

func skipArgvOptions(argv []commandArgvWord, index int, optionsWithValue map[string]struct{}) int {
	for index < len(argv) {
		value := argv[index].value
		if value == "--" {
			return index + 1
		}
		if !strings.HasPrefix(value, "-") || value == "-" {
			return index
		}
		option := value
		if before, _, found := strings.Cut(value, "="); found {
			option = before
		}
		_, consumesValue := optionsWithValue[option]
		index++
		if consumesValue && !strings.Contains(value, "=") && index < len(argv) {
			index++
		}
	}
	return index
}

func siblingCommandArgumentImagePulls(value map[string]any, managedRepositories, managedDigests, managedNamespaces map[string]struct{}) ([]string, error) {
	result := make([]string, 0)
	for _, commandKey := range []string{"command", "entrypoint"} {
		identities, err := commandArgumentImagePulls(value, commandKey, managedRepositories, managedDigests, managedNamespaces)
		if err != nil {
			return nil, err
		}
		result = append(result, identities...)
	}
	return result, nil
}

func commandArgumentImagePulls(value map[string]any, commandKey string, managedRepositories, managedDigests, managedNamespaces map[string]struct{}) ([]string, error) {
	command, commandOK := value[commandKey].([]any)
	arguments, argumentsOK := value["args"].([]any)
	if !commandOK || !argumentsOK || len(command) == 0 || len(arguments) == 0 {
		return nil, nil
	}
	seenPull := false
	candidateCount := 0
	for _, raw := range command {
		text, ok := raw.(string)
		if !ok {
			continue
		}
		candidates, err := boundedImageCandidates(text)
		if err != nil {
			return nil, err
		}
		if len(candidates) > maxImageCandidatesPerScalar-candidateCount {
			return nil, fmt.Errorf("manifest command argv contains too many candidate tokens")
		}
		candidateCount += len(candidates)
		for _, candidate := range candidates {
			if candidate == "pull" {
				seenPull = true
			}
		}
	}
	if !seenPull {
		return nil, nil
	}
	result := make([]string, 0)
	for _, raw := range arguments {
		text, ok := raw.(string)
		if !ok {
			continue
		}
		candidates, err := boundedImageCandidates(text)
		if err != nil {
			return nil, err
		}
		if len(candidates) > maxImageCandidatesPerScalar-candidateCount {
			return nil, fmt.Errorf("manifest command argv contains too many candidate tokens")
		}
		candidateCount += len(candidates)
		for _, candidate := range candidates {
			if candidate == "pull" {
				return result, nil
			}
			if identity, managed := managedIdentityCandidate(candidate, true, true, managedRepositories, managedDigests, managedNamespaces); managed {
				result = append(result, identity)
			}
		}
	}
	return result, nil
}

func commandContextFugueExecutables(value, field string) ([]string, error) {
	if !commandExecutionField(field) {
		return nil, nil
	}
	candidates := 0
	return scanShellContextFugueExecutables(value, 0, &candidates)
}

func shellContextManagedImagePulls(value string, managedRepositories, managedDigests, managedNamespaces map[string]struct{}, globalCandidateCount *int) ([]string, error) {
	localCandidateCount := 0
	return scanShellContextManagedImagePulls(value, 0, &localCandidateCount, globalCandidateCount, managedRepositories, managedDigests, managedNamespaces)
}

func scanShellContextManagedImagePulls(value string, depth int, localCandidateCount, globalCandidateCount *int, managedRepositories, managedDigests, managedNamespaces map[string]struct{}) ([]string, error) {
	if depth > maxShellParseDepth {
		return nil, fmt.Errorf("manifest shell image pull nesting exceeds limit")
	}
	if localCandidateCount == nil || globalCandidateCount == nil {
		return nil, fmt.Errorf("manifest shell image pull scan budget is required")
	}
	value = normalizeShellLineContinuations(value)
	result := make([]string, 0)
	for start := 0; start <= len(value); {
		end := start
		for end < len(value) && !shellCommandSeparator(value[end]) {
			end++
		}
		identities, err := shellSegmentManagedImagePulls(value[start:end], depth, localCandidateCount, globalCandidateCount, managedRepositories, managedDigests, managedNamespaces)
		if err != nil {
			return nil, err
		}
		result = append(result, identities...)
		if end == len(value) {
			break
		}
		start = end + 1
	}
	return result, nil
}

func shellSegmentManagedImagePulls(segment string, depth int, localCandidateCount, globalCandidateCount *int, managedRepositories, managedDigests, managedNamespaces map[string]struct{}) ([]string, error) {
	result := make([]string, 0)
	seenPull := false
	for offset := 0; offset < len(segment); {
		for offset < len(segment) && (segment[offset] == ' ' || segment[offset] == '\t') {
			offset++
		}
		if offset >= len(segment) {
			break
		}
		word, next, err := staticShellWord(segment, offset)
		if err != nil {
			return nil, err
		}
		offset = next
		if strings.ContainsAny(word, " \t") {
			nested, err := scanShellContextManagedImagePulls(word, depth+1, localCandidateCount, globalCandidateCount, managedRepositories, managedDigests, managedNamespaces)
			if err != nil {
				return nil, err
			}
			result = append(result, nested...)
			continue
		}
		word = shellWordBeforeRedirection(word)
		if word == "" {
			continue
		}
		candidates, err := boundedImageCandidates(word)
		if err != nil {
			return nil, err
		}
		if *localCandidateCount < 0 || *localCandidateCount > maxImageCandidatesPerScalar || len(candidates) > maxImageCandidatesPerScalar-*localCandidateCount {
			return nil, fmt.Errorf("manifest shell image pull contains too many candidate tokens")
		}
		if *globalCandidateCount < 0 || *globalCandidateCount > maxImageCandidates || len(candidates) > maxImageCandidates-*globalCandidateCount {
			return nil, fmt.Errorf("Helm manifests exceed managed image scan candidate limit")
		}
		*localCandidateCount += len(candidates)
		*globalCandidateCount += len(candidates)
		for _, candidate := range candidates {
			if seenPull {
				if identity, managed := managedIdentityCandidate(candidate, true, true, managedRepositories, managedDigests, managedNamespaces); managed {
					result = append(result, identity)
				}
			}
			if candidate == "pull" {
				seenPull = true
			}
		}
	}
	return result, nil
}

func scanShellContextFugueExecutables(value string, depth int, candidateCount *int) ([]string, error) {
	if depth > maxShellParseDepth {
		return nil, fmt.Errorf("manifest shell command nesting exceeds limit")
	}
	if candidateCount == nil {
		return nil, fmt.Errorf("manifest shell command scan budget is required")
	}
	value = normalizeShellLineContinuations(value)
	result := make([]string, 0)
	for start := 0; start <= len(value); {
		end := start
		for end < len(value) && !shellCommandSeparator(value[end]) {
			end++
		}
		executables, err := shellSegmentFugueExecutables(value[start:end], depth, candidateCount)
		if err != nil {
			return nil, err
		}
		result = append(result, executables...)
		if end == len(value) {
			break
		}
		start = end + 1
	}
	return result, nil
}

func normalizeShellLineContinuations(value string) string {
	if !strings.Contains(value, "\\\n") && !strings.Contains(value, "\\\r\n") {
		return value
	}
	value = strings.ReplaceAll(value, "\\\r\n", "")
	return strings.ReplaceAll(value, "\\\n", "")
}

func shellCommandSeparator(value byte) bool {
	switch value {
	case '\n', '\r', ';', '|', '&', '(', ')', '`':
		return true
	default:
		return false
	}
}

func shellSegmentFugueExecutables(segment string, depth int, candidateCount *int) ([]string, error) {
	result := make([]string, 0)
	for offset := 0; offset < len(segment); {
		for offset < len(segment) && (segment[offset] == ' ' || segment[offset] == '\t') {
			offset++
		}
		if offset >= len(segment) {
			break
		}
		word, next, err := staticShellWord(segment, offset)
		if err != nil {
			return nil, err
		}
		offset = next
		if strings.ContainsAny(word, " \t") {
			nested, err := scanShellContextFugueExecutables(word, depth+1, candidateCount)
			if err != nil {
				return nil, err
			}
			result = append(result, nested...)
			continue
		}
		word = shellWordBeforeRedirection(word)
		if word == "" {
			continue
		}
		candidates, err := boundedImageCandidates(word)
		if err != nil {
			return nil, err
		}
		if *candidateCount < 0 || *candidateCount > maxImageCandidatesPerScalar || len(candidates) > maxImageCandidatesPerScalar-*candidateCount {
			return nil, fmt.Errorf("manifest shell command contains too many candidate tokens")
		}
		*candidateCount += len(candidates)
		for _, candidate := range candidates {
			if executable, ok := fugueExecutableIdentity(candidate, true); ok {
				result = append(result, executable)
			}
		}
	}
	return result, nil
}

func shellWordBeforeRedirection(word string) string {
	index := strings.IndexAny(word, "<>")
	if index < 0 {
		return word
	}
	prefix := word[:index]
	if prefix == "" {
		return ""
	}
	for _, character := range prefix {
		if character < '0' || character > '9' {
			return prefix
		}
	}
	return ""
}

func staticShellWord(segment string, offset int) (string, int, error) {
	var word strings.Builder
	quote := byte(0)
	for offset < len(segment) {
		current := segment[offset]
		if quote == 0 && (current == ' ' || current == '\t') {
			break
		}
		switch current {
		case '\'', '"':
			if quote == 0 {
				quote = current
				offset++
				continue
			}
			if quote == current {
				quote = 0
				offset++
				continue
			}
		case '\\':
			if quote != '\'' && offset+1 < len(segment) {
				next := segment[offset+1]
				if quote == 0 || next == '$' || next == '`' || next == '"' || next == '\\' {
					if word.Len() >= maxImageCandidateBytes {
						return "", 0, fmt.Errorf("manifest contains an oversized shell command word")
					}
					word.WriteByte(next)
					offset += 2
					continue
				}
			}
		}
		if word.Len() >= maxImageCandidateBytes {
			return "", 0, fmt.Errorf("manifest contains an oversized shell command word")
		}
		word.WriteByte(current)
		offset++
	}
	return word.String(), offset, nil
}

func fugueExecutableIdentity(value string, allowBare bool) (string, bool) {
	const prefix = "/usr/local/bin/fugue-"
	if strings.HasSuffix(value, "/") || strings.Contains(value, "://") || strings.HasPrefix(value, "http:/") || strings.HasPrefix(value, "https:/") {
		return "", false
	}
	cleaned := path.Clean(value)
	if strings.HasPrefix(cleaned, prefix) {
		return cleaned, true
	}
	if allowBare && strings.HasPrefix(path.Base(cleaned), "fugue-") {
		return cleaned, true
	}
	return "", false
}

func endpointPortTag(value string) bool {
	if !registryPort.MatchString(value) {
		return false
	}
	port, err := strconv.Atoi(value)
	return err == nil && port <= 65535
}

func normalizedImageField(field string) string {
	if len(field) > maxYAMLKeyBytes {
		return ""
	}
	return strings.ToLower(strings.ReplaceAll(field, "-", "_"))
}

func equalStringCounts(left, right map[string]int) bool {
	if len(left) != len(right) {
		return false
	}
	for value, count := range left {
		if right[value] != count {
			return false
		}
	}
	return true
}

func isFugueConsumerContainerName(name string) bool {
	switch name {
	case "api", "controller", "registry-gc", "registry-janitor", "node-janitor", "topology-labeler", "image-prepull", "telemetry-agent", "image-cache", "edge", "edge-front", "ssh-front", "mesh-recovery", "app-ssh":
		return true
	default:
		return false
	}
}

func consumerProjectionKey(component, helmPath string, workload Workload) string {
	return strings.Join([]string{component, helmPath, workload.Kind, workload.Name, workload.Container}, "\x00")
}

func activationMatchesRenderedRef(activation Activation, rendered string) bool {
	renderedRef, err := parseImageRef(rendered)
	if err != nil || renderedRef.Repository != activation.Repository {
		return false
	}
	if activation.PinState == "pinned" {
		return rendered == activation.Repository+"@"+activation.Digest && renderedRef.Tag == "" && renderedRef.Digest == activation.Digest
	}
	return rendered == activation.Repository+":"+activation.SourceTag && renderedRef.Tag == activation.SourceTag && renderedRef.Digest == ""
}

func semanticDNSContainer(object manifestObject) (manifestContainer, error) {
	matches := make([]manifestContainer, 0, 1)
	for _, container := range object.Containers {
		if container.Init || container.Ephemeral || len(container.Command) != 1 || container.Command[0] != "/usr/local/bin/fugue-dns" {
			continue
		}
		zoneCount := 0
		validZone := true
		for _, item := range container.Environment {
			if name, _ := item["name"].(string); name == "FUGUE_DNS_ZONE" {
				zoneCount++
				value, ok := item["value"].(string)
				validZone = validZone && ok && value != "" && len(item) == 2
			}
		}
		if zoneCount == 1 && validZone {
			matches = append(matches, container)
		}
	}
	if len(matches) != 1 {
		return manifestContainer{}, fmt.Errorf("expected exactly one fugue-dns command container with one literal FUGUE_DNS_ZONE, found %d", len(matches))
	}
	return matches[0], nil
}

func readDrainCarrier(object manifestObject) (string, string, error) {
	container, err := exactContainer(object, "controller")
	if err != nil {
		return "", "", fmt.Errorf("drain-agent carrier: %w", err)
	}
	names := []string{
		"FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY",
		"FUGUE_DRAIN_AGENT_IMAGE_TAG",
		"FUGUE_DRAIN_AGENT_IMAGE_DIGEST",
		"FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY",
	}
	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		wanted[name] = struct{}{}
	}
	found := make(map[string]string, len(names))
	for index, item := range container.Environment {
		name, _ := item["name"].(string)
		if _, relevant := wanted[name]; !relevant {
			continue
		}
		if len(item) != 2 {
			return "", "", fmt.Errorf("drain-agent carrier env %s must contain only literal name and value", name)
		}
		value, ok := item["value"].(string)
		if !ok {
			return "", "", fmt.Errorf("drain-agent carrier env %s must use a literal string value", name)
		}
		if _, duplicate := found[name]; duplicate {
			return "", "", fmt.Errorf("drain-agent carrier env %s is duplicated at index %d", name, index)
		}
		found[name] = value
	}
	for _, name := range names {
		if _, exists := found[name]; !exists {
			return "", "", fmt.Errorf("drain-agent carrier env %s is missing", name)
		}
	}
	repository := found[names[0]]
	tag := found[names[1]]
	digest := found[names[2]]
	pullPolicy := found[names[3]]
	if err := validateRepository(repository); err != nil || !tagPattern.MatchString(tag) {
		return "", "", fmt.Errorf("drain-agent carrier image repository or tag is invalid")
	}
	if digest != "" && !digestPattern.MatchString(digest) {
		return "", "", fmt.Errorf("drain-agent carrier digest is invalid")
	}
	if pullPolicy != "Always" && pullPolicy != "IfNotPresent" && pullPolicy != "Never" {
		return "", "", fmt.Errorf("drain-agent carrier pull policy is invalid")
	}
	selected := repository + ":" + tag
	if digest != "" {
		selected = repository + "@" + digest
	}
	return selected, pullPolicy, nil
}

func decodeAndValidateLock(data []byte) (Lock, error) {
	if err := scanStrictJSON(data); err != nil {
		return Lock{}, err
	}
	var document map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil {
		return Lock{}, fmt.Errorf("decode lock document failed")
	}
	if err := validateLockDocumentShape(document); err != nil {
		return Lock{}, err
	}
	var lock Lock
	if err := decodeJSONExact(data, &lock); err != nil {
		return Lock{}, err
	}
	if lock.SchemaVersion != 1 || lock.Producer != lockProducer {
		return Lock{}, fmt.Errorf("unsupported lock schema or producer")
	}
	if !digestPattern.MatchString(lock.LockDigest) {
		return Lock{}, fmt.Errorf("lock_digest must be a lowercase sha256 digest")
	}
	delete(document, "lock_digest")
	canonical, err := canonicalJSON(document)
	if err != nil {
		return Lock{}, err
	}
	if got := digestBytes(canonical); got != lock.LockDigest {
		return Lock{}, fmt.Errorf("lock digest mismatch: got %s", got)
	}
	document["lock_digest"] = lock.LockDigest
	canonicalLock, err := canonicalJSON(document)
	if err != nil {
		return Lock{}, err
	}
	canonicalLock = append(canonicalLock, '\n')
	if !bytes.Equal(data, canonicalLock) {
		return Lock{}, fmt.Errorf("release image lock is not canonical builder JSON")
	}
	if lock.Release.Workflow != releaseWorkflow || lock.Release.Platform != releasePlatform || !gitRevisionPattern.MatchString(lock.Release.HeadSHA) {
		return Lock{}, fmt.Errorf("lock release identity is invalid")
	}
	if !positiveInteger.MatchString(lock.Release.RunID) || !positiveInteger.MatchString(lock.Release.RunAttempt) || !validReleaseRepository(lock.Release.Repository) {
		return Lock{}, fmt.Errorf("lock release run identity is invalid")
	}
	if len(lock.Artifacts) > 64 || len(lock.Activations) == 0 || len(lock.Activations) > 4096 {
		return Lock{}, fmt.Errorf("lock artifact or activation count is outside limits")
	}

	artifactSeen := make(map[string]struct{})
	artifactsByComponent := make(map[string]Artifact)
	for index, artifact := range lock.Artifacts {
		if index > 0 && lock.Artifacts[index-1].Component >= artifact.Component {
			return Lock{}, fmt.Errorf("lock artifacts are not in builder canonical order")
		}
		if _, ok := artifactComponents[artifact.Component]; !ok {
			return Lock{}, fmt.Errorf("artifacts[%d] has unsupported component", index)
		}
		if _, duplicate := artifactSeen[artifact.Component]; duplicate {
			return Lock{}, fmt.Errorf("duplicate artifact component %s", artifact.Component)
		}
		artifactSeen[artifact.Component] = struct{}{}
		artifactsByComponent[artifact.Component] = artifact
		if err := validateRepository(artifact.Repository); err != nil {
			return Lock{}, fmt.Errorf("artifacts[%d].repository: %w", index, err)
		}
		for field, value := range map[string]string{"top_digest": artifact.TopDigest, "platform_manifest_digest": artifact.PlatformManifestDigest, "config_digest": artifact.ConfigDigest} {
			if !digestPattern.MatchString(value) {
				return Lock{}, fmt.Errorf("artifacts[%d].%s is invalid", index, field)
			}
		}
		if artifact.SourceTag != lock.Release.HeadSHA || artifact.OCIRevision != lock.Release.HeadSHA || artifact.ImmutableRef != artifact.Repository+"@"+artifact.TopDigest || artifact.Verification != registryVerifyMode {
			return Lock{}, fmt.Errorf("artifacts[%d] identity is inconsistent", index)
		}
	}

	bindingSeen := make(map[string]struct{})
	builtComponents := make(map[string]struct{})
	helmPathIdentities := make(map[string]string)
	previousActivationKey := ""
	for index, activation := range lock.Activations {
		activationKey := strings.Join([]string{activation.Component, activation.HelmPath, activation.Workload.Kind, activation.Workload.Name, activation.Workload.Container, activation.SourceMode, activation.SelectedRef}, "\x00")
		if index > 0 && previousActivationKey >= activationKey {
			return Lock{}, fmt.Errorf("lock activations are not in builder canonical order")
		}
		previousActivationKey = activationKey
		if _, ok := activationComponents[activation.Component]; !ok {
			return Lock{}, fmt.Errorf("activations[%d] has unsupported component", index)
		}
		if activation.SourceMode != "built" && activation.SourceMode != "migration" && activation.SourceMode != "preserve" {
			return Lock{}, fmt.Errorf("activations[%d] has unsupported source_mode", index)
		}
		if activation.PinState != "pinned" && activation.PinState != "legacy_unpinned" {
			return Lock{}, fmt.Errorf("activations[%d] has unsupported pin_state", index)
		}
		if err := validateRepository(activation.Repository); err != nil {
			return Lock{}, fmt.Errorf("activations[%d].repository: %w", index, err)
		}
		selected, err := parseImageRef(activation.SelectedRef)
		if err != nil {
			return Lock{}, fmt.Errorf("activations[%d].selected_ref: %w", index, err)
		}
		if !tagPattern.MatchString(activation.SourceTag) || selected.Repository != activation.Repository || (selected.Tag != "" && selected.Tag != activation.SourceTag) || selected.Digest != activation.Digest {
			return Lock{}, fmt.Errorf("activations[%d] selected_ref identity is inconsistent", index)
		}
		sourceTemplate, err := parseImageRef(activation.SourceTemplateRef)
		if err != nil {
			return Lock{}, fmt.Errorf("activations[%d].source_template_ref: %w", index, err)
		}
		if sourceTemplate.Repository != activation.Repository || (sourceTemplate.Tag != "" && sourceTemplate.Tag != activation.SourceTag) {
			return Lock{}, fmt.Errorf("activations[%d] source_template_ref identity is inconsistent", index)
		}
		if activation.PinState == "pinned" {
			if !digestPattern.MatchString(activation.Digest) || !digestPattern.MatchString(activation.RuntimeManifestDigest) {
				return Lock{}, fmt.Errorf("activations[%d] pinned identity is incomplete", index)
			}
			canonicalDigestRef := activation.Repository + "@" + activation.Digest
			taggedDigestRef := activation.Repository + ":" + activation.SourceTag + "@" + activation.Digest
			if activation.SelectedRef != canonicalDigestRef && activation.SelectedRef != taggedDigestRef {
				return Lock{}, fmt.Errorf("activations[%d] pinned selected_ref must be repository[@tag]@digest", index)
			}
		} else if activation.SourceMode != "preserve" || !activation.MigrationAllowed || activation.Digest != "" || activation.RuntimeManifestDigest != "" || selected.Tag == "" {
			return Lock{}, fmt.Errorf("activations[%d] legacy_unpinned identity is invalid", index)
		} else if activation.SelectedRef != activation.Repository+":"+activation.SourceTag {
			return Lock{}, fmt.Errorf("activations[%d] legacy selected_ref must be canonical repository:tag", index)
		}
		switch activation.SourceMode {
		case "built":
			artifact, exists := artifactsByComponent[activation.Component]
			if !exists || activation.MigrationAllowed || activation.PinState != "pinned" || activation.SourceTag != lock.Release.HeadSHA || activation.Digest != artifact.TopDigest || activation.RuntimeManifestDigest != artifact.PlatformManifestDigest || activation.SelectedRef != artifact.ImmutableRef || (sourceTemplate.Digest != "" && sourceTemplate.Digest != artifact.TopDigest) {
				return Lock{}, fmt.Errorf("activations[%d] built identity does not match its artifact", index)
			}
			builtComponents[activation.Component] = struct{}{}
		case "preserve":
			sourceTag := sourceTemplate.Tag
			if sourceTag == "" {
				sourceTag = activation.SourceTag
			}
			selectedTag := selected.Tag
			if selectedTag == "" {
				selectedTag = activation.SourceTag
			}
			if activation.MigrationAllowed != (activation.PinState == "legacy_unpinned") || sourceTemplate.Repository != selected.Repository || sourceTag != selectedTag || sourceTemplate.Digest != selected.Digest {
				return Lock{}, fmt.Errorf("activations[%d] preserve identity changes its source reference", index)
			}
		case "migration":
			if !activation.MigrationAllowed || activation.PinState != "pinned" || sourceTemplate.Digest != "" || sourceTemplate.Tag != activation.SourceTag || selected.Digest == "" {
				return Lock{}, fmt.Errorf("activations[%d] migration identity is invalid", index)
			}
		}
		if err := validateActivationBinding(activation); err != nil {
			return Lock{}, fmt.Errorf("activations[%d] workload binding is invalid", index)
		}
		helmIdentity := strings.Join([]string{
			activation.Component, activation.SourceMode, activation.SourceTemplateRef,
			activation.SelectedRef, activation.Repository, activation.SourceTag,
			activation.Digest, activation.RuntimeManifestDigest, activation.PinState,
			strconv.FormatBool(activation.MigrationAllowed),
		}, "\x00")
		if previous, exists := helmPathIdentities[activation.HelmPath]; exists && previous != helmIdentity {
			return Lock{}, fmt.Errorf("one Helm image path selects multiple image identities")
		}
		helmPathIdentities[activation.HelmPath] = helmIdentity
		binding := strings.Join([]string{activation.Workload.Kind, activation.Workload.Name, activation.Workload.Container}, "\x00")
		if _, duplicate := bindingSeen[binding]; duplicate {
			return Lock{}, fmt.Errorf("duplicate activation workload binding")
		}
		bindingSeen[binding] = struct{}{}
	}
	for component := range artifactsByComponent {
		if _, built := builtComponents[component]; !built && component != "app_ssh" {
			return Lock{}, fmt.Errorf("artifact component %s has no built activation", component)
		}
	}
	return lock, nil
}

func validateActivationBinding(activation Activation) error {
	if len(activation.HelmPath) > 255 || !helmPathPattern.MatchString(activation.HelmPath) || !validWorkload(activation.Workload) {
		return fmt.Errorf("unsafe Helm path or workload")
	}
	w := activation.Workload
	switch activation.Component {
	case "api":
		if activation.HelmPath == "api.image" && w.Kind == "Deployment" && w.Container == "api" {
			return nil
		}
	case "controller":
		switch activation.HelmPath {
		case "controller.image":
			if (w.Kind == "Deployment" && w.Container == "controller") || (w.Kind == "CronJob" && (w.Container == "registry-gc" || w.Container == "registry-janitor")) {
				return nil
			}
		case "imagePrePull.image":
			if w.Kind == "DaemonSet" && w.Container == "image-prepull" {
				return nil
			}
		case "nodeJanitor.image":
			if w.Kind == "DaemonSet" && w.Container == "node-janitor" {
				return nil
			}
		case "topologyLabeler.image":
			if w.Kind == "DaemonSet" && w.Container == "topology-labeler" {
				return nil
			}
		}
	case "drain_agent":
		if activation.HelmPath == "runtime.strictDrain.agent.image" && w.Kind == "Configuration" && w.Container == "controller" {
			return nil
		}
	case "image_cache":
		if activation.HelmPath == "imageCache.image" && w.Kind == "DaemonSet" && w.Container == "image-cache" {
			return nil
		}
	case "telemetry_agent":
		if activation.HelmPath == "observability.agent.image" && w.Kind == "Deployment" && w.Container == "telemetry-agent" {
			return nil
		}
	case "edge":
		if w.Kind != "DaemonSet" {
			break
		}
		switch {
		case edgeWorkerPath.MatchString(activation.HelmPath) && w.Container == "edge":
			return nil
		case edgeFrontPath.MatchString(activation.HelmPath) && w.Container == "edge-front":
			return nil
		case edgeSSHPath.MatchString(activation.HelmPath) && w.Container == "ssh-front":
			return nil
		case edgeDNSPath.MatchString(activation.HelmPath) && hasDNSNameToken(w.Name):
			return nil
		case activation.HelmPath == "meshRecovery.image" && w.Container == "mesh-recovery":
			return nil
		}
	}
	return fmt.Errorf("component/path/kind/container shape mismatch")
}

func hasDNSNameToken(value string) bool {
	for _, token := range strings.Split(value, "-") {
		if token == "dns" {
			return true
		}
	}
	return false
}

func validateLockDocumentShape(document map[string]any) error {
	if err := requireExactObjectFields(document, "lock", "schema_version", "producer", "release", "artifacts", "activations", "lock_digest"); err != nil {
		return err
	}
	if schema, ok := document["schema_version"].(json.Number); !ok || schema.String() != "1" {
		return fmt.Errorf("lock.schema_version must be the integer 1")
	}
	for _, field := range []string{"producer", "lock_digest"} {
		if _, ok := document[field].(string); !ok {
			return fmt.Errorf("lock.%s must be a string", field)
		}
	}
	release, ok := document["release"].(map[string]any)
	if !ok {
		return fmt.Errorf("lock.release must be an object")
	}
	if err := requireExactObjectFields(release, "lock.release", "repository", "workflow", "run_id", "run_attempt", "head_sha", "platform"); err != nil {
		return err
	}
	if err := requireStringObjectFields(release, "lock.release", "repository", "workflow", "run_id", "run_attempt", "head_sha", "platform"); err != nil {
		return err
	}
	artifacts, ok := document["artifacts"].([]any)
	if !ok {
		return fmt.Errorf("lock.artifacts must be an array")
	}
	for index, raw := range artifacts {
		artifact, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("lock.artifacts[%d] must be an object", index)
		}
		location := fmt.Sprintf("lock.artifacts[%d]", index)
		fields := []string{"component", "repository", "source_tag", "top_digest", "platform_manifest_digest", "config_digest", "oci_revision", "immutable_ref", "verification"}
		if err := requireExactObjectFields(artifact, location, fields...); err != nil {
			return err
		}
		if err := requireStringObjectFields(artifact, location, fields...); err != nil {
			return err
		}
	}
	activations, ok := document["activations"].([]any)
	if !ok {
		return fmt.Errorf("lock.activations must be an array")
	}
	for index, raw := range activations {
		activation, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("lock.activations[%d] must be an object", index)
		}
		location := fmt.Sprintf("lock.activations[%d]", index)
		fields := []string{"component", "source_mode", "source_template_ref", "selected_ref", "repository", "source_tag", "digest", "runtime_manifest_digest", "pin_state", "migration_allowed", "helm_path", "workload"}
		if err := requireExactObjectFields(activation, location, fields...); err != nil {
			return err
		}
		if err := requireStringObjectFields(activation, location, "component", "source_mode", "source_template_ref", "selected_ref", "repository", "source_tag", "digest", "runtime_manifest_digest", "pin_state", "helm_path"); err != nil {
			return err
		}
		if _, ok := activation["migration_allowed"].(bool); !ok {
			return fmt.Errorf("%s.migration_allowed must be a boolean", location)
		}
		workload, ok := activation["workload"].(map[string]any)
		if !ok {
			return fmt.Errorf("%s.workload must be an object", location)
		}
		if err := requireExactObjectFields(workload, location+".workload", "kind", "name", "container"); err != nil {
			return err
		}
		if err := requireStringObjectFields(workload, location+".workload", "kind", "name", "container"); err != nil {
			return err
		}
	}
	return nil
}

func requireExactObjectFields(value map[string]any, location string, fields ...string) error {
	expected := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		expected[field] = struct{}{}
	}
	for field := range value {
		if _, ok := expected[field]; !ok {
			return fmt.Errorf("%s has an unknown field", location)
		}
	}
	for _, field := range fields {
		if _, ok := value[field]; !ok {
			return fmt.Errorf("%s is missing field %q", location, field)
		}
	}
	return nil
}

func requireStringObjectFields(value map[string]any, location string, fields ...string) error {
	for _, field := range fields {
		if _, ok := value[field].(string); !ok {
			return fmt.Errorf("%s.%s must be a string", location, field)
		}
	}
	return nil
}

func validReleaseRepository(value string) bool {
	parts := strings.Split(value, "/")
	return len(parts) == 2 && releaseOwner.MatchString(parts[0]) && releaseRepository.MatchString(parts[1]) && parts[1] != "." && parts[1] != ".."
}

func decodeHelmRelease(data []byte) (helmRelease, error) {
	if err := scanStrictJSON(data); err != nil {
		return helmRelease{}, fmt.Errorf("Helm release JSON: %w", err)
	}
	var root map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return helmRelease{}, fmt.Errorf("decode Helm release JSON failed")
	}
	if root == nil {
		return helmRelease{}, fmt.Errorf("Helm release JSON root must be an object")
	}
	allowed := map[string]struct{}{
		"name": {}, "info": {}, "chart": {}, "config": {}, "manifest": {},
		"hooks": {}, "version": {}, "namespace": {}, "apply_method": {},
	}
	for key := range root {
		if _, ok := allowed[key]; !ok {
			return helmRelease{}, fmt.Errorf("Helm release JSON has an unsupported root field")
		}
	}
	for _, required := range []string{"name", "info", "chart", "manifest", "hooks", "version", "namespace"} {
		if _, ok := root[required]; !ok {
			return helmRelease{}, fmt.Errorf("Helm release JSON is missing %s", required)
		}
	}
	applyMethod := ""
	if method, exists := root["apply_method"]; exists {
		methodString, ok := method.(string)
		if !ok || (methodString != "csa" && methodString != "ssa") {
			return helmRelease{}, fmt.Errorf("Helm release apply_method is invalid")
		}
		applyMethod = methodString
	}
	result := helmRelease{ApplyMethod: applyMethod, Groups: map[string][]string{"edge": {}, "dns": {}}}
	var ok bool
	result.Name, ok = root["name"].(string)
	if !ok || !validDNSLabel(result.Name) {
		return helmRelease{}, fmt.Errorf("Helm release JSON name must be a lowercase DNS label")
	}
	result.Namespace, ok = root["namespace"].(string)
	if !ok || !validDNSLabel(result.Namespace) {
		return helmRelease{}, fmt.Errorf("Helm release JSON namespace must be a lowercase DNS label")
	}
	result.Manifest, ok = root["manifest"].(string)
	if !ok || result.Manifest == "" {
		return helmRelease{}, fmt.Errorf("Helm release JSON name and manifest must not be empty")
	}
	version, ok := root["version"].(json.Number)
	if !ok {
		return helmRelease{}, fmt.Errorf("Helm release version must be an integer")
	}
	version64, err := strconv.ParseInt(version.String(), 10, 32)
	if err != nil || version64 < 1 {
		return helmRelease{}, fmt.Errorf("Helm release version must be a positive integer")
	}
	result.Version = int(version64)
	info, ok := root["info"].(map[string]any)
	if !ok {
		return helmRelease{}, fmt.Errorf("Helm release info must be an object")
	}
	result.Status, ok = info["status"].(string)
	if !ok {
		return helmRelease{}, fmt.Errorf("Helm release info.status must be a string")
	}
	result.Description, ok = info["description"].(string)
	if !ok {
		return helmRelease{}, fmt.Errorf("Helm release info.description must be a string")
	}
	firstDeployed, firstOK := info["first_deployed"].(string)
	lastDeployed, lastOK := info["last_deployed"].(string)
	if !firstOK || !lastOK {
		return helmRelease{}, fmt.Errorf("Helm release deployment timestamps must be strings")
	}
	result.FirstDeployed, err = time.Parse(time.RFC3339Nano, firstDeployed)
	if err != nil || result.FirstDeployed.IsZero() {
		return helmRelease{}, fmt.Errorf("Helm release first deployment timestamp is invalid")
	}
	result.LastDeployed, err = time.Parse(time.RFC3339Nano, lastDeployed)
	if err != nil || result.LastDeployed.IsZero() || result.LastDeployed.Before(result.FirstDeployed) {
		return helmRelease{}, fmt.Errorf("Helm release last deployment timestamp is invalid")
	}

	hooks, ok := root["hooks"].([]any)
	if !ok || len(hooks) > 128 {
		return helmRelease{}, fmt.Errorf("Helm release hooks must be a bounded array")
	}
	totalHookBytes := 0
	for index, rawHook := range hooks {
		hook, ok := rawHook.(map[string]any)
		if !ok {
			return helmRelease{}, fmt.Errorf("Helm hook %d must be an object", index)
		}
		manifest, ok := hook["manifest"].(string)
		if !ok || manifest == "" {
			return helmRelease{}, fmt.Errorf("Helm hook %d manifest must be a non-empty string", index)
		}
		totalHookBytes += len([]byte(manifest))
		if totalHookBytes > maxManifestBytes {
			return helmRelease{}, fmt.Errorf("Helm hook manifests exceed %d bytes", maxManifestBytes)
		}
		result.HookManifests = append(result.HookManifests, manifest)
	}

	chart, ok := root["chart"].(map[string]any)
	if !ok {
		return helmRelease{}, fmt.Errorf("Helm release chart must be an object")
	}
	metadata, ok := chart["metadata"].(map[string]any)
	if !ok {
		return helmRelease{}, fmt.Errorf("Helm release chart.metadata must be an object")
	}
	chartName, ok := metadata["name"].(string)
	if !ok || !validDNSLabel(chartName) {
		return helmRelease{}, fmt.Errorf("Helm release chart name is invalid")
	}
	defaults, ok := chart["values"].(map[string]any)
	if !ok {
		return helmRelease{}, fmt.Errorf("Helm release chart.values must be an object")
	}
	config := map[string]any{}
	if rawConfig, exists := root["config"]; exists && rawConfig != nil {
		config, ok = rawConfig.(map[string]any)
		if !ok {
			return helmRelease{}, fmt.Errorf("Helm release config must be an object")
		}
	}
	nameOverride, err := effectiveString(defaults, config, "nameOverride")
	if err != nil {
		return helmRelease{}, err
	}
	fullnameOverride, err := effectiveString(defaults, config, "fullnameOverride")
	if err != nil {
		return helmRelease{}, err
	}
	effectiveChartName := chartName
	if nameOverride != "" {
		effectiveChartName = nameOverride
	}
	computed := result.Name + "-" + effectiveChartName
	if fullnameOverride != "" {
		computed = fullnameOverride
	}
	computed, err = sprigTruncateAndTrim(computed, 63)
	if err != nil || !validDNSLabel(computed) {
		return helmRelease{}, fmt.Errorf("computed Helm chart fullname is invalid")
	}
	result.ComputedFullname = computed
	for _, family := range []string{"edge", "dns"} {
		names, err := effectiveGroupNames(defaults, config, family)
		if err != nil {
			return helmRelease{}, err
		}
		result.Groups[family] = names
	}
	result.Topology, err = effectiveReleaseTopology(defaults, config)
	if err != nil {
		return helmRelease{}, err
	}
	result.PrePullImages, err = effectiveImagePrePullImages(defaults, config)
	if err != nil {
		return helmRelease{}, err
	}
	return result, nil
}

func effectiveImagePrePullImages(defaults, config map[string]any) ([]string, error) {
	value, exists, err := effectiveValue(defaults, config, "imagePrePull", "images")
	if err != nil {
		return nil, err
	}
	if !exists {
		return []string{}, nil
	}
	items, ok := value.([]any)
	if !ok || len(items) > 4096 {
		return nil, fmt.Errorf("Helm value imagePrePull.images must be a bounded array")
	}
	result := make([]string, 0, len(items))
	for index, item := range items {
		image, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("Helm value imagePrePull.images[%d] must be a string", index)
		}
		if _, err := parseImageRef(image); err != nil {
			return nil, fmt.Errorf("Helm value imagePrePull.images[%d] is not a strict image reference", index)
		}
		result = append(result, image)
	}
	return result, nil
}

func effectiveReleaseTopology(defaults, config map[string]any) (releaseTopology, error) {
	boolValue := func(fallback bool, path ...string) (bool, error) {
		value, exists, err := effectiveValue(defaults, config, path...)
		if err != nil {
			return false, err
		}
		if !exists {
			return fallback, nil
		}
		result, ok := value.(bool)
		if !ok {
			return false, fmt.Errorf("Helm value %s must be a boolean", strings.Join(path, "."))
		}
		return result, nil
	}
	registryEnabled, err := boolValue(false, "registry", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	registryGCEnabled, err := boolValue(false, "registryGC", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	registryJanitorEnabled, err := boolValue(false, "registryJanitor", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	nodeJanitor, err := boolValue(true, "nodeJanitor", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	topologyLabeler, err := boolValue(true, "topologyLabeler", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	imagePrePullEnabled, err := boolValue(false, "imagePrePull", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	images, imagesExist, err := effectiveValue(defaults, config, "imagePrePull", "images")
	if err != nil {
		return releaseTopology{}, err
	}
	imageCount := 0
	if imagesExist {
		imageList, ok := images.([]any)
		if !ok || len(imageList) > 4096 {
			return releaseTopology{}, fmt.Errorf("Helm value imagePrePull.images must be a bounded array")
		}
		imageCount = len(imageList)
	}
	agentEnabled, err := boolValue(false, "observability", "agent", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	imageCache, err := boolValue(false, "imageCache", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	edgeEnabled, err := boolValue(false, "edge", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	blueGreen, err := boolValue(false, "edge", "blueGreen", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	keepLegacy, err := boolValue(false, "edge", "blueGreen", "migration", "keepLegacyDirect")
	if err != nil {
		return releaseTopology{}, err
	}
	dynamicEnabled, err := boolValue(false, "edge", "dynamic", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	dynamicBlueGreen, err := boolValue(true, "edge", "dynamic", "blueGreen", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	sshFront, err := boolValue(true, "edge", "sshFront", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	dns, err := boolValue(false, "dns", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	meshRecovery, err := boolValue(false, "meshRecovery", "enabled")
	if err != nil {
		return releaseTopology{}, err
	}
	return releaseTopology{
		RegistryGC:      registryEnabled && registryGCEnabled,
		RegistryJanitor: registryEnabled && registryJanitorEnabled,
		NodeJanitor:     nodeJanitor,
		TopologyLabeler: topologyLabeler,
		ImagePrePull:    imagePrePullEnabled && imageCount > 0,
		TelemetryAgent:  agentEnabled,
		ImageCache:      imageCache,
		EdgeDirect:      edgeEnabled && (!blueGreen || keepLegacy),
		EdgeBlueGreen:   edgeEnabled && blueGreen,
		EdgeDynamic:     edgeEnabled && blueGreen && dynamicEnabled && dynamicBlueGreen,
		EdgeSSHFront:    edgeEnabled && sshFront,
		DNS:             dns,
		MeshRecovery:    meshRecovery,
	}, nil
}

func effectiveValue(defaults, config map[string]any, path ...string) (any, bool, error) {
	if value, exists, err := valueAtPath(config, path...); err != nil {
		return nil, false, err
	} else if exists {
		return value, true, nil
	}
	return valueAtPath(defaults, path...)
}

func valueAtPath(root map[string]any, path ...string) (any, bool, error) {
	current := root
	for index, segment := range path {
		value, exists := current[segment]
		if !exists {
			return nil, false, nil
		}
		if index == len(path)-1 {
			if value == nil {
				return nil, false, fmt.Errorf("Helm value %s must not be null", strings.Join(path, "."))
			}
			return value, true, nil
		}
		next, ok := value.(map[string]any)
		if !ok {
			return nil, false, fmt.Errorf("Helm value %s must be an object", strings.Join(path[:index+1], "."))
		}
		current = next
	}
	return nil, false, nil
}

func effectiveString(defaults, config map[string]any, key string) (string, error) {
	value, exists := config[key]
	if !exists {
		value = defaults[key]
	}
	if value == nil {
		return "", nil
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("Helm value %s must be a string", key)
	}
	return text, nil
}

func effectiveGroupNames(defaults, config map[string]any, family string) ([]string, error) {
	familyDefaults := map[string]any{}
	if rawDefaults, exists := defaults[family]; exists && rawDefaults != nil {
		var ok bool
		familyDefaults, ok = rawDefaults.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Helm chart default %s must be an object", family)
		}
	}
	familyConfig := map[string]any{}
	if rawConfig, exists := config[family]; exists && rawConfig != nil {
		var ok bool
		familyConfig, ok = rawConfig.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Helm config %s must be an object", family)
		}
	}
	value, exists := familyConfig["groups"]
	if !exists {
		value = familyDefaults["groups"]
	}
	if value == nil {
		return nil, nil
	}
	groups, ok := value.([]any)
	if !ok || len(groups) > 128 {
		return nil, fmt.Errorf("Helm value %s.groups must be a bounded array", family)
	}
	result := make([]string, 0, len(groups))
	seen := make(map[string]int)
	for index, rawGroup := range groups {
		group, ok := rawGroup.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Helm value %s.groups[%d] must be an object", family, index)
		}
		name, ok := group["name"].(string)
		if !ok || name == "" {
			return nil, fmt.Errorf("Helm value %s.groups[%d].name must be a non-empty string", family, index)
		}
		normalized, err := normalizeGroupName(name)
		if err != nil {
			return nil, fmt.Errorf("Helm value %s.groups[%d].name: %w", family, index, err)
		}
		if family == "edge" && (normalized == "dynamic" || normalized == "ssh") {
			return nil, fmt.Errorf("Helm value edge.groups[%d] normalizes to reserved name %q", index, normalized)
		}
		if previous, duplicate := seen[normalized]; duplicate {
			return nil, fmt.Errorf("Helm values %s.groups[%d] and [%d] normalize to the same name", family, previous, index)
		}
		seen[normalized] = index
		result = append(result, normalized)
	}
	return result, nil
}

func normalizeGroupName(value string) (string, error) {
	value = strings.ReplaceAll(strings.ToLower(value), "_", "-")
	return sprigTruncateAndTrim(value, 30)
}

func sprigTruncateAndTrim(value string, maximum int) (string, error) {
	encoded := []byte(value)
	if len(encoded) > maximum {
		encoded = encoded[:maximum]
	}
	if !utf8.Valid(encoded) {
		return "", fmt.Errorf("byte truncation splits a UTF-8 sequence")
	}
	result := strings.TrimSuffix(string(encoded), "-")
	if result == "" || !validDNSLabel(result) {
		return "", fmt.Errorf("normalized value %q is not a DNS label", result)
	}
	return result, nil
}

type yamlBudget struct {
	Documents        int
	Nodes            int
	ExpandedObjects  int
	ParsedContainers int
}

func decodeManifest(data []byte, defaultNamespace string) ([]manifestObject, error) {
	return decodeManifestWithBudget(data, defaultNamespace, &yamlBudget{})
}

func decodeManifestWithBudget(data []byte, defaultNamespace string, budget *yamlBudget) ([]manifestObject, error) {
	if budget == nil {
		return nil, fmt.Errorf("YAML budget is required")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	objects := make([]manifestObject, 0)
	for document := 1; ; document++ {
		var node yaml.Node
		err := decoder.Decode(&node)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Helm manifest document %d YAML parse failed", document)
		}
		budget.Documents++
		if budget.Documents > maxManifestDocs {
			return nil, fmt.Errorf("Helm manifests exceed %d combined YAML documents", maxManifestDocs)
		}
		if implicitEmptyDocument(&node) {
			continue
		}
		value, err := strictYAMLValue(&node, "$", 0, &budget.Nodes)
		if err != nil {
			return nil, fmt.Errorf("Helm manifest document %d is unsafe: %w", document, err)
		}
		root, ok := value.(map[string]any)
		if !ok || len(root) == 0 {
			return nil, fmt.Errorf("Helm manifest document %d root must be a non-empty object", document)
		}
		expanded, err := expandManifestObject(root, defaultNamespace, budget, 0)
		if err != nil {
			return nil, fmt.Errorf("Helm manifest document %d: %w", document, err)
		}
		objects = append(objects, expanded...)
	}
	return objects, nil
}

func expandManifestObject(root map[string]any, defaultNamespace string, budget *yamlBudget, listDepth int) ([]manifestObject, error) {
	if budget == nil {
		return nil, fmt.Errorf("YAML budget is required")
	}
	kind, ok := root["kind"].(string)
	if !ok || !kubeKindPattern.MatchString(kind) {
		return nil, fmt.Errorf("kind must be a bounded Kubernetes kind")
	}
	apiVersion, ok := root["apiVersion"].(string)
	if !ok || !validKubeAPIVersion(apiVersion) {
		return nil, fmt.Errorf("apiVersion must be a bounded Kubernetes API version")
	}
	if kind == "Secret" {
		return nil, fmt.Errorf("Secret appeared in dry-run manifest; --hide-secret is required")
	}
	if _, hasItems := root["items"]; hasItems && kind != "List" {
		return nil, fmt.Errorf("non-List Kubernetes object contains a top-level items field")
	}
	if kind == "List" {
		if listDepth >= maxManifestListDepth {
			return nil, fmt.Errorf("nested Kubernetes Lists exceed the depth limit")
		}
		if apiVersion != "v1" {
			return nil, fmt.Errorf("List apiVersion must be v1")
		}
		items, ok := root["items"].([]any)
		if !ok || len(items) > maxManifestDocs {
			return nil, fmt.Errorf("List items must be a bounded array")
		}
		result := make([]manifestObject, 0, len(items))
		for index, item := range items {
			itemMap, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("List item %d must be an object", index)
			}
			expanded, err := expandManifestObject(itemMap, defaultNamespace, budget, listDepth+1)
			if err != nil {
				return nil, fmt.Errorf("List item %d: %w", index, err)
			}
			result = append(result, expanded...)
		}
		return result, nil
	}
	budget.ExpandedObjects++
	if budget.ExpandedObjects > maxExpandedObjects {
		return nil, fmt.Errorf("Helm manifests exceed the expanded Kubernetes object limit")
	}
	metadata, ok := root["metadata"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("metadata must be an object")
	}
	name, ok := metadata["name"].(string)
	if !ok || !validDNSSubdomain(name) {
		return nil, fmt.Errorf("metadata.name must be a lowercase DNS subdomain")
	}
	namespace := defaultNamespace
	if rawNamespace, exists := metadata["namespace"]; exists {
		var ok bool
		namespace, ok = rawNamespace.(string)
		if !ok || !validDNSLabel(namespace) {
			return nil, fmt.Errorf("metadata.namespace must be a lowercase DNS label")
		}
	}
	object := manifestObject{APIVersion: apiVersion, Kind: kind, Namespace: namespace, Name: name, Raw: root}
	containers, err := workloadContainers(root, kind, budget)
	if err != nil {
		return nil, fmt.Errorf("%s/%s: %w", kind, name, err)
	}
	object.Containers = containers
	return []manifestObject{object}, nil
}

func workloadContainers(root map[string]any, kind string, budget *yamlBudget) ([]manifestContainer, error) {
	var podSpec any
	spec, _ := root["spec"].(map[string]any)
	switch kind {
	case "Deployment", "DaemonSet", "StatefulSet", "ReplicaSet", "ReplicationController", "Job":
		if spec == nil {
			return nil, fmt.Errorf("spec must be an object")
		}
		template, ok := spec["template"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("spec.template must be an object")
		}
		podSpec = template["spec"]
	case "CronJob":
		if spec == nil {
			return nil, fmt.Errorf("spec must be an object")
		}
		jobTemplate, ok := spec["jobTemplate"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("spec.jobTemplate must be an object")
		}
		jobSpec, ok := jobTemplate["spec"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("spec.jobTemplate.spec must be an object")
		}
		template, ok := jobSpec["template"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("spec.jobTemplate.spec.template must be an object")
		}
		podSpec = template["spec"]
	case "Pod":
		podSpec = root["spec"]
	default:
		return recursiveManifestContainers(root, "$", 0, budget)
	}
	podSpecMap, ok := podSpec.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("pod spec must be an object")
	}
	if _, exists := podSpecMap["ephemeralContainers"]; exists && kind != "Pod" {
		return nil, fmt.Errorf("pod templates must not contain ephemeralContainers")
	}
	result := make([]manifestContainer, 0)
	seen := make(map[string]struct{})
	groups := []struct {
		field     string
		init      bool
		ephemeral bool
	}{{"containers", false, false}, {"initContainers", true, false}}
	if kind == "Pod" {
		groups = append(groups, struct {
			field     string
			init      bool
			ephemeral bool
		}{"ephemeralContainers", false, true})
	}
	for _, group := range groups {
		raw, exists := podSpecMap[group.field]
		if !exists {
			if group.field == "containers" {
				return nil, fmt.Errorf("pod spec containers must be a non-empty array")
			}
			continue
		}
		containers, err := parseManifestContainerArray(raw, group.field, group.init, group.ephemeral, group.field == "containers", budget)
		if err != nil {
			return nil, err
		}
		for _, container := range containers {
			identity := strconv.FormatBool(group.init) + "\x00" + strconv.FormatBool(group.ephemeral) + "\x00" + container.Name
			if _, duplicate := seen[identity]; duplicate {
				return nil, fmt.Errorf("%s contains duplicate container name %s", group.field, container.Name)
			}
			seen[identity] = struct{}{}
			result = append(result, container)
		}
	}
	return result, nil
}

func parseManifestContainerArray(raw any, location string, init, ephemeral, required bool, budget *yamlBudget) ([]manifestContainer, error) {
	if budget == nil {
		return nil, fmt.Errorf("YAML budget is required")
	}
	items, ok := raw.([]any)
	if !ok || (required && len(items) == 0) {
		return nil, fmt.Errorf("%s must be a non-empty array when required", location)
	}
	if budget.ParsedContainers < 0 || budget.ParsedContainers > maxManifestContainers || len(items) > maxManifestContainers-budget.ParsedContainers {
		return nil, fmt.Errorf("Helm manifests exceed the parsed container limit")
	}
	budget.ParsedContainers += len(items)
	result := make([]manifestContainer, 0, len(items))
	seen := make(map[string]struct{})
	for index, item := range items {
		containerMap, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s[%d] must be an object", location, index)
		}
		name, nameOK := containerMap["name"].(string)
		image, imageOK := containerMap["image"].(string)
		if !nameOK || !validDNSLabel(name) || !imageOK || image == "" {
			return nil, fmt.Errorf("%s[%d] must have a DNS-label name and non-empty image", location, index)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("%s contains duplicate container name %s", location, name)
		}
		seen[name] = struct{}{}
		command, err := stringArray(containerMap["command"], location+" command", true)
		if err != nil {
			return nil, err
		}
		arguments, err := stringArray(containerMap["args"], location+" args", true)
		if err != nil {
			return nil, err
		}
		environment, err := objectArray(containerMap["env"], location+" env", true)
		if err != nil {
			return nil, err
		}
		result = append(result, manifestContainer{Name: name, Image: image, Init: init, Ephemeral: ephemeral, Command: command, Arguments: arguments, Environment: environment})
	}
	return result, nil
}

func recursiveManifestContainers(value any, path string, depth int, budget *yamlBudget) ([]manifestContainer, error) {
	result := make([]manifestContainer, 0)
	if err := appendRecursiveManifestContainers(value, path, depth, budget, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func appendRecursiveManifestContainers(value any, path string, depth int, budget *yamlBudget, result *[]manifestContainer) error {
	if depth > maxYAMLDepth {
		return fmt.Errorf("container scan exceeds maximum depth")
	}
	switch current := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(current))
		for key := range current {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for ordinal, key := range keys {
			child := current[key]
			if key == "spec" {
				if spec, ok := child.(map[string]any); ok {
					for _, field := range []string{"containers", "initContainers", "ephemeralContainers"} {
						raw, exists := spec[field]
						if !exists {
							continue
						}
						containers, err := parseManifestContainerArray(raw, path+"/spec/"+field, field == "initContainers", field == "ephemeralContainers", field == "containers", budget)
						if err != nil {
							return err
						}
						*result = append(*result, containers...)
					}
				}
			}
			if err := appendRecursiveManifestContainers(child, path+"/mapping-value-"+strconv.Itoa(ordinal), depth+1, budget, result); err != nil {
				return err
			}
		}
	case []any:
		for index, child := range current {
			if err := appendRecursiveManifestContainers(child, path+"/"+strconv.Itoa(index), depth+1, budget, result); err != nil {
				return err
			}
		}
	}
	return nil
}

func exactContainer(object manifestObject, name string) (manifestContainer, error) {
	matches := make([]manifestContainer, 0, 1)
	for _, container := range object.Containers {
		if !container.Init && !container.Ephemeral && container.Name == name {
			matches = append(matches, container)
		}
	}
	if len(matches) != 1 {
		return manifestContainer{}, fmt.Errorf("expected exactly one regular container named %s, found %d", name, len(matches))
	}
	return matches[0], nil
}

func verifyDrainCarrier(object manifestObject, activation Activation) (string, error) {
	container, err := exactContainer(object, activation.Workload.Container)
	if err != nil {
		return "", fmt.Errorf("drain-agent carrier: %w", err)
	}
	expected := map[string]string{
		"FUGUE_DRAIN_AGENT_IMAGE_REPOSITORY": activation.Repository,
		"FUGUE_DRAIN_AGENT_IMAGE_TAG":        activation.SourceTag,
		"FUGUE_DRAIN_AGENT_IMAGE_DIGEST":     activation.Digest,
	}
	found := make(map[string]string)
	pullPolicy := ""
	for index, item := range container.Environment {
		name, _ := item["name"].(string)
		if _, relevant := expected[name]; !relevant && name != "FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY" {
			continue
		}
		if len(item) != 2 {
			return "", fmt.Errorf("drain-agent carrier env %s must contain only literal name and value", name)
		}
		value, ok := item["value"].(string)
		if !ok {
			return "", fmt.Errorf("drain-agent carrier env %s must use a literal string value", name)
		}
		if _, duplicate := found[name]; duplicate {
			return "", fmt.Errorf("drain-agent carrier env %s is duplicated (at index %d)", name, index)
		}
		found[name] = value
		if name == "FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY" {
			pullPolicy = value
		}
	}
	for name, value := range expected {
		if found[name] != value {
			return "", fmt.Errorf("drain-agent carrier env %s does not match the lock", name)
		}
	}
	if found["FUGUE_DRAIN_AGENT_IMAGE_PULL_POLICY"] != pullPolicy || (pullPolicy != "Always" && pullPolicy != "IfNotPresent" && pullPolicy != "Never") {
		return "", fmt.Errorf("drain-agent carrier pull policy must be one literal Kubernetes pull policy")
	}
	return pullPolicy, nil
}

func verifyDNSContainer(container manifestContainer, activation Activation) error {
	if len(container.Command) != 1 || container.Command[0] != "/usr/local/bin/fugue-dns" {
		return fmt.Errorf("DNS activation %s container %s does not have the exact fugue-dns command", activation.Workload.Name, container.Name)
	}
	matches := 0
	for _, item := range container.Environment {
		name, _ := item["name"].(string)
		if name != "FUGUE_DNS_ZONE" {
			continue
		}
		matches++
		value, ok := item["value"].(string)
		if !ok || value == "" || len(item) != 2 {
			return fmt.Errorf("DNS activation %s FUGUE_DNS_ZONE must be one non-empty literal value", activation.Workload.Name)
		}
	}
	if matches != 1 {
		return fmt.Errorf("DNS activation %s must contain exactly one FUGUE_DNS_ZONE", activation.Workload.Name)
	}
	return nil
}

func expectedFugueExecutables(container manifestContainer, activation Activation) ([]string, error) {
	result := make([]string, 0)
	var authoritativeScript string
	switch activation.Workload.Container {
	case "registry-gc", "registry-janitor", "node-janitor", "topology-labeler", "image-prepull":
		if len(container.Command) != 3 || container.Command[0] != "/bin/bash" || container.Command[1] != "-lc" || strings.TrimSpace(container.Command[2]) == "" || len(container.Arguments) != 0 {
			return nil, fmt.Errorf("classified Fugue script consumer does not use its authoritative shell carrier")
		}
		authoritativeScript = container.Command[2]
	}
	if !strings.HasPrefix(activation.HelmPath, "dns.") {
		switch activation.Workload.Container {
		case "api", "controller", "edge", "image-cache", "telemetry-agent":
			if len(container.Command) != 0 || len(container.Arguments) != 0 {
				return nil, fmt.Errorf("classified Fugue consumer overrides its authoritative image entrypoint")
			}
		}
	}
	expected := ""
	if strings.HasPrefix(activation.HelmPath, "dns.") {
		expected = "/usr/local/bin/fugue-dns"
	} else {
		switch activation.Workload.Container {
		case "edge-front":
			expected = "/usr/local/bin/fugue-edge-front"
		case "mesh-recovery":
			expected = "/usr/local/bin/fugue-mesh-recovery"
		case "ssh-front":
			expected = "/usr/local/bin/fugue-ssh-front"
		}
	}
	if expected != "" {
		if len(container.Command) != 1 || container.Command[0] != expected || len(container.Arguments) != 0 {
			return nil, fmt.Errorf("classified Fugue consumer is missing its authoritative executable")
		}
		result = append(result, expected)
	}
	if activation.Workload.Container == "registry-gc" {
		registryName := strings.TrimSuffix(activation.Workload.Name, "-gc")
		prefix := firstNonEmptyShellLines(authoritativeScript, 7)
		wantPrefix := []string{
			"set -euo pipefail",
			`namespace="${POD_NAMESPACE}"`,
			`registry_deploy="` + registryName + `"`,
			`gc_lease="` + activation.Workload.Name + `"`,
			`original_replicas=""`,
			`gc_started="false"`,
			`keep_file="/tmp/registry-workload-digests"`,
		}
		if !equalStrings(prefix, wantPrefix) {
			return nil, fmt.Errorf("registry GC authoritative executable context changed")
		}
		if countExactShellLine(container, `registry_deploy="`+registryName+`"`) != 1 ||
			countExactShellLine(container, `gc_lease="`+activation.Workload.Name+`"`) != 1 ||
			countExactShellLine(container, "fugue-registry-maintenance active-imports --format count") != 1 ||
			countExactShellLine(container, "fugue-registry-maintenance scan \\") != 1 {
			return nil, fmt.Errorf("registry GC authoritative executable context changed")
		}
		expectedCounts := map[string]int{
			"fugue-registry-maintenance": 2,
			activation.Workload.Name:     1,
			registryName:                 1,
		}
		observedCounts := make(map[string]int)
		for _, value := range append(append([]string{}, container.Command...), container.Arguments...) {
			candidates, err := boundedImageCandidates(value)
			if err != nil {
				return nil, err
			}
			for _, candidate := range candidates {
				if _, expected := expectedCounts[candidate]; expected {
					observedCounts[candidate]++
				}
			}
		}
		if !equalStringCounts(observedCounts, expectedCounts) {
			return nil, fmt.Errorf("registry GC authoritative executable context changed")
		}
		for executable, count := range expectedCounts {
			for index := 0; index < count; index++ {
				result = append(result, executable)
			}
		}
	}
	if activation.Workload.Container == "registry-janitor" {
		gcName := strings.TrimSuffix(activation.Workload.Name, "-janitor") + "-gc"
		registryName := strings.TrimSuffix(activation.Workload.Name, "-janitor")
		prefix := firstNonEmptyShellLines(authoritativeScript, 4)
		if len(prefix) != 4 || prefix[0] != "set -euo pipefail" || !validRegistryBaseAssignment(prefix[1], registryName) || prefix[2] != `gc_lease="`+gcName+`"` || prefix[3] != "log() {" {
			return nil, fmt.Errorf("registry janitor authoritative executable context changed")
		}
		if countExactShellLine(container, `gc_lease="`+gcName+`"`) != 1 {
			return nil, fmt.Errorf("registry janitor authoritative executable context changed")
		}
		expectedCounts := map[string]int{gcName: 1}
		observedCounts := make(map[string]int)
		for _, value := range append(append([]string{}, container.Command...), container.Arguments...) {
			candidates, err := boundedImageCandidates(value)
			if err != nil {
				return nil, err
			}
			for _, candidate := range candidates {
				if _, expected := expectedCounts[candidate]; expected {
					observedCounts[candidate]++
				}
			}
		}
		if !equalStringCounts(observedCounts, expectedCounts) {
			return nil, fmt.Errorf("registry janitor authoritative executable context changed")
		}
		result = append(result, gcName)
	}
	return result, nil
}

func firstNonEmptyShellLines(value string, limit int) []string {
	if limit <= 0 {
		return nil
	}
	result := make([]string, 0, limit)
	for start := 0; start <= len(value) && len(result) < limit; {
		end := len(value)
		if relative := strings.IndexByte(value[start:], '\n'); relative >= 0 {
			end = start + relative
		}
		if line := strings.TrimSpace(value[start:end]); line != "" {
			result = append(result, line)
		}
		if end == len(value) {
			break
		}
		start = end + 1
	}
	return result
}

func validRegistryBaseAssignment(value, registryName string) bool {
	prefix := `registry_base="http://` + registryName + `:`
	if !strings.HasPrefix(value, prefix) || !strings.HasSuffix(value, `"`) {
		return false
	}
	port := strings.TrimSuffix(strings.TrimPrefix(value, prefix), `"`)
	return endpointPortTag(port)
}

func countExactShellLine(container manifestContainer, expected string) int {
	count := 0
	for _, value := range append(append([]string{}, container.Command...), container.Arguments...) {
		heredocs := make([]shellHeredoc, 0)
		quote := byte(0)
		continued := false
		heredocCount := 0
		lineCount := 0
		for start := 0; start <= len(value); {
			lineCount++
			if lineCount > maxShellLines {
				return -1
			}
			end := len(value)
			if relative := strings.IndexByte(value[start:], '\n'); relative >= 0 {
				end = start + relative
			}
			line := value[start:end]
			trimmed := strings.TrimSpace(line)
			if len(heredocs) > 0 {
				candidate := line
				if heredocs[0].StripTabs {
					candidate = strings.TrimLeft(candidate, "\t")
				}
				if candidate == heredocs[0].Delimiter {
					heredocs = heredocs[1:]
				}
				if end == len(value) {
					start = len(value) + 1
				} else {
					start = end + 1
				}
				continue
			}
			lineStartsInCode := quote == 0 && !continued
			if lineStartsInCode && trimmed == expected {
				count++
			}
			if quote == 0 {
				found, ok := shellHeredocDelimiters(line)
				if !ok || len(found) > maxShellHeredocs-heredocCount {
					return -1
				}
				heredocCount += len(found)
				heredocs = append(heredocs, found...)
			}
			quote, continued = shellLineState(line, quote)
			if len(heredocs) > 0 {
				if continued {
					return -1
				}
				quote = 0
				continued = false
			}
			if end == len(value) {
				break
			}
			start = end + 1
		}
		if len(heredocs) > 0 || quote != 0 || continued {
			return -1
		}
	}
	return count
}

type shellHeredoc struct {
	Delimiter string
	StripTabs bool
}

func shellLineState(line string, quote byte) (byte, bool) {
	escaped := false
	for index := 0; index < len(line); index++ {
		character := line[index]
		if quote == '\'' {
			if character == '\'' {
				quote = 0
			}
			continue
		}
		if escaped {
			escaped = false
			continue
		}
		if character == '\\' {
			escaped = true
			continue
		}
		if quote == '"' {
			if character == '"' {
				quote = 0
			}
			continue
		}
		if character == '\'' || character == '"' {
			quote = character
			continue
		}
		if character == '#' && (index == 0 || line[index-1] == ' ' || line[index-1] == '\t' || strings.ContainsRune(";|&()", rune(line[index-1]))) {
			return quote, false
		}
	}
	return quote, escaped
}

func shellHeredocDelimiters(line string) ([]shellHeredoc, bool) {
	result := make([]shellHeredoc, 0)
	quote := byte(0)
	escaped := false
	for offset := 0; offset < len(line); offset++ {
		character := line[offset]
		if quote == '\'' {
			if character == '\'' {
				quote = 0
			}
			continue
		}
		if escaped {
			escaped = false
			continue
		}
		if character == '\\' {
			escaped = true
			continue
		}
		if quote == '"' {
			if character == '"' {
				quote = 0
			}
			continue
		}
		if character == '\'' || character == '"' {
			quote = character
			continue
		}
		if character == '#' && (offset == 0 || line[offset-1] == ' ' || line[offset-1] == '\t' || strings.ContainsRune(";|&()", rune(line[offset-1]))) {
			break
		}
		if character != '<' || offset+1 >= len(line) || line[offset+1] != '<' {
			continue
		}
		if offset+2 < len(line) && line[offset+2] == '<' {
			offset += 2
			continue
		}
		offset += 2
		stripTabs := false
		if offset < len(line) && line[offset] == '-' {
			stripTabs = true
			offset++
		}
		for offset < len(line) && (line[offset] == ' ' || line[offset] == '\t') {
			offset++
		}
		if offset >= len(line) {
			return nil, false
		}
		delimiterQuote := byte(0)
		if line[offset] == '\'' || line[offset] == '"' {
			delimiterQuote = line[offset]
			offset++
		}
		start := offset
		for offset < len(line) {
			current := line[offset]
			if delimiterQuote != 0 {
				if current == delimiterQuote {
					break
				}
			} else if current == ' ' || current == '\t' || strings.ContainsRune(";|&()<>", rune(current)) {
				break
			}
			if !(current >= 'a' && current <= 'z' || current >= 'A' && current <= 'Z' || current >= '0' && current <= '9' || current == '_' || current == '.' || current == '-') {
				return nil, false
			}
			offset++
		}
		if start == offset || delimiterQuote != 0 && (offset >= len(line) || line[offset] != delimiterQuote) {
			return nil, false
		}
		result = append(result, shellHeredoc{Delimiter: line[start:offset], StripTabs: stripTabs})
		if len(result) > maxShellHeredocs {
			return nil, false
		}
	}
	return result, true
}

func targetEvidenceDigest(evidence TargetEvidence) (string, error) {
	evidence.EvidenceDigest = ""
	var value map[string]any
	encoded, err := canonicalJSON(evidence)
	if err != nil {
		return "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	delete(value, "evidence_digest")
	canonical, err := canonicalJSON(value)
	if err != nil {
		return "", err
	}
	return digestBytes(canonical), nil
}

// VerifyTargetEvidenceDigest is used again by the runtime verifier before it
// trusts the target projection.
func VerifyTargetEvidenceDigest(evidence TargetEvidence) error {
	if evidence.SchemaVersion != 1 || evidence.Producer != evidenceProducer || evidence.Verification != "target_render" || !validApplyMethod(evidence.ApplyMethod) || !digestPattern.MatchString(evidence.EvidenceDigest) {
		return fmt.Errorf("target evidence identity is invalid")
	}
	want, err := targetEvidenceDigest(evidence)
	if err != nil {
		return err
	}
	if want != evidence.EvidenceDigest {
		return fmt.Errorf("target evidence digest mismatch")
	}
	return nil
}

func validApplyMethod(value string) bool {
	return value == "" || value == "csa" || value == "ssa"
}

func scanStrictJSON(data []byte) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("JSON must be valid UTF-8")
	}
	if err := validateJSONStringEscapes(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	tokens := 0
	if err := scanJSONValue(decoder, "$", 0, &tokens); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("JSON trailing token: %w", err)
		}
		return fmt.Errorf("JSON has a trailing value")
	}
	return nil
}

func validateJSONStringEscapes(data []byte) error {
	for index := 0; index < len(data); index++ {
		if data[index] != '"' {
			continue
		}
		index++
		for ; index < len(data); index++ {
			switch data[index] {
			case '"':
				goto stringComplete
			case '\\':
				index++
				if index >= len(data) {
					return fmt.Errorf("JSON string ends with an incomplete escape")
				}
				if data[index] != 'u' {
					if !strings.ContainsRune(`"\\/bfnrt`, rune(data[index])) {
						return fmt.Errorf("JSON string contains an invalid escape")
					}
					continue
				}
				code, next, err := parseJSONUnicodeEscape(data, index)
				if err != nil {
					return err
				}
				index = next
				if code >= 0xD800 && code <= 0xDBFF {
					if index+2 >= len(data) || data[index+1] != '\\' || data[index+2] != 'u' {
						return fmt.Errorf("JSON string contains an unpaired high surrogate")
					}
					low, lowNext, err := parseJSONUnicodeEscape(data, index+2)
					if err != nil || low < 0xDC00 || low > 0xDFFF {
						return fmt.Errorf("JSON string contains an unpaired high surrogate")
					}
					index = lowNext
				} else if code >= 0xDC00 && code <= 0xDFFF {
					return fmt.Errorf("JSON string contains an unpaired low surrogate")
				}
			default:
				if data[index] < 0x20 {
					return fmt.Errorf("JSON string contains an unescaped control character")
				}
			}
		}
		return fmt.Errorf("JSON contains an unterminated string")
	stringComplete:
	}
	return nil
}

func parseJSONUnicodeEscape(data []byte, uIndex int) (uint16, int, error) {
	if uIndex+4 >= len(data) {
		return 0, uIndex, fmt.Errorf("JSON string contains an incomplete Unicode escape")
	}
	value := uint16(0)
	for offset := 1; offset <= 4; offset++ {
		character := data[uIndex+offset]
		value <<= 4
		switch {
		case character >= '0' && character <= '9':
			value += uint16(character - '0')
		case character >= 'a' && character <= 'f':
			value += uint16(character-'a') + 10
		case character >= 'A' && character <= 'F':
			value += uint16(character-'A') + 10
		default:
			return 0, uIndex, fmt.Errorf("JSON string contains an invalid Unicode escape")
		}
	}
	return value, uIndex + 4, nil
}

func scanJSONValue(decoder *json.Decoder, path string, depth int, tokens *int) error {
	if depth > maxJSONDepth {
		return fmt.Errorf("JSON exceeds maximum depth %d", maxJSONDepth)
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("JSON parse at %s: %w", path, err)
	}
	*tokens++
	if *tokens > maxJSONTokens {
		return fmt.Errorf("JSON exceeds token limit %d", maxJSONTokens)
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("JSON object key at %s: %w", path, err)
			}
			*tokens++
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("JSON object key at %s is not a string", path)
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("JSON contains a duplicate object key at %s", path)
			}
			seen[key] = struct{}{}
			if err := scanJSONValue(decoder, path+"/object-value", depth+1, tokens); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return fmt.Errorf("JSON object at %s is not closed", path)
		}
	case '[':
		for index := 0; decoder.More(); index++ {
			if err := scanJSONValue(decoder, path+"/"+strconv.Itoa(index), depth+1, tokens); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return fmt.Errorf("JSON array at %s is not closed", path)
		}
	default:
		return fmt.Errorf("JSON has unexpected delimiter %q at %s", delimiter, path)
	}
	return nil
}

func strictYAMLValue(node *yaml.Node, path string, depth int, count *int) (any, error) {
	if node == nil {
		return nil, fmt.Errorf("%s is missing", path)
	}
	if depth > maxYAMLDepth {
		return nil, fmt.Errorf("YAML exceeds maximum depth %d", maxYAMLDepth)
	}
	*count++
	if *count > maxYAMLNodes {
		return nil, fmt.Errorf("YAML exceeds node limit %d", maxYAMLNodes)
	}
	if node.Anchor != "" {
		return nil, fmt.Errorf("%s uses a YAML anchor", path)
	}
	if node.Kind == yaml.AliasNode || node.Tag == "!!merge" {
		return nil, fmt.Errorf("%s uses a YAML alias or merge key", path)
	}
	switch node.Kind {
	case yaml.DocumentNode:
		if len(node.Content) != 1 {
			return nil, fmt.Errorf("%s document must have exactly one value", path)
		}
		return strictYAMLValue(node.Content[0], path, depth+1, count)
	case yaml.MappingNode:
		if node.Tag != "!!map" || len(node.Content)%2 != 0 {
			return nil, fmt.Errorf("%s must be a standard complete mapping", path)
		}
		result := make(map[string]any, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			keyNode := node.Content[index]
			*count++
			if *count > maxYAMLNodes {
				return nil, fmt.Errorf("YAML exceeds node limit %d", maxYAMLNodes)
			}
			if keyNode.Kind != yaml.ScalarNode || keyNode.Tag != "!!str" || keyNode.Anchor != "" {
				return nil, fmt.Errorf("%s mapping key must be an unanchored string", path)
			}
			key := keyNode.Value
			if len(key) > maxYAMLKeyBytes {
				return nil, fmt.Errorf("%s contains an oversized mapping key", path)
			}
			if key == "<<" {
				return nil, fmt.Errorf("%s uses a YAML merge key", path)
			}
			if _, duplicate := result[key]; duplicate {
				return nil, fmt.Errorf("%s contains a duplicate mapping key", path)
			}
			value, err := strictYAMLValue(node.Content[index+1], path+"/mapping-value-"+strconv.Itoa(index/2), depth+1, count)
			if err != nil {
				return nil, err
			}
			result[key] = value
		}
		return result, nil
	case yaml.SequenceNode:
		if node.Tag != "!!seq" {
			return nil, fmt.Errorf("%s sequence has an unsupported tag", path)
		}
		result := make([]any, 0, len(node.Content))
		for index, child := range node.Content {
			value, err := strictYAMLValue(child, path+"/"+strconv.Itoa(index), depth+1, count)
			if err != nil {
				return nil, err
			}
			result = append(result, value)
		}
		return result, nil
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!str":
			return node.Value, nil
		case "!!null":
			return nil, nil
		case "!!bool":
			if strings.EqualFold(node.Value, "true") {
				return true, nil
			}
			if strings.EqualFold(node.Value, "false") {
				return false, nil
			}
			return nil, fmt.Errorf("%s has invalid boolean", path)
		case "!!int":
			if len(node.Value) > 1024 {
				return nil, fmt.Errorf("%s integer exceeds scalar limit", path)
			}
			integer, ok := new(big.Int).SetString(strings.ReplaceAll(node.Value, "_", ""), 0)
			if !ok {
				return nil, fmt.Errorf("%s has invalid integer", path)
			}
			return json.Number(integer.String()), nil
		case "!!float":
			return nil, fmt.Errorf("%s floating-point YAML scalars are not allowed in release manifests", path)
		default:
			return nil, fmt.Errorf("%s scalar has an unsupported tag", path)
		}
	default:
		return nil, fmt.Errorf("%s has unsupported YAML node kind %d", path, node.Kind)
	}
}

func implicitEmptyDocument(document *yaml.Node) bool {
	if document == nil || document.Kind != yaml.DocumentNode || len(document.Content) != 1 {
		return false
	}
	root := document.Content[0]
	return root != nil && root.Kind == yaml.ScalarNode && root.Tag == "!!null" && root.Value == "" && root.Style == 0
}

func decodeJSONExact(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode strict JSON failed")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("strict JSON contains trailing data")
	}
	return nil
}

type secureInput struct {
	file        *os.File
	stat        unix.Stat_t
	description string
}

func openSecureInput(path, description string) (*secureInput, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("%s path must be absolute", description)
	}
	parentPath := filepath.Dir(path)
	base := filepath.Base(path)
	if base == "." || base == ".." || strings.Contains(base, "/") {
		return nil, fmt.Errorf("%s path must name one file", description)
	}
	parentFD, err := unix.Open(parentPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("securely open %s parent: %w", description, err)
	}
	defer unix.Close(parentFD)
	var parentStat unix.Stat_t
	if err := unix.Fstat(parentFD, &parentStat); err != nil {
		return nil, fmt.Errorf("inspect open %s parent: %w", description, err)
	}
	if parentStat.Mode&unix.S_IFMT != unix.S_IFDIR || parentStat.Mode&0o077 != 0 || parentStat.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("%s parent must be a private current-user directory", description)
	}
	fileFD, err := unix.Openat(parentFD, base, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("securely open %s: %w", description, err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fileFD, &stat); err != nil {
		unix.Close(fileFD)
		return nil, fmt.Errorf("inspect open %s: %w", description, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o777 != 0o600 || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		unix.Close(fileFD)
		return nil, fmt.Errorf("%s must be a current-user 0600 regular non-symlink file", description)
	}
	file := os.NewFile(uintptr(fileFD), base)
	if file == nil {
		unix.Close(fileFD)
		return nil, fmt.Errorf("adopt securely open %s", description)
	}
	return &secureInput{file: file, stat: stat, description: description}, nil
}

func (input *secureInput) Close() {
	if input != nil && input.file != nil {
		_ = input.file.Close()
		input.file = nil
	}
}

func (input *secureInput) sameIdentity(other *secureInput) bool {
	return input != nil && other != nil && input.stat.Dev == other.stat.Dev && input.stat.Ino == other.stat.Ino
}

func (input *secureInput) read(maximum int64) ([]byte, error) {
	if input == nil || input.file == nil {
		return nil, fmt.Errorf("secure input is not open")
	}
	data, err := io.ReadAll(io.LimitReader(input.file, maximum+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", input.description, err)
	}
	if int64(len(data)) > maximum {
		return nil, fmt.Errorf("%s exceeds %d bytes", input.description, maximum)
	}
	var after unix.Stat_t
	if err := unix.Fstat(int(input.file.Fd()), &after); err != nil {
		return nil, fmt.Errorf("reinspect %s after read: %w", input.description, err)
	}
	if input.stat.Dev != after.Dev || input.stat.Ino != after.Ino || input.stat.Mode != after.Mode || input.stat.Uid != after.Uid || input.stat.Nlink != after.Nlink || after.Nlink != 1 || input.stat.Size != after.Size || after.Size != int64(len(data)) {
		return nil, fmt.Errorf("%s changed during read", input.description)
	}
	return data, nil
}

func readLimitedFile(path string, maximum int64, description string) ([]byte, error) {
	input, err := openSecureInput(path, description)
	if err != nil {
		return nil, err
	}
	defer input.Close()
	return input.read(maximum)
}

func parseImageRef(value string) (imageRef, error) {
	if value == "" || strings.Contains(value, "://") || strings.IndexFunc(value, func(r rune) bool { return r <= 0x20 || r == 0x7f }) >= 0 || strings.Count(value, "@") > 1 {
		return imageRef{}, fmt.Errorf("not a strict image reference")
	}
	nameAndTag := value
	digest := ""
	if strings.Contains(value, "@") {
		var found bool
		nameAndTag, digest, found = strings.Cut(value, "@")
		if !found || nameAndTag == "" || !digestPattern.MatchString(digest) {
			return imageRef{}, fmt.Errorf("invalid image digest")
		}
	}
	repository := nameAndTag
	tag := ""
	lastSlash := strings.LastIndex(nameAndTag, "/")
	lastColon := strings.LastIndex(nameAndTag, ":")
	if lastColon > lastSlash {
		repository, tag = nameAndTag[:lastColon], nameAndTag[lastColon+1:]
		if !tagPattern.MatchString(tag) {
			return imageRef{}, fmt.Errorf("invalid image tag")
		}
	}
	if err := validateRepository(repository); err != nil {
		return imageRef{}, err
	}
	if tag == "" && digest == "" {
		return imageRef{}, fmt.Errorf("image reference must have a tag or digest")
	}
	return imageRef{Repository: repository, Tag: tag, Digest: digest}, nil
}

func parseImageRefForDetection(value string) (imageRef, error) {
	if parsed, err := parseImageRef(value); err == nil {
		return parsed, nil
	}
	normalized, err := normalizeDetectionRegistryAuthority(value)
	if err != nil {
		return imageRef{}, err
	}
	return parseImageRef(normalized)
}

func repositoryBeforeDigestForDetection(value string) (string, error) {
	marker := strings.IndexByte(value, '@')
	if marker <= 0 {
		return "", fmt.Errorf("image reference has no digest separator")
	}
	nameAndTag := value[:marker]
	repository := nameAndTag
	lastSlash := strings.LastIndexByte(nameAndTag, '/')
	if lastColon := strings.LastIndexByte(nameAndTag, ':'); lastColon > lastSlash {
		if !tagPattern.MatchString(nameAndTag[lastColon+1:]) {
			return "", fmt.Errorf("image reference has an invalid tag")
		}
		repository = nameAndTag[:lastColon]
	}
	if validateRepository(repository) == nil {
		return repository, nil
	}
	normalized, err := normalizeDetectionRegistryAuthority(repository)
	if err != nil || validateRepository(normalized) != nil {
		return "", fmt.Errorf("image reference has an invalid repository")
	}
	return normalized, nil
}

func normalizeDetectionRegistryAuthority(value string) (string, error) {
	slash := strings.IndexByte(value, '/')
	if slash <= 0 {
		return "", fmt.Errorf("not a detectable registry reference")
	}
	authority := value[:slash]
	lowerAuthority := strings.ToLower(authority)
	if lowerAuthority != "localhost" && !strings.Contains(lowerAuthority, ".") && !strings.Contains(lowerAuthority, ":") {
		return "", fmt.Errorf("not a registry authority alias")
	}
	normalizedAuthority := lowerAuthority
	if host, port, found := strings.Cut(lowerAuthority, ":"); found {
		if strings.Contains(port, ":") || port == "" {
			return "", fmt.Errorf("invalid registry authority alias")
		}
		for index := 0; index < len(port); index++ {
			if port[index] < '0' || port[index] > '9' {
				return "", fmt.Errorf("invalid registry authority alias")
			}
		}
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 || !validRegistryHost(host) {
			return "", fmt.Errorf("invalid registry authority alias")
		}
		normalizedAuthority = host
		if portNumber != 443 {
			normalizedAuthority += ":" + strconv.Itoa(portNumber)
		}
	} else if !validRegistryHost(normalizedAuthority) {
		return "", fmt.Errorf("invalid registry authority alias")
	}
	return normalizedAuthority + value[slash:], nil
}

func validateRepository(value string) error {
	if value == "" || len(value) > 255 || strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") || strings.Contains(value, "//") || strings.ContainsAny(value, "@ \t\r\n") {
		return fmt.Errorf("invalid image repository")
	}
	parts := strings.Split(value, "/")
	repositoryParts := parts
	first := parts[0]
	if strings.Contains(first, ":") {
		if strings.Count(first, ":") != 1 || len(parts) < 2 {
			return fmt.Errorf("invalid registry authority")
		}
		host, port, _ := strings.Cut(first, ":")
		portNumber, err := strconv.Atoi(port)
		if err != nil || !registryPort.MatchString(port) || portNumber > 65535 || !validRegistryHost(host) {
			return fmt.Errorf("invalid registry authority")
		}
		repositoryParts = parts[1:]
	} else if len(parts) >= 2 && (first == "localhost" || strings.Contains(first, ".")) {
		if !validRegistryHost(first) {
			return fmt.Errorf("invalid registry authority")
		}
		repositoryParts = parts[1:]
	}
	if len(repositoryParts) == 0 {
		return fmt.Errorf("invalid image repository")
	}
	for _, part := range repositoryParts {
		if !repositoryPart.MatchString(part) {
			return fmt.Errorf("invalid image repository component")
		}
	}
	return nil
}

// canonicalRepository collapses OCI/Docker spellings that select the same
// registry endpoint. validateRepository must run before calling this helper.
func canonicalRepository(value string) string {
	parts := strings.Split(value, "/")
	first := parts[0]
	explicitRegistry := len(parts) >= 2 && (first == "localhost" || strings.Contains(first, ".") || strings.Contains(first, ":"))
	if !explicitRegistry {
		if len(parts) == 1 {
			return "docker.io/library/" + value
		}
		return "docker.io/" + value
	}
	registry := first
	if host, port, found := strings.Cut(first, ":"); found && port == "443" {
		registry = host
	}
	switch registry {
	case "index.docker.io", "registry-1.docker.io", "registry.hub.docker.com":
		registry = "docker.io"
	}
	path := strings.Join(parts[1:], "/")
	if registry == "docker.io" && !strings.Contains(path, "/") {
		path = "library/" + path
	}
	return registry + "/" + path
}

func validRegistryHost(host string) bool {
	if host == "" || len(host) > 253 || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") || strings.Contains(host, "..") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || !registryLabel.MatchString(label) {
			return false
		}
	}
	return true
}

func validWorkload(workload Workload) bool {
	if workload.Kind != "Deployment" && workload.Kind != "DaemonSet" && workload.Kind != "CronJob" && workload.Kind != "Configuration" {
		return false
	}
	return validDNSLabel(workload.Name) && validDNSLabel(workload.Container)
}

func validDNSLabel(value string) bool {
	return len(value) <= 63 && dnsLabelPattern.MatchString(value)
}

func validDNSSubdomain(value string) bool {
	if len(value) == 0 || len(value) > 253 {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if !validDNSLabel(label) {
			return false
		}
	}
	return true
}

func validKubeAPIVersion(value string) bool {
	parts := strings.Split(value, "/")
	if len(parts) == 1 {
		return validDNSLabel(parts[0])
	}
	return len(parts) == 2 && validDNSSubdomain(parts[0]) && validDNSLabel(parts[1])
}

func stringArray(value any, location string, optional bool) ([]string, error) {
	if value == nil && optional {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an array", location)
	}
	result := make([]string, 0, len(items))
	for index, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("%s[%d] must be a string", location, index)
		}
		result = append(result, text)
	}
	return result, nil
}

func objectArray(value any, location string, optional bool) ([]map[string]any, error) {
	if value == nil && optional {
		return nil, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an array", location)
	}
	result := make([]map[string]any, 0, len(items))
	for index, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s[%d] must be an object", location, index)
		}
		result = append(result, object)
	}
	return result, nil
}

func canonicalJSON(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, fmt.Errorf("encode canonical JSON: %w", err)
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n")), nil
}

// EncodeTargetEvidence returns deterministic compact JSON with one trailing newline.
func EncodeTargetEvidence(evidence TargetEvidence) ([]byte, error) {
	if err := VerifyTargetEvidenceDigest(evidence); err != nil {
		return nil, err
	}
	encoded, err := canonicalJSON(evidence)
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func digestStringSlice(values []string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = fmt.Fprintf(hash, "%d:", len([]byte(value)))
		_, _ = hash.Write([]byte(value))
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func equalStrings(left, right []string) bool {
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

func equalStringMap(left, right map[string][]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, values := range left {
		if !equalStrings(values, right[key]) {
			return false
		}
	}
	return true
}

func sortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

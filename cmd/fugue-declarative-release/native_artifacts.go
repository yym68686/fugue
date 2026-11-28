package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"fugue/internal/declarativerelease"
	"fugue/internal/releaseguardian"

	"github.com/google/go-containerregistry/pkg/authn"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

type nativeExecutionRuntime struct {
	binding  releaseguardian.NativeRuntimeBinding
	registry *releaseguardian.IndependentRegistry
}

// Native Guardians run with independently provisioned registry settings. A
// marked target without those settings (or a dedicated runtime with no marked
// target) must never drop into the legacy verifier.
func nativeExecutionRuntimeFromEnv() (*nativeExecutionRuntime, error) {
	raw := os.Getenv(releaseguardian.NativeRuntimeBindingEnv)
	host := os.Getenv(releaseguardian.NativeRuntimeRegistryEnv)
	authFile := os.Getenv(releaseguardian.NativeRuntimeAuthFileEnv)
	if raw == "" && host == "" && authFile == "" {
		return nil, nil
	}
	if os.Getenv("FUGUE_COMPONENT_LEASE_OWNER") != "guardian" || host == "" || raw == "" {
		return nil, errors.New("native registry execution requires a marked Guardian target and an independent registry")
	}
	binding, err := releaseguardian.DecodeNativeRuntimeBinding(raw)
	if err != nil {
		return nil, err
	}
	var authenticator authn.Authenticator
	if authFile != "" {
		authenticator, err = readNativeRegistryAuth(authFile)
		if err != nil {
			return nil, err
		}
	}
	registry, err := releaseguardian.NewIndependentRegistry(host, authenticator, nil)
	if err != nil {
		return nil, err
	}
	return &nativeExecutionRuntime{binding, registry}, nil
}

func readNativeRegistryAuth(path string) (authn.Authenticator, error) {
	info, err := os.Stat(path)
	if err != nil || !filepath.IsAbs(path) || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > 8192 {
		return nil, errors.New("native registry credentials require a bounded, mounted file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("open native registry credentials")
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 8193))
	var credentials struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err != nil || len(raw) > 8192 || decodeStrictJSON(raw, &credentials) != nil || credentials.Username == "" || credentials.Password == "" {
		return nil, errors.New("native registry credentials are invalid")
	}
	return authn.FromConfig(authn.AuthConfig{Username: credentials.Username, Password: credentials.Password}), nil
}

func (runtime *nativeExecutionRuntime) checkPointer(ctx context.Context, client coreclient.CoreV1Interface, release declarativerelease.PlanRelease) error {
	if runtime == nil {
		return nil
	}
	if release.ComponentID != runtime.binding.Key.Component ||
		(release.Delivery != nil && (release.Delivery.Writer != "guardian" || release.Delivery.Group != runtime.binding.Key.Group)) || release.Transition != nil ||
		len(release.RuntimeResourcesFromForward) != 0 || release.Workload.NodeParticipation != "" ||
		release.Workload.APIVersion != "apps/v1" || release.Workload.Kind != "Deployment" {
		return errors.New("native execution is restricted to one enrolled Guardian Deployment")
	}
	return runtime.binding.Verify(ctx, client, release.Workload.Namespace)
}

type nativeArtifactPair struct {
	target  declarativerelease.TargetIdentity
	receipt declarativerelease.ArtifactReceipt
}

// A successful per-operation readback can only be used with these exact
// reviewed references. Neither a foreign revision nor a historical receipt
// supplied via the legacy env variable can satisfy this verifier.
type nativeKubectlArtifacts struct {
	workload        declarativerelease.ResourceIdentity
	pairs           []nativeArtifactPair
	verify          func(context.Context, declarativerelease.ArtifactReceipt) (declarativerelease.RegistryVerification, error)
	services        []releaseguardian.NativeStaticService
	serviceReader   coreclient.CoreV1Interface
	serviceManifest []byte
}

func (runtime *nativeExecutionRuntime) artifactSet(release declarativerelease.PlanRelease, prepared declarativerelease.ExecutionPlan, forward declarativerelease.ArtifactReceipt, lkg *monitorBundle, forwardBytes, lkgBytes []byte) (*nativeKubectlArtifacts, error) {
	if runtime == nil {
		return nil, nil
	}
	scope := declarativerelease.ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name}
	pairs := []nativeArtifactPair{{prepared.Forward, forward}}
	manifests := [][]byte{forwardBytes}
	var services []releaseguardian.NativeStaticService
	if lkg != nil {
		if lkg.Record.RecordDigest != runtime.binding.LKGMonitorRecordDigest || lkg.Prepared.Forward != prepared.LKG || !bytes.Equal(lkg.Forward, lkgBytes) {
			return nil, errors.New("native execution LKG is not the exact bound monitor record")
		}
		pairs = append(pairs, nativeArtifactPair{prepared.LKG, lkg.Artifact})
		manifests = append(manifests, lkgBytes)
		declared, err := releaseguardian.VerifyNativeReleaseResourceSet(forwardBytes, scope, prepared.Forward)
		if err != nil {
			return nil, err
		}
		if len(declared) != 0 {
			services, err = releaseguardian.BindNativeStaticServices(scope, prepared, forwardBytes, lkgBytes)
			if err != nil {
				return nil, err
			}
		}
	}
	expected := make([]releaseguardian.RegistryRehomeService, len(services))
	for i, service := range services {
		expected[i] = service.Declaration
	}
	for i, pair := range pairs {
		receipt, target := pair.receipt, pair.target
		if !target.Present || receipt.Component != release.ComponentID || receipt.ConfigSHA != target.ConfigSHA ||
			receipt.ImmutableRef != target.ImageRef || receipt.OCIRevision != target.OCIRevision {
			return nil, errors.New("native runtime receipt differs from the sealed target")
		}
		actual, err := releaseguardian.VerifyNativeReleaseResourceSet(manifests[i], scope, target)
		if err != nil || !slices.Equal(actual, expected) {
			return nil, errors.Join(errors.New("native artifact set contains a different Service declaration"), err)
		}
		for _, previous := range pairs[:i] {
			if previous.receipt.ImmutableRef == receipt.ImmutableRef && previous.receipt.OCIRevision == receipt.OCIRevision &&
				(previous.receipt.PlatformManifestDigest != receipt.PlatformManifestDigest || previous.receipt.ConfigDigest != receipt.ConfigDigest) {
				return nil, errors.New("native runtime has conflicting receipts for one image")
			}
		}
	}
	return &nativeKubectlArtifacts{workload: scope, pairs: pairs, verify: runtime.registry.VerifyRuntimeArtifact, services: services,
		serviceManifest: lkgBytes}, nil
}

func (cluster *kubectlCluster) preflightNativeArtifacts(parent context.Context) error {
	if cluster.nativeArtifacts == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	for i, pair := range cluster.nativeArtifacts.pairs {
		if err := cluster.VerifyTarget(ctx, pair.target); err != nil {
			role := "forward"
			if i != 0 {
				role = "rollback"
			}
			return fmt.Errorf("preflight native %s image: %w", role, err)
		}
	}
	cluster.registryMu.Lock()
	cluster.nativePreflight = true
	cluster.registryMu.Unlock()
	return nil
}

func (artifacts *nativeKubectlArtifacts) verifyImage(ctx context.Context, image, revision string) (declarativerelease.RegistryVerification, error) {
	for _, pair := range artifacts.pairs {
		if pair.target.ImageRef == image && pair.target.OCIRevision == revision {
			return artifacts.verify(ctx, pair.receipt)
		}
	}
	return declarativerelease.RegistryVerification{}, errors.New("runtime image is not in the native approval's artifact set")
}

func (artifacts *nativeKubectlArtifacts) checkApply(target declarativerelease.TargetIdentity, manifest []byte) error {
	for _, pair := range artifacts.pairs {
		if pair.target == target {
			actual, err := releaseguardian.VerifyNativeReleaseResourceSet(manifest, artifacts.workload, target)
			if err != nil || len(actual) != len(artifacts.services) {
				return errors.Join(errors.New("native apply differs from the approved resource inventory"), err)
			}
			for i, d := range actual {
				if d != artifacts.services[i].Declaration {
					return errors.New("native apply changes a read-only Service declaration")
				}
			}
			return nil
		}
	}
	return errors.New("native apply target is not approved for this execution")
}

func (artifacts *nativeKubectlArtifacts) isStaticService(id declarativerelease.ResourceIdentity) bool {
	return artifacts != nil && slices.ContainsFunc(artifacts.services, func(service releaseguardian.NativeStaticService) bool {
		return service.Declaration.Identity == id
	})
}

func (cluster *kubectlCluster) applyNativeDeployment(ctx context.Context, release declarativerelease.PlanRelease, manifest []byte) error {
	if len(cluster.envRetirements) != 0 {
		return fmt.Errorf("%w: native update cannot delegate legacy environment retirement writes", declarativerelease.ErrWriteRejected)
	}
	if err := releaseguardian.VerifyNativeStaticServices(ctx, cluster.nativeArtifacts.serviceReader, manifest, cluster.nativeArtifacts.services); err != nil {
		return errors.Join(declarativerelease.ErrWriteRejected, err)
	}
	return releaseguardian.ApplyNativeDeployment(ctx, cluster.nativeDeployment, cluster.nativeArtifacts.workload, release.Workload.FieldManager, manifest)
}

func sameMonitorSnapshot(left, right monitorSnapshot) bool {
	return left.RecordName == right.RecordName && left.StateUID == right.StateUID &&
		left.StateRV == right.StateRV && left.Bundle.Record == right.Bundle.Record
}

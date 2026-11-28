package releaseguardian

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"fugue/internal/declarativerelease"
	"github.com/google/go-containerregistry/pkg/name"
)

type RegistryRehomeDraft struct {
	Schema        string                              `json:"schema"`
	ArchiveDigest string                              `json:"archiveDigest"`
	Component     string                              `json:"component"`
	Workload      declarativerelease.ResourceIdentity `json:"workload"`
	Services      []RegistryRehomeService             `json:"unchangedServices"`
	Current       RegistryRehomeProjection            `json:"current"`
	Rollback      RegistryRehomeProjection            `json:"rollback"`
	Mirror        RegistryRecoveryReport              `json:"mirror"`
	DraftDigest   string                              `json:"draftDigest,omitempty"`
}

type RegistryRehomeService struct {
	Identity    declarativerelease.ResourceIdentity `json:"identity"`
	ShapeDigest string                              `json:"shapeDigest"`
}

type RegistryRehomeProjection struct {
	RecordDigest         string                    `json:"recordDigest"`
	SourceManifestDigest string                    `json:"sourceManifestDigest"`
	SourceImage          string                    `json:"sourceImage"`
	IndependentImage     string                    `json:"independentImage"`
	SourcePodSpecDigest  string                    `json:"sourcePodSpecDigest"`
	TargetPodSpecDigest  string                    `json:"targetPodSpecDigest"`
	Containers           []RegistryRehomeContainer `json:"containers"`
}

type RegistryRehomeContainer struct {
	Field string `json:"field"`
	Name  string `json:"name"`
}

// DraftRegistryRehome checks a registry-only change for one existing
// Deployment against the independently pinned recovery point. Services can
// be carried as unchanged context only. The draft is neither a Kubernetes
// apply manifest nor an approval; it cannot replace either historical seal.
func (r *IndependentRegistry) DraftRegistryRehome(ctx context.Context, raw []byte, archiveDigest, currentRepository, rollbackRepository string) (RegistryRehomeDraft, error) {
	if r == nil || r.transport == nil {
		return RegistryRehomeDraft{}, errors.New("independent registry is unavailable")
	}
	if currentRepository != rollbackRepository {
		return RegistryRehomeDraft{}, errors.New("rehome requires one independent repository for both component versions")
	}
	archive, err := readReleaseRecoveryArchive(raw, archiveDigest)
	if err != nil {
		return RegistryRehomeDraft{}, err
	}
	project := func(record sealedRecoveryRecord, repository string) (RegistryRehomeProjection, []RegistryRehomeService, error) {
		source := record.version.Receipt
		repo, err := name.NewRepository(repository, name.StrictValidation)
		sourceRepo, srcErr := name.NewRepository(source.Repository, name.StrictValidation)
		if err != nil || srcErr != nil || repo.Name() != repository || repo.RegistryStr() != r.host ||
			sourceRepo.RegistryStr() == r.host {
			return RegistryRehomeProjection{}, nil, errors.New("rehome requires a change of registry to the configured independent origin")
		}
		return projectRegistryRehome(record, repo.Digest(source.TopDigest).Name())
	}
	current, currentServices, err := project(archive.forward, currentRepository)
	if err != nil {
		return RegistryRehomeDraft{}, fmt.Errorf("draft current registry move: %w", err)
	}
	rollback, rollbackServices, err := project(archive.rollback, rollbackRepository)
	if err != nil {
		return RegistryRehomeDraft{}, fmt.Errorf("draft rollback registry move: %w", err)
	}
	if len(currentServices) != len(rollbackServices) {
		return RegistryRehomeDraft{}, errors.New("registry rehome cannot introduce or remove a Service")
	}
	for i, service := range currentServices {
		if service != rollbackServices[i] {
			return RegistryRehomeDraft{}, errors.New("registry rehome cannot change a Service")
		}
	}
	pair := archive.report.Pair
	mirror, err := r.VerifyRecoveryMirror(ctx,
		RegistryRecoveryArtifact{Source: pair.Forward.Receipt, SourceReceiptDigest: pair.Forward.Monitor.ArtifactDigest, DestinationRepository: currentRepository},
		RegistryRecoveryArtifact{Source: pair.Rollback.Receipt, SourceReceiptDigest: pair.Rollback.Monitor.ArtifactDigest, DestinationRepository: rollbackRepository})
	if err != nil {
		return RegistryRehomeDraft{}, err
	}
	if mirror.Forward.RecoveryImage.Image != current.IndependentImage || mirror.Rollback.RecoveryImage.Image != rollback.IndependentImage {
		return RegistryRehomeDraft{}, errors.New("draft image projection differs from independent registry readback")
	}
	draft := RegistryRehomeDraft{Schema: "fugue.production.registry-rehome-draft.v1", ArchiveDigest: archiveDigest,
		Component: pair.Component, Workload: recoveryWorkloadKey(archive.forward.workload).resource, Services: currentServices,
		Current: current, Rollback: rollback, Mirror: mirror}
	draft.DraftDigest, err = registryRehomeDraftDigest(draft)
	if err != nil {
		return RegistryRehomeDraft{}, err
	}
	return draft, nil
}

func registryRehomeDraftDigest(draft RegistryRehomeDraft) (string, error) {
	draft.DraftDigest = ""
	canonical, err := declarativerelease.CanonicalJSON(draft)
	if err != nil {
		return "", err
	}
	return digest(canonical), nil
}

func projectRegistryRehome(record sealedRecoveryRecord, destination string) (RegistryRehomeProjection, []RegistryRehomeService, error) {
	p, services, _, err := projectRegistryRehomeWorkload(record, destination)
	return p, services, err
}

// projectRegistryRehomeWorkload keeps the original archives immutable. In
// addition to the reviewed PodSpec digest, it derives a single-Deployment
// recovery template. Companions remain context, never delegated writes.
func projectRegistryRehomeWorkload(record sealedRecoveryRecord, destination string) (RegistryRehomeProjection, []RegistryRehomeService, []byte, error) {
	fail := func(err error) (RegistryRehomeProjection, []RegistryRehomeService, []byte, error) {
		return RegistryRehomeProjection{}, nil, nil, err
	}
	scope := recoveryWorkloadKey(record.workload).resource
	source := record.prepared.Forward.ImageRef
	destinationRef, err := name.NewDigest(destination, name.StrictValidation)
	if err != nil || destinationRef.Name() != destination || destinationRef.DigestStr() != record.version.Receipt.TopDigest ||
		source != record.version.Receipt.ImmutableRef || source == destination ||
		scope.APIVersion != "apps/v1" || scope.Kind != "Deployment" {
		return fail(errors.New("registry rehome projection requires the original Deployment artifact digest"))
	}
	set, err := declarativerelease.DecodeResourceSet(bytes.NewReader(record.forward))
	if err != nil {
		return fail(err)
	}
	ids, err := declarativerelease.ResourceSetIdentities(record.forward)
	if err != nil {
		return fail(err)
	}
	deployment, err := set.Primary(record.workload)
	if err != nil {
		return fail(err)
	}
	services := make([]RegistryRehomeService, 0, len(ids)-1)
	for index, id := range ids {
		if id == scope {
			continue
		}
		if id.Kind != "Service" || id.APIVersion != "v1" || id.Namespace != scope.Namespace {
			return fail(errors.New("registry rehome cannot delegate writes to other release resources"))
		}
		canonical, err := registryRehomeServiceShape(set.Items[index])
		if err != nil {
			return fail(err)
		}
		services = append(services, RegistryRehomeService{Identity: id, ShapeDigest: digest(canonical)})
	}
	spec, _ := deployment["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	pod, _ := template["spec"].(map[string]any)
	containers, err := singleArtifactDeploymentContainers(pod, source)
	if err != nil {
		return fail(err)
	}
	before, err := declarativerelease.CanonicalJSON(pod)
	if err != nil {
		return fail(err)
	}
	changes := make([]RegistryRehomeContainer, 0, len(containers))
	for _, container := range containers {
		container.object["image"] = destination
		changes = append(changes, RegistryRehomeContainer{Field: container.field, Name: container.name})
	}
	after, err := declarativerelease.CanonicalJSON(pod)
	if err != nil {
		return fail(err)
	}
	if meta, ok := template["metadata"].(map[string]any); ok {
		if value, exists := meta["annotations"]; exists {
			annotations, ok := value.(map[string]any)
			if !ok {
				return fail(errors.New("registry rehome has invalid pod template annotations"))
			}
			if stamp, exists := annotations[podArtifactImageAnnotation]; exists {
				if stamp != source {
					return fail(errors.New("registry rehome source artifact stamp differs from its sealed image"))
				}
				annotations[podArtifactImageAnnotation] = destination
			}
		}
	}
	set.Items = []map[string]any{deployment}
	workload, err := declarativerelease.CanonicalJSON(set)
	if err != nil {
		return fail(err)
	}
	monitor := record.version.Monitor
	return RegistryRehomeProjection{RecordDigest: monitor.RecordDigest, SourceManifestDigest: monitor.ForwardManifestDigest,
		SourceImage: source, IndependentImage: destination, SourcePodSpecDigest: digest(before), TargetPodSpecDigest: digest(after), Containers: changes}, services, workload, nil
}

func registryRehomeServiceShape(item map[string]any) ([]byte, error) {
	meta, _ := item["metadata"].(map[string]any)
	if raw, present := meta["annotations"]; present {
		annotations, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.New("registry rehome Service annotations are invalid")
		}
		// These three stamps describe the release that observed this object,
		// not a change to the Service. All other fields bind.
		for _, key := range []string{"fugue.pro/production-config-sha", "fugue.pro/release-plan-digest", "fugue.pro/artifact-receipt-digest"} {
			delete(annotations, key)
		}
		if len(annotations) == 0 {
			delete(meta, "annotations")
		}
	}
	return declarativerelease.CanonicalJSON(item)
}

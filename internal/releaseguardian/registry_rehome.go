package releaseguardian

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"fugue/internal/declarativerelease"
	"github.com/google/go-containerregistry/pkg/name"
)

type RegistryRehomeDraft struct {
	Schema        string                              `json:"schema"`
	ArchiveDigest string                              `json:"archiveDigest"`
	Component     string                              `json:"component"`
	Workload      declarativerelease.ResourceIdentity `json:"workload"`
	Services      []RegistryRehomeService             `json:"unchangedServices"`
	RBAC          []RegistryRehomeCompanion           `json:"unchangedRBAC,omitempty"`
	Current       RegistryRehomeProjection            `json:"current"`
	Rollback      RegistryRehomeProjection            `json:"rollback"`
	Mirror        RegistryRecoveryReport              `json:"mirror"`
	DraftDigest   string                              `json:"draftDigest,omitempty"`
}

type RegistryRehomeCompanion struct {
	Identity    declarativerelease.ResourceIdentity `json:"identity"`
	ShapeDigest string                              `json:"shapeDigest"`
}

type RegistryRehomeService = RegistryRehomeCompanion

const (
	registryRehomeDraftV1 = "fugue.production.registry-rehome-draft.v1"
	registryRehomeDraftV2 = "fugue.production.registry-rehome-draft.v2"
)

type registryRehomeCompanions struct {
	services []RegistryRehomeService
	rbac     []RegistryRehomeCompanion
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
// Deployment against the independently pinned recovery point. Services and
// static RBAC can be carried as unchanged context only. The draft is neither
// a Kubernetes apply manifest nor an approval; it cannot replace either seal.
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
	project := func(record sealedRecoveryRecord, repository string) (RegistryRehomeProjection, registryRehomeCompanions, error) {
		source := record.version.Receipt
		repo, err := name.NewRepository(repository, name.StrictValidation)
		sourceRepo, srcErr := name.NewRepository(source.Repository, name.StrictValidation)
		if err != nil || srcErr != nil || repo.Name() != repository || repo.RegistryStr() != r.host ||
			sourceRepo.RegistryStr() == r.host {
			return RegistryRehomeProjection{}, registryRehomeCompanions{}, errors.New("rehome requires a change of registry to the configured independent origin")
		}
		projection, companions, _, err := projectRegistryRehomeWorkload(record, repo.Digest(source.TopDigest).Name())
		return projection, companions, err
	}
	current, currentCompanions, err := project(archive.forward, currentRepository)
	if err != nil {
		return RegistryRehomeDraft{}, fmt.Errorf("draft current registry move: %w", err)
	}
	rollback, rollbackCompanions, err := project(archive.rollback, rollbackRepository)
	if err != nil {
		return RegistryRehomeDraft{}, fmt.Errorf("draft rollback registry move: %w", err)
	}
	if !slices.Equal(currentCompanions.services, rollbackCompanions.services) || !slices.Equal(currentCompanions.rbac, rollbackCompanions.rbac) {
		return RegistryRehomeDraft{}, errors.New("registry rehome cannot introduce, remove or change a Service or RBAC companion")
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
	schema := registryRehomeDraftV1
	if len(currentCompanions.rbac) != 0 {
		schema = registryRehomeDraftV2
	}
	draft := RegistryRehomeDraft{Schema: schema, ArchiveDigest: archiveDigest,
		Component: pair.Component, Workload: recoveryWorkloadKey(archive.forward.workload).resource,
		Services: currentCompanions.services, RBAC: currentCompanions.rbac,
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

// projectRegistryRehomeWorkload keeps the original archives immutable. In
// addition to the reviewed PodSpec digest, it derives a single-Deployment
// recovery template. Companions remain context, never delegated writes.
func projectRegistryRehomeWorkload(record sealedRecoveryRecord, destination string) (RegistryRehomeProjection, registryRehomeCompanions, []byte, error) {
	fail := func(err error) (RegistryRehomeProjection, registryRehomeCompanions, []byte, error) {
		return RegistryRehomeProjection{}, registryRehomeCompanions{}, nil, err
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
	companions := registryRehomeCompanions{services: make([]RegistryRehomeService, 0, len(ids)-1)}
	for index, id := range ids {
		if id == scope {
			continue
		}
		companion, err := registryRehomeCompanionShape(set.Items[index], id, scope.Namespace)
		if err != nil {
			return fail(err)
		}
		if id.Kind == "Service" {
			companions.services = append(companions.services, companion)
		} else {
			companions.rbac = append(companions.rbac, companion)
		}
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
		SourceImage: source, IndependentImage: destination, SourcePodSpecDigest: digest(before), TargetPodSpecDigest: digest(after), Containers: changes}, companions, workload, nil
}

func registryRehomeCompanionShape(item map[string]any, id declarativerelease.ResourceIdentity, namespace string) (RegistryRehomeCompanion, error) {
	if !registryRehomeServiceIdentity(id, namespace) && !registryRehomeRBACIdentity(id) {
		return RegistryRehomeCompanion{}, errors.New("registry rehome can only carry Service or static RBAC companions")
	}
	if registryRehomeRBACIdentity(id) {
		if _, err := registryRehomeRBACAccess(id, item); err != nil {
			return RegistryRehomeCompanion{}, err
		}
	}
	meta, _ := item["metadata"].(map[string]any)
	if raw, present := meta["annotations"]; present {
		annotations, ok := raw.(map[string]any)
		if !ok {
			return RegistryRehomeCompanion{}, errors.New("registry rehome companion annotations are invalid")
		}
		// These three stamps describe the release that observed this object,
		// not a change to the companion. All other fields bind.
		for _, key := range []string{"fugue.pro/production-config-sha", "fugue.pro/release-plan-digest", "fugue.pro/artifact-receipt-digest"} {
			delete(annotations, key)
		}
		if len(annotations) == 0 {
			delete(meta, "annotations")
		}
	}
	canonical, err := declarativerelease.CanonicalJSON(item)
	if err != nil {
		return RegistryRehomeCompanion{}, err
	}
	return RegistryRehomeCompanion{Identity: id, ShapeDigest: digest(canonical)}, nil
}

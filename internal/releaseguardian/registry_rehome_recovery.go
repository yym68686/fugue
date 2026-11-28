package releaseguardian

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	"github.com/google/go-containerregistry/pkg/name"
)

const (
	registryRehomeRecoverySchema   = "fugue.production.registry-rehome-recovery.v1"
	MaxRegistryRehomeRecoveryBytes = 2 * MaxReleaseArchiveBytes
)

// RegistryRehomeRecoveryReport identifies two derived recovery templates.
// It is not a serving receipt, dispatch permit, or terminal journal result.
type RegistryRehomeRecoveryReport struct {
	BundleDigest  string                              `json:"bundleDigest"`
	ArchiveDigest string                              `json:"archiveDigest"`
	IntentDigest  string                              `json:"intentDigest"`
	Workload      declarativerelease.ResourceIdentity `json:"workload"`
	Current       RegistryRehomeRecoveryVersion       `json:"current"`
	Rollback      RegistryRehomeRecoveryVersion       `json:"rollback"`
}

type RegistryRehomeRecoveryVersion struct {
	SourceRecordDigest string `json:"sourceRecordDigest"`
	TemplateDigest     string `json:"templateDigest"`
	Image              string `json:"image"`
}

type registryRehomeRecoveryBundle struct {
	Schema   string                       `json:"schema"`
	Archive  []byte                       `json:"sealedSourceArchive"`
	Review   productioncontrol.Submission `json:"review"`
	Current  []byte                       `json:"currentDeploymentTemplate"`
	Rollback []byte                       `json:"rollbackDeploymentTemplate"`
}

// deriveRegistryRehomeRecovery validates both halves against their actual
// immutable records. Validating the shape of a digest in the review alone
// would allow a well-formed but unrelated rollback into a reviewed proposal.
func deriveRegistryRehomeRecovery(archive verifiedReleaseRecoveryArchive, review RegistryRehomeReview) ([]byte, []byte, error) {
	d := review.Draft
	pair := archive.report.Pair
	if d.ArchiveDigest != archive.report.ArchiveDigest || d.Component != pair.Component ||
		d.Workload != recoveryWorkloadKey(archive.forward.workload).resource ||
		review.Source.MonitorUID != pair.Pointer.UID {
		return nil, nil, errors.New("registry rehome recovery does not bind the exact sealed archive and monitor")
	}
	if delivery := archive.forward.release.Delivery; delivery != nil &&
		(delivery.Writer != "guardian" || delivery.Group != review.Source.Key.Group) {
		return nil, nil, errors.New("registry rehome recovery group differs from the sealed release")
	}
	if review.Source.Desired == (RecoveryRecordRef{}) {
		stable, _, err := canonicalStableReleaseRecord(review.Source.Key, archive.forward.files)
		if err != nil || stable.RecordDigest != review.Source.Version.Digest {
			return nil, nil, errors.New("registry rehome recovery predecessor does not match the sealed release")
		}
	}
	build := func(record sealedRecoveryRecord, p RegistryRehomeProjection, m RegistryRecoveryReadback) ([]byte, error) {
		receipt := record.version.Receipt
		if record.release.Transition != nil || len(record.release.RuntimeResourcesFromForward) != 0 || record.workload.NodeParticipation != "" {
			return nil, errors.New("sealed release delegates a runtime transition")
		}
		projected, services, template, err := projectRegistryRehomeWorkload(record, p.IndependentImage)
		if err != nil {
			return nil, err
		}
		if !slices.Equal(services, d.Services) || !slices.Equal(projected.Containers, p.Containers) {
			return nil, errors.New("reviewed resources differ from the sealed release")
		}
		// Containers have already been compared by value and order.
		projected.Containers, p.Containers = nil, nil
		actual, err := declarativerelease.CanonicalJSON(projected)
		expected, expectedErr := declarativerelease.CanonicalJSON(p)
		image := m.RecoveryImage
		if err != nil || expectedErr != nil || !bytes.Equal(actual, expected) || m.SourceReceiptDigest != receipt.ReceiptDigest ||
			image.IndexDigest != receipt.TopDigest || image.ManifestDigest != receipt.PlatformManifestDigest ||
			image.ConfigDigest != receipt.ConfigDigest || image.OCIRevision != receipt.OCIRevision || image.Platform != receipt.Platform {
			return nil, errors.New("reviewed projection or mirror does not match its sealed manifest and artifact identity")
		}
		return template, nil
	}
	current, err := build(archive.forward, d.Current, d.Mirror.Forward)
	if err != nil {
		return nil, nil, fmt.Errorf("registry rehome current recovery: %w", err)
	}
	rollback, err := build(archive.rollback, d.Rollback, d.Mirror.Rollback)
	if err != nil {
		return nil, nil, fmt.Errorf("registry rehome rollback recovery: %w", err)
	}
	return current, rollback, nil
}

// BuildRegistryRehomeRecoveryBundle produces a self-contained recovery
// artifact, retaining the source archive byte-for-byte and allowing only
// the single Deployment in each derived template. Keep this package
// private: it contains the same historical manifests as the source archive.
func BuildRegistryRehomeRecoveryBundle(raw []byte, archiveDigest string, s productioncontrol.Submission) ([]byte, RegistryRehomeRecoveryReport, error) {
	archive, err := readReleaseRecoveryArchive(raw, archiveDigest)
	if err != nil {
		return nil, RegistryRehomeRecoveryReport{}, err
	}
	return buildRegistryRehomeRecoveryBundle(raw, archive, s, nil)
}

func buildRegistryRehomeRecoveryBundle(raw []byte, archive verifiedReleaseRecoveryArchive, s productioncontrol.Submission, expected *registryRehomeRecoveryBundle) ([]byte, RegistryRehomeRecoveryReport, error) {
	review, err := InspectRegistryRehomeSubmission(s)
	if err != nil {
		return nil, RegistryRehomeRecoveryReport{}, err
	}
	current, rollback, err := deriveRegistryRehomeRecovery(archive, review)
	if err != nil {
		return nil, RegistryRehomeRecoveryReport{}, err
	}
	if expected != nil && (!bytes.Equal(current, expected.Current) || !bytes.Equal(rollback, expected.Rollback)) {
		return nil, RegistryRehomeRecoveryReport{}, errors.New("registry rehome recovery template differs from the sealed projection")
	}
	packaged, err := declarativerelease.CanonicalJSON(registryRehomeRecoveryBundle{registryRehomeRecoverySchema, raw, s, current, rollback})
	if err != nil || len(packaged) > MaxRegistryRehomeRecoveryBytes {
		return nil, RegistryRehomeRecoveryReport{}, errors.New("registry rehome recovery bundle exceeds its canonical size bound")
	}
	intent, err := s.Intent.Digest()
	if err != nil {
		return nil, RegistryRehomeRecoveryReport{}, err
	}
	d := review.Draft
	return packaged, RegistryRehomeRecoveryReport{digest(packaged), archive.report.ArchiveDigest, intent, d.Workload,
		RegistryRehomeRecoveryVersion{d.Current.RecordDigest, digest(current), d.Current.IndependentImage},
		RegistryRehomeRecoveryVersion{d.Rollback.RecordDigest, digest(rollback), d.Rollback.IndependentImage}}, nil
}

// VerifyRegistryRehomeRecoveryBundle starts with an externally retained pin,
// reconstructs both templates, and optionally checks that both private
// runtime images still read back from the independent OCI origin.
func VerifyRegistryRehomeRecoveryBundle(ctx context.Context, raw []byte, pin string, registry *IndependentRegistry) (RegistryRehomeRecoveryReport, error) {
	verified, err := verifyRegistryRehomeRecoveryBundle(raw, pin)
	if err != nil {
		return RegistryRehomeRecoveryReport{}, err
	}
	if registry != nil {
		if err := verifyRegistryRehomeRecoveryMirror(ctx, verified, registry); err != nil {
			return RegistryRehomeRecoveryReport{}, err
		}
	}
	return verified.report, nil
}

func verifyRegistryRehomeRecoveryMirror(ctx context.Context, verified verifiedRegistryRehomeRecoveryBundle, registry *IndependentRegistry) error {
	ref, err := name.NewDigest(verified.report.Current.Image, name.StrictValidation)
	if err != nil {
		return err
	}
	pair := verified.archive.report.Pair
	source := func(v SealedRecoveryVersion) RegistryRecoveryArtifact {
		return RegistryRecoveryArtifact{Source: v.Receipt, SourceReceiptDigest: v.Monitor.ArtifactDigest, DestinationRepository: ref.Context().Name()}
	}
	readback, err := registry.VerifyRecoveryMirror(ctx, source(pair.Forward), source(pair.Rollback))
	if err != nil {
		return err
	}
	review, err := InspectRegistryRehomeSubmission(verified.bundle.Review)
	if err != nil || readback != review.Draft.Mirror {
		return errors.Join(errors.New("independent registry readback differs from the recovery bundle"), err)
	}
	return nil
}

type verifiedRegistryRehomeRecoveryBundle struct {
	bundle  registryRehomeRecoveryBundle
	archive verifiedReleaseRecoveryArchive
	report  RegistryRehomeRecoveryReport
}

func verifyRegistryRehomeRecoveryBundle(raw []byte, pin string) (verifiedRegistryRehomeRecoveryBundle, error) {
	var none verifiedRegistryRehomeRecoveryBundle
	if len(raw) == 0 || len(raw) > MaxRegistryRehomeRecoveryBytes || !digestPattern.MatchString(pin) || digest(raw) != pin {
		return none, errors.New("registry rehome recovery bundle does not match its independently pinned digest or size bound")
	}
	var bundle registryRehomeRecoveryBundle
	if err := decodeStrict(raw, &bundle); err != nil || bundle.Schema != registryRehomeRecoverySchema {
		return none, errors.New("invalid registry rehome recovery bundle")
	}
	archive, err := readReleaseRecoveryArchive(bundle.Archive, digest(bundle.Archive))
	if err != nil {
		return none, errors.Join(errors.New("registry rehome recovery bundle contains an invalid sealed archive"), err)
	}
	canonical, report, err := buildRegistryRehomeRecoveryBundle(bundle.Archive, archive, bundle.Review, &bundle)
	if err != nil || !bytes.Equal(canonical, raw) {
		return none, errors.Join(errors.New("registry rehome recovery bundle is not its canonical sealed projection"), err)
	}
	return verifiedRegistryRehomeRecoveryBundle{bundle, archive, report}, nil
}

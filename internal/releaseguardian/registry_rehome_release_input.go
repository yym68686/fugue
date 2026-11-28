package releaseguardian

import (
	"bytes"
	"errors"

	"fugue/internal/declarativerelease"
)

// RegistryRehomeReleaseInput describes a derived release manifest, not a
// release record, serving observation, or authorization to use it. The old
// artifact and monitor receipts remain historical and are not reissued.
type RegistryRehomeReleaseInput struct {
	BundleDigest       string                                `json:"bundleDigest"`
	IntentDigest       string                                `json:"intentDigest"`
	Role               string                                `json:"role"`
	SourceRecordDigest string                                `json:"sourceRecordDigest"`
	Workload           declarativerelease.ResourceIdentity   `json:"workload"`
	Companions         []declarativerelease.ResourceIdentity `json:"companions,omitempty"`
	Target             declarativerelease.TargetIdentity     `json:"target"`
}

// ExportRegistryRehomeReleaseInput reconstructs the full canonical resource
// set for one retained version from a pinned bundle. The Deployment-only
// mutation template is intentionally unsuitable as the next Guardian
// release's exact LKG: omitting its companions loses rollback context.
func ExportRegistryRehomeReleaseInput(raw []byte, pin, role string) ([]byte, RegistryRehomeReleaseInput, error) {
	var none RegistryRehomeReleaseInput
	if role != "current" && role != "rollback" {
		return nil, none, errors.New("registry rehome release input role must be current or rollback")
	}
	verified, err := verifyRegistryRehomeRecoveryBundle(raw, pin)
	if err != nil {
		return nil, none, err
	}
	bundle, report, archive := verified.bundle, verified.report, verified.archive
	record, projection, version := archive.forward, bundle.Current, report.Current
	if role == "rollback" {
		record, projection, version = archive.rollback, bundle.Rollback, report.Rollback
	}
	set, err := declarativerelease.DecodeResourceSet(bytes.NewReader(record.forward))
	if err != nil {
		return nil, none, err
	}
	ids, err := declarativerelease.ResourceSetIdentities(record.forward)
	if err != nil {
		return nil, none, err
	}
	replacement, err := declarativerelease.ResourceSetItem(projection, report.Workload)
	if err != nil {
		return nil, none, err
	}
	replaced := false
	var companions []declarativerelease.ResourceIdentity
	for i, id := range ids {
		if id == report.Workload {
			set.Items[i] = replacement
			replaced = true
		} else {
			companions = append(companions, id)
		}
	}
	if !replaced {
		return nil, none, errors.New("registry rehome release input lacks its sealed Deployment")
	}
	manifest, err := declarativerelease.CanonicalJSON(set)
	if err != nil {
		return nil, none, err
	}
	// Both the envelope and its Service objects come from the corresponding
	// sealed release. Only the intentionally relocated workload image differs.
	target := record.prepared.Forward
	target.ImageRef, target.ManifestDigest = version.Image, digest(manifest)
	if err := VerifyNativeDeploymentImages(projection, report.Workload, target); err != nil {
		return nil, none, err
	}
	return manifest, RegistryRehomeReleaseInput{
		BundleDigest: pin, IntentDigest: report.IntentDigest, Role: role,
		SourceRecordDigest: version.SourceRecordDigest, Workload: report.Workload, Companions: companions, Target: target,
	}, nil
}

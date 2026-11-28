package releaseguardian

import (
	"bytes"
	"context"
	"errors"

	"fugue/internal/declarativerelease"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

const (
	recoveryArchiveSchema  = "fugue.production.sealed-release-recovery.v1"
	MaxReleaseArchiveBytes = 8 << 20
)

type archivedRecoveryRecord struct {
	Ref   RecoveryRecordRef `json:"ref"`
	Files map[string]string `json:"files"`
}

type sealedRecoveryArchive struct {
	Schema    string                          `json:"schema"`
	Namespace string                          `json:"namespace"`
	Component string                          `json:"component"`
	Pointer   RecoveryRecordRef               `json:"pointer"`
	State     declarativerelease.MonitorState `json:"state"`
	Forward   archivedRecoveryRecord          `json:"forward"`
	Rollback  archivedRecoveryRecord          `json:"rollback"`
}

type ReleaseRecoveryArchiveReport struct {
	ArchiveDigest string             `json:"archiveDigest"`
	Pair          SealedRecoveryPair `json:"pair"`
}

// CaptureReleaseRecoveryArchive packages the exact files taken from a
// bracketed provider observation. It does not request Kubernetes Secret
// objects or restage any release. Manifests are private recovery material.
func CaptureReleaseRecoveryArchive(ctx context.Context, client coreclient.CoreV1Interface, namespace, component string) ([]byte, ReleaseRecoveryArchiveReport, error) {
	var archive sealedRecoveryArchive
	pair, err := observeSealedRecoveryPair(ctx, client, namespace, component, &archive)
	if err != nil {
		return nil, ReleaseRecoveryArchiveReport{}, err
	}
	raw, err := declarativerelease.CanonicalJSON(archive)
	if err != nil {
		return nil, ReleaseRecoveryArchiveReport{}, err
	}
	pin := digest(raw)
	report, err := VerifyReleaseRecoveryArchive(raw, pin)
	if err != nil {
		return nil, ReleaseRecoveryArchiveReport{}, err
	}
	if pair != report.Pair {
		return nil, ReleaseRecoveryArchiveReport{}, errors.New("release recovery archive differs from its source observation")
	}
	return raw, report, nil
}

// VerifyReleaseRecoveryArchive needs a digest retained independently of the
// archive bytes and trusted as the intended historical recovery point. A
// valid archive is neither current serving evidence nor permission to apply.
func VerifyReleaseRecoveryArchive(raw []byte, expectedDigest string) (ReleaseRecoveryArchiveReport, error) {
	invalid := func(reason string) (ReleaseRecoveryArchiveReport, error) {
		return ReleaseRecoveryArchiveReport{}, errors.New(reason)
	}
	if len(raw) == 0 || len(raw) > MaxReleaseArchiveBytes || !digestPattern.MatchString(expectedDigest) || digest(raw) != expectedDigest {
		return invalid("release recovery archive does not match its independently pinned digest or size bound")
	}
	var archive sealedRecoveryArchive
	if decodeStrict(raw, &archive) != nil || archive.Schema != recoveryArchiveSchema ||
		!componentPattern.MatchString(archive.Namespace) || !componentPattern.MatchString(archive.Component) ||
		archive.Pointer.Name != "fugue-release-monitor-"+archive.Component || archive.Pointer.UID == "" || archive.Pointer.ResourceVersion == "" ||
		archive.State.Validate() != nil || archive.State.RollbackStatus != "" || archive.State.Component != archive.Component ||
		len(archive.Forward.Files) != 7 || len(archive.Rollback.Files) != 7 {
		return invalid("invalid release recovery archive envelope or file set")
	}
	canonical, err := declarativerelease.CanonicalJSON(archive)
	if err != nil || !bytes.Equal(raw, canonical) {
		return invalid("release recovery archive is not canonical")
	}
	forward, fErr := decodeSealedRecoveryRecord(archive.Forward.Ref, archive.Forward.Files, archive.Component)
	rollback, rErr := decodeSealedRecoveryRecord(archive.Rollback.Ref, archive.Rollback.Files, archive.Component)
	if fErr != nil || rErr != nil || forward.version.Monitor.RecordDigest != archive.State.RecordDigest ||
		forward.version.Monitor.ConfigSHA != archive.State.ConfigSHA || forward.workload.Namespace != archive.Namespace ||
		!isSealedRollback(forward, rollback) {
		return invalid("release recovery archive does not bind its exact verified forward and rollback records")
	}
	return ReleaseRecoveryArchiveReport{ArchiveDigest: expectedDigest, Pair: SealedRecoveryPair{
		Namespace: archive.Namespace, Component: archive.Component, Pointer: archive.Pointer,
		Forward: forward.version, Rollback: rollback.version,
	}}, nil
}

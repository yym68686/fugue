package store

import (
	"errors"
	"strings"

	"fugue/internal/model"
)

// ErrCanonicalDigestRequired is returned when a distributed image identity is
// about to be advertised as usable without a content-addressed identity.  A
// tag (or a manifest HEAD response) is not sufficient evidence for a
// Present/Available record: callers must first verify the manifest graph and
// persist its canonical digest.
var (
	ErrCanonicalDigestRequired = errors.New("canonical image digest is required")
	ErrImageDigestMismatch     = errors.New("image replica digest does not match canonical image digest")
)

// CanonicalImageDigest reports a normalized, content-addressed sha256 digest
// when raw is one.  The image-cache and registry paths currently use sha256
// digests; accepting arbitrary strings here would re-introduce the identity
// ambiguity that this guard is intended to prevent.
func CanonicalImageDigest(raw string) string {
	raw = normalizeImageDigest(raw)
	if !isCanonicalImageDigest(raw) {
		return ""
	}
	return strings.ToLower(raw)
}

func isCanonicalImageDigest(raw string) bool {
	if len(raw) != len("sha256:")+64 || !strings.HasPrefix(strings.ToLower(raw), "sha256:") {
		return false
	}
	for _, r := range raw[len("sha256:"):] {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F') {
			continue
		}
		return false
	}
	return true
}

// ImageLocationIsDistributed is deliberately based on physical identity,
// rather than on a particular runtime name.  It covers reports from both
// managed shared and explicitly managed runtimes.
func ImageLocationIsDistributed(location model.ImageLocation) bool {
	return strings.TrimSpace(location.NodeID) != "" ||
		strings.TrimSpace(location.RuntimeID) != "" ||
		strings.TrimSpace(location.ClusterNodeName) != "" ||
		strings.TrimSpace(location.CacheEndpoint) != ""
}

func validateAvailableImage(image model.Image) error {
	if strings.EqualFold(strings.TrimSpace(image.LifecycleState), model.ImageLifecycleAvailable) &&
		CanonicalImageDigest(image.CanonicalDigest) == "" {
		return errors.Join(ErrInvalidInput, ErrCanonicalDigestRequired)
	}
	return nil
}

func validatePresentImageReplica(replica model.ImageReplica) error {
	if strings.EqualFold(strings.TrimSpace(replica.Status), model.ImageReplicaStatusPresent) &&
		CanonicalImageDigest(replica.Digest) == "" {
		return errors.Join(ErrInvalidInput, ErrCanonicalDigestRequired)
	}
	return nil
}

func validatePresentImageLocation(location model.ImageLocation) error {
	if strings.EqualFold(strings.TrimSpace(location.Status), model.ImageLocationStatusPresent) &&
		ImageLocationIsDistributed(location) && CanonicalImageDigest(location.Digest) == "" {
		return errors.Join(ErrInvalidInput, ErrCanonicalDigestRequired)
	}
	return nil
}

// ValidateDistributedImageLocation is the public boundary used by API and
// controller reporters.  The generic Store location table still accepts
// historical tag-only compatibility rows so old read paths can be repaired;
// any new distributed Present report must pass this guard first.
func ValidateDistributedImageLocation(location model.ImageLocation) error {
	return validatePresentImageLocation(location)
}

func imageDigestFromReference(ref string) string {
	ref = strings.ToLower(strings.TrimSpace(ref))
	idx := strings.LastIndex(ref, "@sha256:")
	if idx < 0 {
		return ""
	}
	return CanonicalImageDigest(ref[idx+1:])
}

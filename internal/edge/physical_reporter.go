package edge

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"fugue/internal/config"
	"fugue/internal/edgegroupfront"
	"github.com/google/go-containerregistry/pkg/name"
)

var physicalSource = regexp.MustCompile(`^[0-9a-f]{40}$`)

func (s *Service) physicalMode() bool {
	return s.RouteBundleSource.ReporterImageRef != nil || s.RouteBundleSource.PrewarmPredecessor != nil
}

func (s *Service) physicalImageDigest() string {
	ref := s.RouteBundleSource.ReporterImageRef
	if ref == nil {
		return ""
	}
	image, err := name.NewDigest(*ref, name.StrictValidation)
	if err != nil || image.Name() != *ref || !edgeRouteDigestPattern.MatchString(image.DigestStr()) {
		return ""
	}
	return image.DigestStr()
}

func (s *Service) prewarmIdentityBound(cfg config.EdgeConfig) bool {
	pin := s.RouteBundleSource.PrewarmPredecessor
	if pin == nil {
		return false
	}
	node, digest, ok := strings.Cut(*pin, "@")
	return ok && node != "" && node == cfg.EdgeID && edgeRouteDigestPattern.MatchString(digest) &&
		cfg.EdgeInstanceUID != "" && cfg.EdgeGroupID != "" && physicalSource.MatchString(cfg.EdgeReleaseEpoch) &&
		(cfg.EdgeSlot == "a" || cfg.EdgeSlot == "b") && s.physicalImageDigest() != ""
}

func (s *Service) canPrewarm(a edgegroupfront.ActivationState) bool {
	if !s.prewarmIdentityBound(s.Config) || a.GroupID != s.Config.EdgeGroupID || a.ActiveSlot == s.Config.EdgeSlot ||
		a.WorkerImageDigest == s.physicalImageDigest() {
		return false
	}
	// Use the canonical on-disk bytes covered by ActivationReceipt.StateDigest.
	wire, err := json.Marshal(a)
	return err == nil && *s.RouteBundleSource.PrewarmPredecessor == fmt.Sprintf("%s@sha256:%x", s.Config.EdgeID, sha256.Sum256(append(wire, '\n')))
}

func (s *Service) matchesPreparedImage(digest string) bool {
	return !s.physicalMode() || (digest != "" && s.physicalImageDigest() == digest)
}

// In overlap mode a retained Worker and the replacement can have the same
// slot letter, on the same node. Only the exact selected incarnation may
// speak for that node. This is not authority to delete the other process or
// to stop its established streams. The release supplies an immutable image
// reference from the Pod template; expiry/selection still comes from the
// Front's validated activation document, not from an environment toggle.
func (s *Service) physicalReporterMatches(cfg config.EdgeConfig, a edgegroupfront.ActivationState) bool {
	if !s.physicalMode() {
		return true // legacy checks at the caller are unchanged
	}
	if s.RouteBundleSource.PrewarmPredecessor != nil && !s.prewarmIdentityBound(cfg) {
		return false
	}
	digest := s.physicalImageDigest()
	return digest != "" &&
		cfg.EdgeInstanceUID != "" && cfg.EdgeID != "" && cfg.EdgeGroupID == a.GroupID &&
		cfg.EdgeSlot == a.ActiveSlot && cfg.EdgeReleaseEpoch == a.WorkerSourceCommit && digest == a.WorkerImageDigest
}

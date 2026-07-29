package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/imagecachekeys"
	"fugue/internal/model"
	"fugue/internal/store"
)

// reconcileDistributedImageIntegrity repairs legacy rows that were written
// before canonical digest enforcement existed.  It is intentionally
// evidence-based: a fresh, unique image-cache manifest graph may promote a
// row to its digest; missing or ambiguous evidence is demoted to a
// non-serving state and never guessed.
func (s *Service) reconcileDistributedImageIntegrity(ctx context.Context) error {
	if s == nil || s.Store == nil || !s.imageStoreDistributedMode() {
		return nil
	}
	ttl := s.Config.ImageCacheInventoryTTL
	if ttl <= 0 {
		ttl = 2 * time.Hour
	}
	now := time.Now().UTC()
	if s.now != nil {
		now = s.now().UTC()
	}
	cutoff := now.Add(-ttl)
	manifests, err := s.Store.ListImageCacheManifests(model.ImageCacheManifestFilter{
		SeenAfter:   cutoff,
		PresentOnly: true,
	})
	if err != nil {
		return fmt.Errorf("list image-cache manifests for integrity repair: %w", err)
	}

	if err := s.repairDistributedImages(ctx, manifests, now, cutoff); err != nil {
		return err
	}
	if err := s.repairDistributedImageLocations(ctx, manifests, now, cutoff); err != nil {
		return err
	}
	return nil
}

func (s *Service) repairDistributedImages(ctx context.Context, manifests []model.ImageCacheManifest, now, cutoff time.Time) error {
	images, err := s.Store.ListImages(model.ImageFilter{PlatformAdmin: true})
	if err != nil {
		return fmt.Errorf("list distributed images for integrity repair: %w", err)
	}
	for _, image := range images {
		if err := ctx.Err(); err != nil {
			return err
		}
		if digest := store.CanonicalImageDigest(image.CanonicalDigest); digest != "" {
			if image.CanonicalDigest != digest {
				image.CanonicalDigest = digest
				if _, err := s.Store.UpsertImage(image); err != nil {
					return fmt.Errorf("normalize canonical digest for image %s: %w", image.ID, err)
				}
			}
			if err := s.repairDistributedImageReplicas(image, manifests, cutoff); err != nil {
				return err
			}
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(image.LifecycleState), model.ImageLifecycleAvailable) {
			continue
		}
		candidates := imageIntegrityDigestsForRef(image.ImageRef, manifests)
		if len(candidates) == 1 {
			for digest := range candidates {
				if distributedImageIdentityExists(images, image, digest) {
					if _, err := s.Store.DemoteLegacyImageIfAvailableCanonicalPeer(
						image.ID,
						image.TenantID,
						image.ImageRef,
						digest,
					); err != nil {
						return fmt.Errorf("demote superseded legacy image %s: %w", image.ID, err)
					}
					break
				}
				if distributedImageIdentityExistsAtAnyLifecycle(images, image, digest) {
					return fmt.Errorf("backfill canonical digest for image %s: canonical peer is not available", image.ID)
				}

				image.CanonicalDigest = digest
				if _, err := s.Store.UpsertImage(image); err != nil {
					// A concurrent import may have materialized the same canonical
					// identity after the initial snapshot. Re-read only on a unique
					// conflict and demote the legacy empty-digest row when the exact
					// identity is now present; unrelated conflicts still fail closed.
					if !errors.Is(err, store.ErrConflict) {
						return fmt.Errorf("backfill canonical digest for image %s: %w", image.ID, err)
					}
					latest, listErr := s.Store.ListImages(model.ImageFilter{PlatformAdmin: true})
					if listErr != nil {
						return fmt.Errorf("re-read distributed images after identity conflict for image %s: %w", image.ID, listErr)
					}
					if !distributedImageIdentityExists(latest, image, digest) {
						return fmt.Errorf("backfill canonical digest for image %s: %w", image.ID, err)
					}
					if _, demoteErr := s.Store.DemoteLegacyImageIfAvailableCanonicalPeer(
						image.ID,
						image.TenantID,
						image.ImageRef,
						digest,
					); demoteErr != nil {
						return fmt.Errorf("demote concurrently superseded legacy image %s: %w", image.ID, demoteErr)
					}
				}
			}
		} else {
			// An Available image with no unique physical proof is not safe to
			// serve or prune. Lost is recoverable by a later fresh inventory.
			image.LifecycleState = model.ImageLifecycleLost
			if _, err := s.Store.UpsertImage(image); err != nil {
				return fmt.Errorf("demote unverified image %s: %w", image.ID, err)
			}
		}
		if refreshed, err := s.Store.GetImage(image.ID, "", true); err == nil {
			if err := s.repairDistributedImageReplicas(refreshed, manifests, cutoff); err != nil {
				return err
			}
		}
	}
	return nil
}

func distributedImageIdentityExists(images []model.Image, legacy model.Image, digest string) bool {
	return distributedImageIdentityExistsMatching(images, legacy, digest, func(candidate model.Image) bool {
		return strings.EqualFold(strings.TrimSpace(candidate.LifecycleState), model.ImageLifecycleAvailable)
	})
}

func distributedImageIdentityExistsAtAnyLifecycle(images []model.Image, legacy model.Image, digest string) bool {
	return distributedImageIdentityExistsMatching(images, legacy, digest, func(model.Image) bool { return true })
}

func distributedImageIdentityExistsMatching(
	images []model.Image,
	legacy model.Image,
	digest string,
	accept func(model.Image) bool,
) bool {
	digest = store.CanonicalImageDigest(digest)
	if digest == "" || accept == nil {
		return false
	}
	for _, candidate := range images {
		if candidate.ID == legacy.ID || strings.TrimSpace(candidate.TenantID) != strings.TrimSpace(legacy.TenantID) ||
			strings.TrimSpace(candidate.ImageRef) != strings.TrimSpace(legacy.ImageRef) || !accept(candidate) {
			continue
		}
		if store.CanonicalImageDigest(candidate.CanonicalDigest) == digest {
			return true
		}
	}
	return false
}

func (s *Service) repairDistributedImageReplicas(
	image model.Image,
	manifests []model.ImageCacheManifest,
	cutoff time.Time,
) error {
	replicas, err := s.Store.ListImageReplicas(model.ImageReplicaFilter{ImageID: image.ID, PlatformAdmin: true})
	if err != nil {
		return fmt.Errorf("list replicas for image %s integrity repair: %w", image.ID, err)
	}
	digest := store.CanonicalImageDigest(image.CanonicalDigest)
	for _, replica := range replicas {
		if replica.Status != model.ImageReplicaStatusPresent {
			continue
		}
		if digest == "" {
			replica.Status = model.ImageReplicaStatusMissing
			replica.LastError = "canonical image digest unavailable; replica cannot be trusted"
		} else if rawDigest := strings.TrimSpace(replica.Digest); rawDigest == "" {
			manifest, proven := imageIntegrityManifestForReplica(image, replica, manifests, cutoff)
			if !proven {
				replica.Status = model.ImageReplicaStatusMissing
				replica.LastError = "no fresh complete image-cache manifest proves this replica on its exact target"
			} else {
				verifiedAt := manifest.LastSeenAt.UTC()
				leaseExpiresAt := verifiedAt.Add(s.imageReplicaLeaseTTL())
				replica.Digest = digest
				replica.LastVerifiedAt = &verifiedAt
				replica.LeaseExpiresAt = &leaseExpiresAt
				replica.LastError = ""
			}
		} else if replicaDigest := store.CanonicalImageDigest(rawDigest); replicaDigest != digest {
			replica.Status = model.ImageReplicaStatusMissing
			replica.LastError = fmt.Sprintf("replica digest %s does not match canonical image digest %s", rawDigest, digest)
		} else {
			replica.Digest = replicaDigest
		}
		if _, err := s.Store.UpsertImageReplica(replica); err != nil {
			return fmt.Errorf("repair replica %s for image %s: %w", replica.ID, image.ID, err)
		}
	}
	return nil
}

func imageIntegrityManifestForReplica(
	image model.Image,
	replica model.ImageReplica,
	manifests []model.ImageCacheManifest,
	cutoff time.Time,
) (model.ImageCacheManifest, bool) {
	digest := store.CanonicalImageDigest(image.CanonicalDigest)
	if digest == "" {
		return model.ImageCacheManifest{}, false
	}
	keys := integrityImageReferenceKeys(image.ImageRef, digest)
	var newest model.ImageCacheManifest
	found := false
	for _, manifest := range manifests {
		if !imageIntegrityManifestUsable(manifest) ||
			store.CanonicalImageDigest(manifest.Digest) != digest ||
			!integrityManifestMatchesImageRepository(image.ImageRef, manifest) ||
			!integrityManifestMatchesKeys(manifest, keys) ||
			!integrityManifestMatchesReplicaIdentity(replica, manifest) ||
			manifest.LastSeenAt.IsZero() || manifest.LastSeenAt.Before(cutoff) {
			continue
		}
		if !found || manifest.LastSeenAt.After(newest.LastSeenAt) {
			newest = manifest
			found = true
		}
	}
	return newest, found
}

func integrityManifestMatchesImageRepository(imageRef string, manifest model.ImageCacheManifest) bool {
	repo, _, ok := imagecachekeys.SplitRepoTarget(imagecachekeys.StripRegistry(strings.TrimSpace(imageRef)))
	if !ok {
		return false
	}
	repo = strings.ToLower(strings.Trim(strings.TrimSpace(repo), "/"))
	manifestRepo := strings.ToLower(strings.Trim(strings.TrimSpace(manifest.Repo), "/"))
	return repo != "" && manifestRepo == repo
}

func integrityManifestMatchesReplicaIdentity(replica model.ImageReplica, manifest model.ImageCacheManifest) bool {
	matched := false
	for _, pair := range [][2]string{
		{replica.NodeID, manifest.NodeID},
		{replica.RuntimeID, manifest.RuntimeID},
		{replica.ClusterNodeName, manifest.ClusterNodeName},
	} {
		replicaValue := strings.TrimSpace(pair[0])
		if replicaValue == "" {
			continue
		}
		matched = true
		if manifestValue := strings.TrimSpace(pair[1]); manifestValue == "" || manifestValue != replicaValue {
			return false
		}
	}
	return matched
}

func (s *Service) repairDistributedImageLocations(ctx context.Context, manifests []model.ImageCacheManifest, now, cutoff time.Time) error {
	locations, err := s.Store.ListImageLocations(model.ImageLocationFilter{Status: model.ImageLocationStatusPresent, PlatformAdmin: true})
	if err != nil {
		return fmt.Errorf("list image locations for integrity repair: %w", err)
	}
	for _, location := range locations {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !store.ImageLocationIsDistributed(location) || strings.TrimSpace(location.Digest) != "" {
			continue
		}
		if !distributedImageLocationFreshForIntegrity(location, cutoff) {
			if err := s.demoteLegacyImageLocationIfUnchanged(
				location.ID,
				location.UpdatedAt,
				now,
				"image location evidence is stale and has no canonical digest",
			); err != nil {
				return fmt.Errorf("demote stale image location %s: %w", location.ID, err)
			}
			continue
		}
		candidates := imageIntegrityDigestsForLocation(location, manifests)
		demotionReason := ""
		if len(candidates) == 1 {
			for digest := range candidates {
				repaired := location
				// The canonical digest is a new identity.  Do not carry the
				// legacy row ID into the upsert, otherwise an ID-based update
				// would overwrite the row that must be retained as Missing.
				repaired.ID = ""
				repaired.Digest = digest
				repaired.LastSeenAt = timePointer(now)
				if _, err := s.Store.UpsertImageLocation(repaired); err != nil {
					return fmt.Errorf("backfill canonical digest for image location %s: %w", location.ID, err)
				}
			}
			demotionReason = "superseded by canonical-digest image location"
		} else {
			if len(candidates) == 0 {
				demotionReason = "no fresh complete image-cache manifest proves this location"
			} else {
				demotionReason = "multiple image-cache digests match this location"
			}
		}
		if err := s.demoteLegacyImageLocationIfUnchanged(
			location.ID,
			location.UpdatedAt,
			now,
			demotionReason,
		); err != nil {
			return fmt.Errorf("finalize legacy image location %s: %w", location.ID, err)
		}
	}
	return nil
}

func (s *Service) demoteLegacyImageLocationIfUnchanged(
	id string,
	expectedUpdatedAt time.Time,
	observedAt time.Time,
	lastError string,
) error {
	_, err := s.Store.DemoteLegacyImageLocationIfUnchanged(
		id,
		expectedUpdatedAt,
		observedAt,
		lastError,
	)
	// A reporter may refresh or supersede the row after the maintenance scan.
	// The CAS conflict proves this pass did not mutate that newer evidence; the
	// periodic maintenance loop will classify the refreshed row again.
	if errors.Is(err, store.ErrConflict) {
		return nil
	}
	return err
}

func imageIntegrityDigestsForRef(ref string, manifests []model.ImageCacheManifest) map[string]struct{} {
	keys := integrityImageReferenceKeys(ref, "")
	out := map[string]struct{}{}
	for _, manifest := range manifests {
		digest := store.CanonicalImageDigest(manifest.Digest)
		if !imageIntegrityManifestUsable(manifest) || digest == "" || !integrityManifestMatchesKeys(manifest, keys) {
			continue
		}
		out[digest] = struct{}{}
	}
	return out
}

func imageIntegrityDigestsForLocation(location model.ImageLocation, manifests []model.ImageCacheManifest) map[string]struct{} {
	keys := integrityImageReferenceKeys(location.ImageRef, location.Digest)
	out := map[string]struct{}{}
	for _, manifest := range manifests {
		if !integrityManifestMatchesPhysicalIdentity(location, manifest) {
			continue
		}
		digest := store.CanonicalImageDigest(manifest.Digest)
		if !imageIntegrityManifestUsable(manifest) || digest == "" || !integrityManifestMatchesKeys(manifest, keys) {
			continue
		}
		out[digest] = struct{}{}
	}
	return out
}

// imageIntegrityManifestUsable deliberately requires the complete blob graph
// evidence emitted by the image-cache inventory endpoint.  A manifest row
// with only a HEAD/digest (or with referenced_blob_bytes=0) is exactly the
// historical false-positive that allowed a Present/Available record to be
// recreated while config/layers were missing.
func imageIntegrityManifestUsable(manifest model.ImageCacheManifest) bool {
	if !manifest.Present || store.CanonicalImageDigest(manifest.Digest) == "" || len(manifest.ReferencedBlobs) == 0 {
		return false
	}
	for _, blob := range manifest.ReferencedBlobs {
		if store.CanonicalImageDigest(blob) == "" {
			return false
		}
	}
	return manifest.TotalBlobBytes > 0
}

func integrityImageReferenceKeys(ref, digest string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, key := range imagecachekeys.ExactImageReferenceKeys(strings.TrimSpace(ref), store.CanonicalImageDigest(digest)) {
		key = strings.ToLower(strings.TrimSpace(key))
		if key != "" {
			out[key] = struct{}{}
		}
	}
	return out
}

func integrityManifestMatchesKeys(manifest model.ImageCacheManifest, keys map[string]struct{}) bool {
	for _, key := range imagecachekeys.ExactManifestReferenceKeys(manifest.Repo, manifest.Target, manifest.Digest, manifest.ImageRef) {
		if _, ok := keys[strings.ToLower(strings.TrimSpace(key))]; ok {
			return true
		}
	}
	return false
}

func integrityManifestMatchesPhysicalIdentity(location model.ImageLocation, manifest model.ImageCacheManifest) bool {
	if location.NodeID != "" && manifest.NodeID != "" && strings.TrimSpace(location.NodeID) != strings.TrimSpace(manifest.NodeID) {
		return false
	}
	if location.RuntimeID != "" && manifest.RuntimeID != "" && strings.TrimSpace(location.RuntimeID) != strings.TrimSpace(manifest.RuntimeID) {
		return false
	}
	if location.ClusterNodeName != "" && manifest.ClusterNodeName != "" && strings.TrimSpace(location.ClusterNodeName) != strings.TrimSpace(manifest.ClusterNodeName) {
		return false
	}
	return true
}

func distributedImageLocationFreshForIntegrity(location model.ImageLocation, cutoff time.Time) bool {
	observed := location.UpdatedAt
	if location.LastSeenAt != nil && !location.LastSeenAt.IsZero() {
		observed = *location.LastSeenAt
	}
	return !observed.IsZero() && !observed.Before(cutoff)
}

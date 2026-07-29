package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/imagecachekeys"
	"fugue/internal/model"
)

type legacyDistributedImageManifestGroup struct {
	repo      string
	digest    string
	manifests []model.ImageCacheManifest
}

// reconcileLegacyDistributedImageMetadata materializes the distributed image
// records that predate the distributed image index. Only fresh physical cache
// evidence for an app's current desired image is accepted. Missing or
// ambiguous evidence remains untouched and therefore cannot become deletion
// authority.
func (s *Service) reconcileLegacyDistributedImageMetadata(ctx context.Context) error {
	if s == nil || s.Store == nil || !s.imageStoreDistributedMode() {
		return nil
	}
	apps, err := s.Store.ListAppsMetadata("", true)
	if err != nil {
		return fmt.Errorf("list apps for distributed image metadata backfill: %w", err)
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
	nodes, err := s.Store.ListImageCacheNodeInventories(model.ImageCacheNodeInventoryFilter{StaleAfter: cutoff})
	if err != nil {
		return fmt.Errorf("list image-cache nodes for distributed image metadata backfill: %w", err)
	}
	manifests, err := s.Store.ListImageCacheManifests(model.ImageCacheManifestFilter{
		SeenAfter:   cutoff,
		PresentOnly: true,
	})
	if err != nil {
		return fmt.Errorf("list image-cache manifests for distributed image metadata backfill: %w", err)
	}
	endpoints := legacyImageCacheEndpointByNode(nodes)
	backfilled := 0
	for _, app := range apps {
		if err := ctx.Err(); err != nil {
			return err
		}
		if app.Spec.Replicas <= 0 {
			continue
		}
		refs := s.legacyDistributedImageCurrentRefs(app)
		group, ok := legacyDistributedImageManifestForRefs(refs, manifests)
		if !ok {
			continue
		}
		existing, err := s.distributedImagesForRefs(app, append(append([]string(nil), refs...), group.digest)...)
		if err != nil {
			return fmt.Errorf("inspect existing distributed image metadata for app %s: %w", app.ID, err)
		}
		indexedImage, alreadyIndexed := legacyDistributedImageForDigest(existing, app.ID, group.digest)
		if alreadyIndexed {
			current, err := s.legacyDistributedImageMetadataCurrent(app, indexedImage, refs, group, now)
			if err != nil {
				return fmt.Errorf("inspect distributed image metadata health for app %s: %w", app.ID, err)
			}
			if current {
				continue
			}
		}
		var repairImage *model.Image
		if alreadyIndexed {
			repairImage = &indexedImage
		}
		if err := s.backfillLegacyDistributedImage(app, repairImage, refs, group, endpoints, ttl); err != nil {
			return err
		}
		backfilled++
	}
	if backfilled > 0 && s.Logger != nil {
		s.Logger.Printf("backfilled distributed image metadata for %d legacy app image(s)", backfilled)
	}
	return nil
}

func (s *Service) legacyDistributedImageCurrentRefs(app model.App) []string {
	if s == nil || app.Spec.Replicas <= 0 {
		return nil
	}
	values := []string{app.Spec.Image, s.managedDeployImageRef(app)}
	if source := model.AppBuildSource(app); source != nil {
		values = append(values, source.ResolvedImageRef)
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		ref := strings.TrimSpace(s.managedImageRefFromRuntimeValue(value))
		if ref == "" {
			continue
		}
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		out = append(out, ref)
	}
	return out
}

func legacyDistributedImageManifestForRefs(refs []string, manifests []model.ImageCacheManifest) (legacyDistributedImageManifestGroup, bool) {
	for _, ref := range refs {
		repo, _, ok := imagecachekeys.SplitRepoTarget(imagecachekeys.StripRegistry(ref))
		if !ok || strings.TrimSpace(repo) == "" {
			continue
		}
		repo = strings.ToLower(strings.Trim(strings.TrimSpace(repo), "/"))
		refKeys := keySetFromControllerValues(imagecachekeys.ExactImageReferenceKeys(ref, ""))
		groups := map[string]legacyDistributedImageManifestGroup{}
		for _, manifest := range manifests {
			manifestRepo := strings.ToLower(strings.Trim(strings.TrimSpace(manifest.Repo), "/"))
			digest := imagecachekeys.NormalizeDigest(manifest.Digest)
			if manifestRepo != repo || digest == "" ||
				!controllerKeySetContainsAny(refKeys, imagecachekeys.ExactManifestReferenceKeys(manifest.Repo, manifest.Target, manifest.Digest, manifest.ImageRef)...) {
				continue
			}
			key := manifestRepo + "\x00" + digest
			group := groups[key]
			group.repo = manifestRepo
			group.digest = digest
			group.manifests = append(group.manifests, manifest)
			groups[key] = group
		}
		if len(groups) != 1 {
			continue
		}
		for _, group := range groups {
			return group, true
		}
	}
	return legacyDistributedImageManifestGroup{}, false
}

func legacyDistributedImageForDigest(images []model.Image, appID, digest string) (model.Image, bool) {
	digest = imagecachekeys.NormalizeDigest(digest)
	appID = strings.TrimSpace(appID)
	var fallback model.Image
	found := false
	for _, image := range images {
		if imagecachekeys.NormalizeDigest(image.CanonicalDigest) != digest {
			continue
		}
		if strings.TrimSpace(image.AppID) == appID {
			return image, true
		}
		if !found {
			fallback = image
			found = true
		}
	}
	return fallback, found
}

func keySetFromControllerValues(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	addControllerImageKeys(out, values...)
	return out
}

func (s *Service) legacyDistributedImageMetadataCurrent(
	app model.App,
	image model.Image,
	refs []string,
	group legacyDistributedImageManifestGroup,
	now time.Time,
) (bool, error) {
	if strings.TrimSpace(image.LifecycleState) != model.ImageLifecycleAvailable ||
		imagecachekeys.NormalizeDigest(image.CanonicalDigest) != group.digest {
		return false, nil
	}

	aliases, err := s.Store.ListImageAliases(model.ImageAliasFilter{
		ImageID:  image.ID,
		TenantID: image.TenantID,
	})
	if err != nil {
		return false, err
	}
	for _, expected := range s.legacyDistributedImageAliasRefs(app, refs, group) {
		found := false
		for _, alias := range aliases {
			if strings.TrimSpace(alias.AliasRef) == expected &&
				imagecachekeys.NormalizeDigest(alias.Digest) == group.digest {
				found = true
				break
			}
		}
		if !found {
			return false, nil
		}
	}

	pins, err := s.Store.ListImagePins(model.ImagePinFilter{
		ImageID:  image.ID,
		TenantID: image.TenantID,
		AppID:    app.ID,
		Reason:   model.ImagePinReasonCurrentDeploy,
	})
	if err != nil {
		return false, err
	}
	currentDeployPinned := false
	for _, pin := range pins {
		if pin.MinReplicas >= 1 && (pin.ExpiresAt == nil || !pin.ExpiresAt.Before(now)) {
			currentDeployPinned = true
			break
		}
	}
	if !currentDeployPinned {
		return false, nil
	}

	replicas, err := s.Store.ListImageReplicas(model.ImageReplicaFilter{
		ImageID:  image.ID,
		TenantID: image.TenantID,
	})
	if err != nil {
		return false, err
	}
	locations, err := s.Store.ListImageLocations(model.ImageLocationFilter{
		TenantID: image.TenantID,
		ImageRef: image.ImageRef,
		Digest:   group.digest,
		Status:   model.ImageLocationStatusPresent,
	})
	if err != nil {
		return false, err
	}
	for _, manifest := range group.manifests {
		replicaCurrent := false
		for _, replica := range replicas {
			if legacyDistributedImageManifestMatchesTarget(manifest, replica.NodeID, replica.RuntimeID, replica.ClusterNodeName) &&
				imagecachekeys.NormalizeDigest(replica.Digest) == group.digest &&
				replica.Status == model.ImageReplicaStatusPresent &&
				replica.LastVerifiedAt != nil &&
				!replica.LastVerifiedAt.Before(manifest.LastSeenAt) &&
				replica.LeaseExpiresAt != nil &&
				!replica.LeaseExpiresAt.Before(now) {
				replicaCurrent = true
				break
			}
		}
		if !replicaCurrent {
			return false, nil
		}
		locationCurrent := false
		for _, location := range locations {
			if legacyDistributedImageManifestMatchesTarget(manifest, location.NodeID, location.RuntimeID, location.ClusterNodeName) &&
				location.LastSeenAt != nil &&
				!location.LastSeenAt.Before(manifest.LastSeenAt) {
				locationCurrent = true
				break
			}
		}
		if !locationCurrent {
			return false, nil
		}
	}
	return true, nil
}

func legacyDistributedImageManifestMatchesTarget(manifest model.ImageCacheManifest, nodeID, runtimeID, clusterNodeName string) bool {
	return strings.TrimSpace(manifest.NodeID) == strings.TrimSpace(nodeID) &&
		strings.TrimSpace(manifest.RuntimeID) == strings.TrimSpace(runtimeID) &&
		strings.TrimSpace(manifest.ClusterNodeName) == strings.TrimSpace(clusterNodeName)
}

func (s *Service) legacyDistributedImageAliasRefs(
	app model.App,
	refs []string,
	group legacyDistributedImageManifestGroup,
) []string {
	digestRef := strings.Trim(strings.TrimSpace(s.registryPushBase), "/") + "/" + group.repo + "@" + group.digest
	aliasRefs := append(append([]string(nil), refs...), app.Spec.Image, digestRef)
	if source := model.AppBuildSource(app); source != nil {
		aliasRefs = append(aliasRefs, source.ResolvedImageRef)
	}
	return compactImageRefs(aliasRefs)
}

func (s *Service) backfillLegacyDistributedImage(
	app model.App,
	existing *model.Image,
	refs []string,
	group legacyDistributedImageManifestGroup,
	endpoints map[string]string,
	ttl time.Duration,
) error {
	if len(refs) == 0 || len(group.manifests) == 0 {
		return nil
	}
	primary := group.manifests[0]
	imageRecord := model.Image{
		TenantID:                 strings.TrimSpace(app.TenantID),
		AppID:                    strings.TrimSpace(app.ID),
		ImageRef:                 refs[0],
		CanonicalDigest:          group.digest,
		MediaType:                strings.TrimSpace(primary.MediaType),
		ManifestSizeBytes:        primary.ManifestSizeBytes,
		BlobBytes:                primary.TotalBlobBytes,
		LifecycleState:           model.ImageLifecycleAvailable,
		RequiredReplicaCount:     s.imageTargetReplicaCount(model.Image{}),
		MinAvailableReplicaCount: s.imageMinReplicaCount(),
	}
	if existing != nil {
		imageRecord = *existing
		imageRecord.LifecycleState = model.ImageLifecycleAvailable
		imageRecord.CanonicalDigest = group.digest
		imageRecord.RequiredReplicaCount = s.imageTargetReplicaCount(model.Image{})
		imageRecord.MinAvailableReplicaCount = s.imageMinReplicaCount()
		if strings.TrimSpace(imageRecord.TenantID) == "" {
			imageRecord.TenantID = strings.TrimSpace(app.TenantID)
		}
		if strings.TrimSpace(primary.MediaType) != "" {
			imageRecord.MediaType = strings.TrimSpace(primary.MediaType)
		}
		if primary.ManifestSizeBytes > 0 {
			imageRecord.ManifestSizeBytes = primary.ManifestSizeBytes
		}
		if primary.TotalBlobBytes > 0 {
			imageRecord.BlobBytes = primary.TotalBlobBytes
		}
	}
	image, err := s.Store.UpsertImage(imageRecord)
	if err != nil {
		return fmt.Errorf("backfill distributed image for app %s: %w", app.ID, err)
	}
	for _, aliasRef := range s.legacyDistributedImageAliasRefs(app, refs, group) {
		if _, err := s.Store.UpsertImageAlias(model.ImageAlias{
			ImageID:  image.ID,
			TenantID: image.TenantID,
			AliasRef: aliasRef,
			Digest:   group.digest,
		}); err != nil {
			return fmt.Errorf("backfill distributed image alias %s for app %s: %w", aliasRef, app.ID, err)
		}
	}
	if _, err := s.Store.UpsertImagePin(model.ImagePin{
		ImageID:     image.ID,
		TenantID:    image.TenantID,
		AppID:       strings.TrimSpace(app.ID),
		Reason:      model.ImagePinReasonCurrentDeploy,
		MinReplicas: 1,
	}); err != nil {
		return fmt.Errorf("backfill current image pin for app %s: %w", app.ID, err)
	}
	for _, manifest := range group.manifests {
		verifiedAt := manifest.LastSeenAt.UTC()
		leaseExpiresAt := verifiedAt.Add(ttl)
		replica, err := s.Store.UpsertImageReplica(model.ImageReplica{
			ImageID:         image.ID,
			TenantID:        image.TenantID,
			AppID:           strings.TrimSpace(app.ID),
			Digest:          group.digest,
			NodeID:          strings.TrimSpace(manifest.NodeID),
			RuntimeID:       strings.TrimSpace(manifest.RuntimeID),
			ClusterNodeName: strings.TrimSpace(manifest.ClusterNodeName),
			CacheEndpoint:   legacyImageCacheEndpoint(endpoints, manifest),
			Status:          model.ImageReplicaStatusPresent,
			LastVerifiedAt:  &verifiedAt,
			LeaseExpiresAt:  &leaseExpiresAt,
			SizeBytes:       manifest.TotalBlobBytes,
		})
		if err != nil {
			return fmt.Errorf("backfill distributed image replica for app %s node %s: %w", app.ID, manifest.ClusterNodeName, err)
		}
		if _, err := s.Store.UpsertImageLocation(imageLocationFromDistributedReplica(image, replica)); err != nil {
			return fmt.Errorf("backfill distributed image location for app %s node %s: %w", app.ID, manifest.ClusterNodeName, err)
		}
	}
	return nil
}

func legacyImageCacheEndpointByNode(nodes []model.ImageCacheNodeInventory) map[string]string {
	out := map[string]string{}
	for _, node := range nodes {
		endpoint := strings.TrimRight(strings.TrimSpace(node.CacheEndpoint), "/")
		if endpoint == "" {
			continue
		}
		for _, key := range []string{node.NodeID, node.RuntimeID, node.ClusterNodeName} {
			if key = strings.TrimSpace(key); key != "" {
				out[key] = endpoint
			}
		}
	}
	return out
}

func legacyImageCacheEndpoint(endpoints map[string]string, manifest model.ImageCacheManifest) string {
	for _, key := range []string{manifest.NodeID, manifest.RuntimeID, manifest.ClusterNodeName} {
		if endpoint := strings.TrimSpace(endpoints[strings.TrimSpace(key)]); endpoint != "" {
			return endpoint
		}
	}
	return ""
}

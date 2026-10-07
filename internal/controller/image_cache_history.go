package controller

import (
	"context"
	"fmt"
	"fugue/internal/imagecachekeys"
	"fugue/internal/model"
	"fugue/internal/store"
	"strings"
	"time"
)

// Historical attribution only accepts a completed operation with an immutable
// digest reference matching a complete physical graph. A name/tag, old mtime,
// absent app, or failed build is never promoted into retirement authority.
// Recovered generations enter Lost; ordinary retention decides whether they
// must be kept for rollback or explicitly retired. This never changes serving.
func (s *Service) reconcileHistoricalImageProvenance(ctx context.Context) error {
	if s == nil || s.Store == nil || !s.imageStoreDistributedMode() {
		return nil
	}
	images, err := s.Store.ListImagesWithRetirementAuthority()
	if err != nil {
		return err
	}
	known := map[string]bool{}
	for _, im := range images {
		if d := store.CanonicalImageDigest(im.CanonicalDigest); d != "" {
			known[d] = true
		}
	}
	ttl := s.Config.ImageCacheInventoryTTL
	if ttl <= 0 {
		ttl = 2 * time.Hour
	}
	manifests, err := s.Store.ListImageCacheManifests(model.ImageCacheManifestFilter{PresentOnly: true, SeenAfter: time.Now().Add(-ttl)})
	if err != nil {
		return err
	}
	groups := map[string]model.ImageCacheManifest{}
	for _, m := range manifests {
		d := store.CanonicalImageDigest(m.Digest)
		if d == "" || known[d] || !imageIntegrityManifestUsable(m) {
			continue
		}
		groups[m.Repo+"@"+d] = m
	}
	if len(groups) == 0 {
		return nil
	}
	apps, err := s.Store.ListAppsMetadata("", true)
	if err != nil {
		return err
	}
	deleted, err := s.Store.ListDeletedAppsMetadata("", true)
	if err != nil {
		return err
	}
	apps = append(apps, deleted...)
	ids := make([]string, 0, len(apps))
	appByID := map[string]model.App{}
	for _, a := range apps {
		ids = append(ids, a.ID)
		appByID[a.ID] = a
	}
	ops, err := s.Store.ListImageCandidateOperationsByApps("", true, ids)
	if err != nil {
		return err
	}
	type claim struct {
		op       model.Operation
		ref      string
		manifest model.ImageCacheManifest
	}
	claims := map[string]claim{}
	ambiguous := map[string]bool{}
	for appID, list := range ops {
		app := appByID[appID]
		for _, op := range list {
			if op.Status != model.OperationStatusCompleted || op.AppID != appID || op.TenantID != app.TenantID {
				continue
			}
			refs := []string{}
			if op.DesiredSpec != nil {
				refs = append(refs, op.DesiredSpec.Image)
			}
			if op.DesiredSource != nil {
				refs = append(refs, op.DesiredSource.ResolvedImageRef)
			}
			for _, ref := range refs {
				if !strings.Contains(ref, "@sha256:") {
					continue
				}
				ref = s.managedImageRefFromRuntimeValue(ref)
				if ref == "" {
					continue
				}
				repo, target, ok := imagecachekeys.SplitRepoTarget(imagecachekeys.StripRegistry(ref))
				if !ok {
					continue
				}
				digest := store.CanonicalImageDigest(target)
				if digest == "" {
					continue
				}
				key := repo + "@" + digest
				m, ok := groups[key]
				if !ok {
					continue
				}
				if prev, ok := claims[key]; ok && (prev.op.AppID != appID || prev.op.TenantID != op.TenantID) {
					ambiguous[key] = true
					continue
				}
				claims[key] = claim{op: op, ref: ref, manifest: m}
			}
		}
	}
	recovered := 0
	for key, c := range claims {
		if err := ctx.Err(); err != nil {
			return err
		}
		if ambiguous[key] {
			continue
		}
		m := c.manifest
		if _, err := s.Store.UpsertImage(model.Image{TenantID: c.op.TenantID, AppID: c.op.AppID, ImageRef: c.ref, CanonicalDigest: m.Digest, MediaType: m.MediaType, ManifestSizeBytes: m.ManifestSizeBytes, BlobBytes: m.TotalBlobBytes, SourceOperationID: c.op.ID, LifecycleState: model.ImageLifecycleLost, RequiredReplicaCount: 1, MinAvailableReplicaCount: 1}); err != nil {
			return fmt.Errorf("recover historical image authority: %w", err)
		}
		recovered++
	}
	if recovered > 0 && s.Logger != nil {
		s.Logger.Printf("recovered %d historical image generation(s) from completed immutable operation references; retention remains independently gated", recovered)
	}
	return nil
}
